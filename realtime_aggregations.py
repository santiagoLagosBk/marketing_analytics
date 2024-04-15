import logging

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from user_agents import parse
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType, TimestampType)

from dotenv import load_dotenv
import os

load_dotenv()


HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')
DATABASES = os.environ.get('DATABASE')
USER = os.environ.get('USER_NAME')
PASSWORD = os.environ.get('PASSWORD')
CLICK_EVENT = 'CLICKED'



def parse_user_agent(user_agent):
    ua_data = parse(user_agent)
    return {
        "browser_name": ua_data["user_agent"]["family"] if ua_data["user_agent"] else 'None',
        "os_name": ua_data["os"]["family"] if ua_data["os"] else 'None',
        "device_family": ua_data["device"]["family"] if ua_data["device"] else 'None',
        "device_brand": ua_data["device"]["brand"] if ua_data["device"] else 'None'
    }


def aggregate_by_city(df):
    return df.groupBy("city").count().alias("total_events_by_city")


def aggregate_event_type_by_campaign_id(df):

    return df.groupBy("campaign_id").agg(
        f.sum(f.when(f.col('type_event') == 'DELIVERED_EVENT', 1).otherwise(0)).alias('DELIVERED'),
        f.sum(f.when(f.col('type_event') == 'CLICKED', 1).otherwise(0)).alias('CLICKED'),
        f.sum(f.when(f.col('type_event') == 'SPAM', 1).otherwise(0)).alias('SPAM'),
        f.sum(f.when(f.col('type_event') == 'OPENED', 1).otherwise(0)).alias('OPENED'),
        f.sum(f.when(f.col('type_event') == 'SENT', 1).otherwise(0)).alias('SENT'),
    ).fillna(0)


def get_products(spark_obj) -> pyspark.sql.DataFrame:
    """
    Get product data from the database.

    :param spark_obj: SparkSession object.
    :return: DataFrame containing product data.
    """
    url = ('jdbc:postgresql://{}:{}/{}'.
           format(HOST, PORT, DATABASES))

    try:
        df_products = (spark_obj.read.format("jdbc").
                       option("url", url).
                       option("dbtable", 'product').
                       option("user", USER).
                       option("password", PASSWORD).
                       option("driver", "org.postgresql.Driver").load())

        return df_products

    except Exception as e:
        print(e)
        logging.error("An error occurred while getting products from the database: %s", e)
        return None


def aggregate_most_loved_product_by_campaign(df_events: pyspark.sql.DataFrame,
                                             df_products: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Aggregate the most loved product by campaign.

    :param df_events: DataFrame containing event data.
    :param df_products: DataFrame containing product data.
    :return: DataFrame with aggregated results.
    """

    # Aggregate count of clicks per product and campaign
    df_click_count = df_events.filter(df_events.type_event == "CLICK_EVENT") \
        .groupBy(["campaign_id", "product_id"]) \
        .count() \
        .withColumnRenamed("count", "count_product_id")

    # Find the maximum count for each campaign
    max_count_df = df_click_count.groupBy("campaign_id").agg(f.max("count_product_id").alias("max_count"))

    # Join to get the product with the maximum count for each campaign
    most_loved_products_df = max_count_df.alias("a"). \
        join(
        df_click_count.alias("b"),
        [
            max_count_df.campaign_id == df_click_count.campaign_id,
            max_count_df.max_count == df_click_count.count_product_id
        ],
        "inner"
    ) \
        .select("b.campaign_id",
                "b.product_id",
                "b.count_product_id")

    result_df = most_loved_products_df.alias("ml") \
        .join(
        df_products.alias("pr"),
        [
            most_loved_products_df.product_id == df_products.id
        ],
        "inner"
    ) \
        .select(
        "ml.campaign_id",
        "ml.product_id",
        "pr.title",
        "pr.category",
        "pr.image",
        "pr.price"
    )

    return result_df


def read_kafka_stream(spark_obj):
    schema_events = StructType(
        [
            StructField("customer_id", IntegerType(), False),
            StructField("campaign_id", StringType(), False),
            StructField("type_event", StringType(), False),
            StructField("event_date", StringType(), False),
            StructField("gender", StringType()),
            StructField("email", StringType()),
            StructField("user_agent", StringType()),
            StructField("country", StringType(), False),
            StructField("city", StringType(), False),
            StructField("state", StringType(), False),
            StructField("product_id", IntegerType(), True)
        ])

    df = (spark_obj
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", os.getenv("EVENTS_TOPIC"))
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("CAST(value AS STRING)")
          .select(f.from_json(f.col("value"), schema_events).alias("data"))
          .select("data.*"))

    parse_user_agent_udf = f.udf(parse_user_agent, StringType())

    events_topic_df = (df
                       .withColumn("event_date", f.col("event_date").cast(TimestampType()))
                       .withColumn("user_agent_info", parse_user_agent_udf("user_agent")))

    return events_topic_df


def write_to_kafka_stream(df, topic, checkpoint_location):
    return (df
            .selectExpr("to_json(struct(*)) AS value")
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", topic)
            .option("checkpointLocation", checkpoint_location)
            .outputMode("update")
            .start())


if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("Realtime Aggregations")
             .master("local[3]")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
             .config("spark.jars", "/home/santiago/PycharmProjects/marketing_analytics/packages/postgresql-42.7.1.jar")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())

    events_df = read_kafka_stream(spark)

    events_by_country_stream = write_to_kafka_stream(
        aggregate_by_city(events_df), "events-country-aggregation", "./checkpoints/checkpoint1")

    events_by_campaign_stream = write_to_kafka_stream(
        aggregate_event_type_by_campaign_id(events_df), "events-campaign-aggregation", "./checkpoints/checkpoint2")

    loved_product_by_campaign_stream = write_to_kafka_stream(
        aggregate_most_loved_product_by_campaign(events_df, get_products(spark_obj=spark)),
        "loved-product-aggregation", "./checkpoints/checkpoint3")

    events_by_country_stream.awaitTermination()
    events_by_campaign_stream.awaitTermination()
