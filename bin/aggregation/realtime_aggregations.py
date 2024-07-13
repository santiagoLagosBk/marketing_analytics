import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType, TimestampType)

from user_agents import parse
from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')
DATABASES = os.environ.get('DATABASE')
USER = os.environ.get('USER_NAME')
PASSWORD = os.environ.get('PASSWORD')
CLICK_EVENT = 'CLICKED'
OPEN_EVENT = 'OPENED'


def parse_user_agent(user_agent: str) -> dict:
    ua = parse(user_agent)
    return {
        "browser_name": ua.browser.family if ua.browser.family is not None else 'None',
        "os_name": ua.os.family if ua.os.family is not None else 'None',
        "device_family": ua.device.family if ua.device.family is not None else 'None',
        "device_brand": ua.device.brand if ua.device.brand is not None else 'None'
    }


def group_by_campaign_city(df: DataFrame) -> DataFrame:
    return df.groupBy("campaign_id", "city").count().alias("total_events_by_city")


def group_by_campaign_event_type(df: DataFrame) -> DataFrame:
    return df.groupBy("campaign_id").agg(
        f.sum(f.when(f.col('type_event') == 'DELIVERED_EVENT', 1).otherwise(0)).alias('DELIVERED'),
        f.sum(f.when(f.col('type_event') == 'CLICKED', 1).otherwise(0)).alias('CLICKED'),
        f.sum(f.when(f.col('type_event') == 'SPAM', 1).otherwise(0)).alias('SPAM'),
        f.sum(f.when(f.col('type_event') == 'OPENED', 1).otherwise(0)).alias('OPENED'),
        f.sum(f.when(f.col('type_event') == 'SENT', 1).otherwise(0)).alias('SENT'),
    ).fillna(0)


def aggregate_effectiveness_by_campaign_id(df: DataFrame) -> DataFrame:
    df_main_events = df.filter((df.type_event == 'CLICKED') | (df.type_event == 'OPENED'))

    unique_events = df_main_events. \
        groupBy(["campaign_id", "customer_id", "type_event"]). \
        agg(f.count('type_event').alias('total_events'))

    return unique_events


def get_products(spark_obj) -> DataFrame:
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
          .select("data.*").withColumn("event_date", f.col("event_date").cast(TimestampType())))
    df = df.withWatermark('event_date', '1 minute')
    return df


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

    df_campaign_city = write_to_kafka_stream(
        group_by_campaign_city(events_df), "events-city-aggregation", "./checkpoints/checkpointCityAggregation")

    df_campaign_event_type = write_to_kafka_stream(
        group_by_campaign_event_type(events_df), "events-campaign-aggregation", "./checkpoints/checkpointCampaignAggregation")

    effectiveness_campaign_stream = write_to_kafka_stream(
        aggregate_effectiveness_by_campaign_id(events_df), "campaign-effectiveness", "./checkpoints/checkpointEffectiveness"
    )

    df_campaign_city.awaitTermination()
    df_campaign_event_type.awaitTermination()
    effectiveness_campaign_stream.awaitTermination()
