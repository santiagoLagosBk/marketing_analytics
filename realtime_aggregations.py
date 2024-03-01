from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType, TimestampType)

from user_agents import parse

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
        StructField("state", StringType(), False)
    ])


def parse_browser_name(user_agent):
    ua = parse(user_agent)
    return ua.browser.family if ua.browser else 'None'


def parse_os_name(user_agent):
    ua = parse(user_agent)
    return ua.os.family if ua.os else 'None'


def parse_device_family(user_agent):
    ua = parse(user_agent)
    return ua.device.family if ua.device else 'None'


def parse_device_brand(user_agent):
    ua = parse(user_agent)
    return ua.device.brand if ua.device else 'None'


def aggregation_by_country(df):
    return df.groupBy("country").count().alias("total_events_by_country")


def aggregation_event_type_by_campaign_id(df):
    return df.groupBy("campaign_id", "type_event").count().alias("count_events_by_campaign_id")


def aggregate_by_campaign_id(spark_obj: SparkSession):
    df = (spark_obj.
          readStream.
          format("kafka").
          option("kafka.bootstrap.servers", "localhost:9092").
          option("subscribe", "events").
          option("startingOffsets", "earliest").
          load().
          selectExpr("CAST(value AS STRING)").
          select(f.from_json(f.col("value"), schema_events).alias("data")).
          select("data.*"))

    parse_browser_name_udf = f.udf(parse_browser_name, StringType())
    parse_os_name_udf = f.udf(parse_os_name, StringType())
    parse_device_family_udf = f.udf(parse_device_family, StringType())
    parse_device_brand_udf = f.udf(parse_device_brand, StringType())

    events_df = (df.withColumn("event_date", f.col("event_date").cast(TimestampType()))
                 .withColumn("browser_name", parse_browser_name_udf("user_agent"))
                 .withColumn("os_name", parse_os_name_udf("user_agent"))
                 .withColumn("device_family", parse_device_family_udf("user_agent"))
                 .withColumn("device_brand", parse_device_brand_udf("user_agent")))

    enrichment_data = events_df.withWatermark("event_date", delayThreshold="1 minutes")

    events_by_country_to_kafka = (aggregation_by_country(enrichment_data)
                                  .selectExpr("to_json(struct(*)) AS value")
                                  .writeStream
                                  .format("kafka")
                                  .option("kafka.bootstrap.servers", "localhost:9092")
                                  .option("topic", "events-country-aggregation")
                                  .option("checkpointLocation", "./checkpoints/checkpoint1")
                                  .outputMode("update")
                                  .start())

    events_by_campaign_id_to_kafka = (aggregation_event_type_by_campaign_id(enrichment_data)
                                      .selectExpr("to_json(struct(*)) AS value")
                                      .writeStream
                                      .format("kafka")
                                      .option("kafka.bootstrap.servers", "localhost:9092")
                                      .option("topic", "events-campaign-aggregation")
                                      .option("checkpointLocation", "./checkpoints/checkpoint2")
                                      .outputMode("update")
                                      .start())

    events_by_country_to_kafka.awaitTermination()
    events_by_campaign_id_to_kafka.awaitTermination()


if __name__ == '__main__':
    spark = (SparkSession.
             builder.
             appName("Realtime Aggregations").
             master("local[*]").
             config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").
             config("spark.jars", "/home/santiago/PycharmProjects/marketing_analytics/packages/postgresql-42.7.1.jar").
             config("spark.sql.adaptive.enabled", "false").
             getOrCreate())

    aggregate_by_campaign_id(spark)