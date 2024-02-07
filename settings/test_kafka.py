import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


parent_path = os.getcwd()

if __name__ == '__main__':
    spark = (SparkSession.builder.
             appName('Product Data901').
             master("local[*]").
             config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").
             config("spark.jars", "../packages/postgresql-42.7.1.jar").
             config("spark.sql.adaptive.enabled", "false").
             getOrCreate())

    kafka_input_config = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "input",
        "startingOffsets": "latest",
        "checkpointLocation": parent_path+"/checkpoints/checkpointwriteEnqueue",
        "failOnDataLoss": "false",
        "groupIdPrefix": "dataconsumer"
    }

    kafka_output_config = {
        "kafka.bootstrap.servers": "localhost:9092",
        "topic": "output",
        "checkpointLocation": parent_path+"/checkpoints/checkpoint1"
    }

    df_schema = StructType([
        StructField("transaction_id",StringType(),True),
        StructField("customer_id", StringType(),True),
        StructField("amount", IntegerType(),True),
        StructField("transaction_timestamp", TimestampType(),True),
        StructField("merchant_id", StringType(),True)
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_input_config) \
        .load()

    print(df.printSchema())

    # Write the DataFrame to Kafka
    query = df \
        .selectExpr("CAST(key AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .outputMode("append") \
        .options(**kafka_output_config) \
        .start()

    query.awaitTermination()
