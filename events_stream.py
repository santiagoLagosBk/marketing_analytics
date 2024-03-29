import json
import time
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType, FloatType, BooleanType, TimestampType)
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from random import choice
from faker import Faker
import sys

from pyspark.sql import SparkSession

from dotenv import load_dotenv
import os

load_dotenv()

fake = Faker()
import random

host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')

sent_event = os.getenv("SENT_EVENT")
delivered_event = os.getenv("DELIVERED_EVENT")
opened_event = os.getenv("OPENED_EVENT")
clicked_event = os.getenv("CLICKED_EVENT")
forwarded_event = os.getenv("FORWARDED_EVENT")
spam_event = os.getenv("SPAM_EVENT")
hard_bounce_event = os.getenv("HARD_BOUNCE_EVENT")

EVENTS_LIST = [sent_event, delivered_event, opened_event, clicked_event, forwarded_event]

PROBABILITIES = [0.1, 0.3, 0.6, 0.2, 0.2]


class EventsStream:

    def __init__(self):
        pass

    def start_consuming(self, spark_obj):
        conf = {
            'bootstrap.servers': 'localhost:9092',  # Kafka broker(s) address
            'group.id': 'consumecustomertopic01',  # Consumer group ID
            'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the kafka topic
        }

        consumer = Consumer(conf)
        consumer.subscribe([str(os.getenv("CUSTOMERS_TOPIC"))])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                self.msg_process(msg, spark_obj)
            time.sleep(0.5)

    def create_random_event(self, event_name: str, user_agent: str) -> dict:
        Faker.seed(0)
        return {
            "type_event": event_name,
            "user_agent": user_agent,
            "event_date": datetime.utcnow()
        }

    def process_event(self, customer: dict) -> list[dict[str, Any | None]]:

        customer['customer_id'] = customer.pop('id')

        # Select event limit for the customer
        event_limit_for_customer = random.choices(EVENTS_LIST, weights=PROBABILITIES, k=1)[0]

        # Determine the number of events based on the selected event limit
        num_events = EVENTS_LIST.index(event_limit_for_customer)

        # Create a list of events up to the selected event limit
        list_final_events = EVENTS_LIST[:num_events]
        weight_option_error = [0.7, 0.3]

        # Randomly select if there will be any errors or not
        possible_errors = random.choices(['no-error', 'error'], weights=weight_option_error, k=1)[0]

        # Handle error events if the number of events is 1 or less and there's a chance of error
        if num_events <= 1 and possible_errors == 'error':
            option_error_events = [spam_event, hard_bounce_event]

            # Randomly select an error event and append it to the final events list
            event_to_append = random.choices(option_error_events, weights=weight_option_error, k=1)[0]
            list_final_events.append(event_to_append)

        user_agent = choice([
            fake.chrome(),
            fake.firefox(),
            fake.internet_explorer(),
            fake.safari(),
            fake.user_agent()])

        customer_events = []
        for event_name in list_final_events:
            selected_keys = ["customer_id", "campaign_id", "country", "gender", "email", "city", "state"]
            extra_event = self.create_random_event(event_name, user_agent)
            event = {key: customer.get(key) for key in selected_keys} | extra_event
            customer_events.append(event)

        return customer_events

    def msg_process(self, msg, spark_obj):
        customer = json.loads(msg.value().decode('utf-8'))
        events_for_customer = self.process_event(customer)

        kafka_options = {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": str(os.getenv("EVENTS_TOPIC"))
        }

        url = ('jdbc:postgresql://{}:{}/{}'.
               format(host, port, database))

        df_event_schema = StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("campaign_id", StringType(), False),
                StructField("type_event", StringType(), False),
                StructField("event_date", TimestampType(), False),
                StructField("gender", StringType()),
                StructField("email", StringType()),
                StructField("user_agent", StringType()),
                StructField("country", StringType()),
                StructField("city", StringType()),
                StructField("state", StringType())
            ])

        df_events = spark_obj.createDataFrame(events_for_customer, df_event_schema)

        (df_events.write.
         mode('append').
         format("jdbc").
         option("url", url).
         option("dbtable", 'events').
         option("user", user).
         option("password", password).
         option("driver", "org.postgresql.Driver").
         save())

        (df_events
         .selectExpr("CAST(customer_id AS STRING) AS key", "to_json(struct(*)) AS value")
         .write
         .format("kafka")
         .options(**kafka_options)
         .save())

        df_events.unpersist()


if __name__ == '__main__':
    spark = (SparkSession.builder.
             appName('Creation of events').
             master("local[1]").
             config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").
             config("spark.jars", "/home/santiago/PycharmProjects/marketing_analytics/packages/postgresql-42.7.1.jar").
             config("spark.sql.adaptive.enabled", "false").
             getOrCreate())

    event_stream = EventsStream()
    event_stream.start_consuming(spark_obj=spark)

    spark.stop()
