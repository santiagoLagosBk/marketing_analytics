import pandas as pd
import streamlit as st
import psycopg2
import time

from kafka import KafkaConsumer, TopicPartition

from dotenv import load_dotenv
import os
import simplejson as json
import sys

load_dotenv()

LIMIT=100

@st.cache_data
def fetch_basic_stats() -> tuple:
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM public.campaign;
    """)

    campaigns_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM public.events;
    """)

    events_count = cur.fetchone()[0]

    return campaigns_count, events_count


def create_kafka_consumer() -> KafkaConsumer:

    consumer = KafkaConsumer(
        'events-country-aggregation',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for record in message:
            data.append(record.value)
    return data


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f'Refresh time: {time.strftime("%Y-%m-%d %H:%M:%S")}')

    campaigns_count, events_count = fetch_basic_stats()

    # Display statistics
    st.markdown("""----""")
    col1, col2 = st.columns(2)
    col1.metric("Total campaigns", campaigns_count)
    col2.metric("Total events", events_count)

    consumer_ob = create_kafka_consumer()
    data_raw = fetch_data_from_kafka(consumer_ob)
    df_city_aggregation = pd.DataFrame(data_raw)
    st.table(df_city_aggregation.groupby('city').agg({'count': 'sum'}).reset_index())


if __name__ == '__main__':
    update_data()