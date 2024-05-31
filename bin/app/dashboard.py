import time
from dotenv import load_dotenv
import os
import simplejson as json

import streamlit as st
import psycopg2
import pandas as pd
from kafka import KafkaConsumer

# Load environment variables from .env file
load_dotenv()

host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')


@st.cache_data
def fetch_events_stats():
    connection_string = 'host={} dbname={} user={} password={}'.format(host, database, user, password)
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT COUNT(*) AS count_events FROM public.events;""")
            count_total_events = cursor.fetchone()[0]

            cursor.execute("""SELECT COUNT(*) AS total_events FROM public.customers;""")
            count_total_customers = cursor.fetchone()[0]

            cursor.execute("""
                WITH events_count AS (
                    SELECT product_id, COUNT(product_id) count_product
                    FROM public.events
                    WHERE product_id IS NOT NULL
                    GROUP BY product_id ORDER BY count_product DESC LIMIT 10
                )
                SELECT pr.title,pr.image,pr.category,ev.count_product, DENSE_RANK() OVER(order by ev.count_product DESC) as rank
                FROM events_count ev
                INNER JOIN public.product pr
                ON ev.product_id = pr.id;
            """)
            most_loved_products = cursor.fetchall()

    return count_total_events, count_total_customers, most_loved_products


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda value: json.loads(value.decode('utf-8')),
    )
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data


@st.cache_data
def fetch_campaign_city_stats():
    consumer = create_kafka_consumer("events-city-aggregation")
    data = fetch_data_from_kafka(consumer)
    if len(data) == 0:
        data = {
            'campaign_id': ['None'],
            'city': ['None'],
            'count': [0]
        }

    return pd.DataFrame(data).sort_values(by='count', ascending=False).head(10)


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f'Last refresh at: {time.strftime("%Y-%m-%d %H:%M:%S")}')

    # Fetch events statistics from PostgresSQL
    total_events, total_customers, most_loved_products = fetch_events_stats()

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric('Total events', total_events)
    col2.metric('Total customers', total_customers)

    st.markdown("""---""")
    st.title('Best Ranked Product')

    columns = ['title', 'image', 'category', 'count_product', 'rank']

    # Create a DataFrame
    df = pd.DataFrame(most_loved_products, columns=columns)
    best_product = df[df['rank'] == 1].iloc[0]

    col1, col2 = st.columns([1, 2])


    with col1:
        st.image(best_product['image'], width=200)

    # Display the details in the second column
    with col2:
        st.header(best_product['title'])
        st.subheader(f"Category: {best_product['category']}")
        st.subheader(f"Total Count: {best_product['count_product']}")


if __name__ == '__main__':
    st.title("Real Time Dashboard")
    update_data()
