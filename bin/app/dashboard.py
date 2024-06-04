import time
import os
import simplejson as json
from kafka import KafkaConsumer

import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

# Load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')


st.set_page_config(layout="wide", page_title="Image Background Remover")


@st.cache_data
def get_name_campaigns(id_campaigns: list):
    connection_string = f'host={host} dbname={database} user={user} password={password}'
    query = """
            SELECT uuid_campaign, name 
            FROM public.campaign 
            WHERE uuid_campaign IN %s;
        """

    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (tuple(id_campaigns),))
            results = cursor.fetchall()

    # Convert the results to a pandas DataFrame
    df = pd.DataFrame(results, columns=['uuid_campaign', 'name'])

    return df


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


def plot_header(total_events, total_customers):
    col1, col2 = st.columns(2)
    col1.metric('Total events', total_events)
    col2.metric('Total customers', total_customers)


def plot_most_loved_products(most_loved_products):
    columns = ['title', 'image', 'category', 'count_product', 'rank']
    df = pd.DataFrame(most_loved_products, columns=columns)
    best_product = df[df['rank'] == 1].iloc[0]

    # Display the best-ranked product details
    col1, col2 = st.columns([1, 2])
    with col1:
        st.image(best_product['image'], width=200)
    with col2:
        st.header(best_product['title'])
        st.subheader(f"Category: {best_product['category']}")
        st.subheader(f"Clicks in the product: {best_product['count_product']}")


def plot_barplot_chart(df_data: pd.DataFrame):
    plt.bar(df_data['city'], df_data['count'])
    plt.tight_layout()
    return plt


def plot_effectiveness_chart():
    df_data = fetch_effect_stats()

    df_names = get_name_campaigns(df_data['campaign_id'].tolist())
    df_merge = pd.merge(df_data, df_names, how='inner', left_on='campaign_id', right_on='uuid_campaign')
    col1, col2 = st.columns(2)
    with col1:
        st.table(df_merge[['name', 'CLICKED', 'OPENED', 'effectiveness']])
    with col2:
        fig = px.pie(df_merge, values='CLICKED', names='name')
        st.plotly_chart(fig)

@st.cache_data
def fetch_campaign_city_stats():
    consumer = create_kafka_consumer("events-city-aggregation")
    data = fetch_data_from_kafka(consumer)
    df = pd.DataFrame(data)
    df_res = (df.groupby(['city']).agg({'count': 'sum'}).
              reset_index().
              rename(columns={'count': 'Total events'}).
              sort_values(by='Total events', ascending=False).head(10))
    return df_res

@st.cache_data
def fetch_effect_stats() -> pd.DataFrame:
    consumer = create_kafka_consumer("campaign-effectiveness")
    data = fetch_data_from_kafka(consumer)
    df_data = pd.DataFrame(data)
    df_effect = (df_data.
                 groupby(['campaign_id', 'type_event'])['total_events'].
                 count().
                 reset_index().
                 fillna(0).
                 pivot(index='campaign_id', columns='type_event', values='total_events'))

    df_effect['effectiveness'] = df_effect.apply(
        lambda row: (row['CLICKED'] / row['OPENED']) * 100
        if row['CLICKED'] <= row['OPENED'] else 100, axis=1)

    return df_effect.sort_values(by=['CLICKED', 'OPENED', 'effectiveness'], ascending=False).reset_index().head(10)


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f'Last refresh at: {time.strftime("%Y-%m-%d %H:%M:%S")}')

    # Fetch events statistics from PostgresSQL
    total_events, total_customers, most_loved_products = fetch_events_stats()

    # Display metrics in two columns
    st.markdown("""---""")
    plot_header(total_events, total_customers)
    plot_effectiveness_chart()

    # Display best-ranked product
    st.markdown("""---""")
    st.title('Best Ranked Product')
    plot_most_loved_products(most_loved_products)

    st.markdown("""---""")
    st.subheader('Cities with most events traffic')
    col1_city, col2_city = st.columns(2)
    df_city = fetch_campaign_city_stats()
    with col1_city:
        fig = px.bar(df_city, x='city', y='Total events', color=df_city['city'])
        st.plotly_chart(fig)
    with col2_city:
        st.dataframe(df_city)


if __name__ == '__main__':
    st.title("Real Time Dashboard")
    update_data()
