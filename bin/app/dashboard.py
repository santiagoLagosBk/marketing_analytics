import time
import simplejson as json
import pandas as pd
from kafka import KafkaConsumer
from statefull.dashboard_data_db import (get_name_campaigns, fetch_count_total_events,
                                         fetch_count_total_customers, fetch_most_loved_products)

import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt

st.set_page_config(layout="wide", page_title="Image Background Remover")


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


def main():
    last_refresh = st.empty()
    last_refresh.text(f'Last refresh at: {time.strftime("%Y-%m-%d %H:%M:%S")}')

    # Fetch events statistics from PostgresSQL
    total_events = fetch_count_total_events()
    total_customers = fetch_count_total_customers()
    most_loved_products = fetch_most_loved_products()

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
    main()
