import streamlit as st
import pandas as pd
from functools import wraps

import os
from dotenv import load_dotenv

import psycopg2


load_dotenv()

host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')


def create_connection():
    connection_string = f'host={host} dbname={database} user={user} password={password}'
    return psycopg2.connect(connection_string)


def with_connection(func):
    @wraps(func)
    def with_connection_wrapper(*args, **kwargs):
        conn = create_connection()
        try:
            result = func(conn, *args, **kwargs)
        finally:
            conn.close()
        return result

    return with_connection_wrapper


@st.cache_data(ttl=2)
@with_connection
def get_name_campaigns(conn, id_campaigns: list) -> pd.DataFrame:
    query = """
        SELECT uuid_campaign, name 
        FROM public.campaign 
        WHERE uuid_campaign IN %s;
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (tuple(id_campaigns),))
        results = cursor.fetchall()
    df = pd.DataFrame(results, columns=['uuid_campaign', 'name'])
    return df


@st.cache_data(ttl=2)
@with_connection
def fetch_count_total_events(conn) -> int:
    query = "SELECT COUNT(*) AS count_events FROM public.events;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        count_total_events = cursor.fetchone()[0]
    return count_total_events


@st.cache_data
@with_connection
def fetch_count_total_customers(conn) -> int:
    query = "SELECT COUNT(*) AS count_customers FROM public.customers;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        count_total_customers = cursor.fetchone()[0]
    return count_total_customers


@st.cache_data(ttl=2)
@with_connection
def fetch_most_loved_products(conn) -> list:
    query = """
        WITH events_count AS (
            SELECT product_id, COUNT(product_id) count_product
            FROM public.events
            WHERE product_id IS NOT NULL
            GROUP BY product_id 
            ORDER BY count_product DESC 
            LIMIT 10
        )
        SELECT pr.title, pr.image, pr.category, ev.count_product, 
               DENSE_RANK() OVER(order by ev.count_product DESC) as rank
        FROM events_count ev
        INNER JOIN public.product pr ON ev.product_id = pr.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        most_loved_products = cursor.fetchall()
    return most_loved_products
