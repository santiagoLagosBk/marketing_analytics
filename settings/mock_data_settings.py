import os

import requests
import logging
from dataclasses import dataclass
from faker import Faker
import random
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType, FloatType, BooleanType)

BASE_URL_PRODUCTS = 'https://fakestoreapi.com/products'
BASE_URL_CUSTOMERS = 'https://randomuser.me/api/?nat=gb'

fake = Faker()


@dataclass
class MockDataParameters:
    host: str
    port: str
    database: str
    user: str
    password: str
    logger: logging.Logger
    customers_count: int


@dataclass
class MockDataSettings:
    mock_data_object: MockDataParameters

    _spark = (SparkSession.builder.
              appName('Product Data').
              master("local[*]").
              config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").
              config("spark.jars", "/home/santiago/PycharmProjects/marketing_analytics/packages/postgresql-42.7.1.jar").
              config("spark.sql.adaptive.enabled", "false").
              config("spark.driver.memory", "8g").
              config("spark.executor.memory", "8g").
              getOrCreate())

    @classmethod
    def _generate_product_data(cls):
        response = requests.get(BASE_URL_PRODUCTS)
        if response.status_code == 200:
            data = response.json()
            return data
        return None

    @classmethod
    def _process_product(cls, product: dict):
        if 'price' in product:
            try:
                product['price'] = float(product['price'])
            except ValueError as e:
                logging.exception(e)
                product['price'] = 0.0

        return product

    @classmethod
    def _create_product_df(cls, product_data: list[dict], spark_object, mock_params: MockDataParameters):
        if product_data is not None:
            product_data = list(map(cls._process_product, product_data))

            product_schema = StructType([
                StructField('id', IntegerType(), False),
                StructField('title', StringType(), False),
                StructField('description', StringType(), False),
                StructField('category', StringType(), False),
                StructField('image', StringType(), False),
                StructField('price', FloatType(), False)
            ])

            df = spark_object.createDataFrame(product_data, product_schema)

            url = ('jdbc:postgresql://{}:{}/{}'.
                   format(mock_params.host, mock_params.port, mock_params.database))

            res = (df.write.
                   mode('overwrite').
                   format("jdbc").
                   option("url", url).
                   option("dbtable", 'product').
                   option("user", mock_params.user).
                   option("password", mock_params.password).
                   option("driver", "org.postgresql.Driver").
                   save())

            mock_params.logger.info(res)

            df.unpersist()

            return 'successfully created'

        return 'something was wrong please try again'

    @classmethod
    def _create_customers_data(cls, params, campaigns, spark_object):

        response = requests.get(BASE_URL_CUSTOMERS + f'&results={str(params.customers_count)}')
        if response.status_code != 200:
            return f'Error fetching data {response.reason}'

        user_data = response.json()['results']

        email_domains = ['gmail', 'hotmail']

        customer_list = []

        for id_campaign in campaigns:
            for idx, user in enumerate(user_data):
                customer_list.append({
                    'id': idx,
                    'campaign_id': id_campaign,
                    'email': '{}-{}@{}.com.co'.
                    format(
                        user['name']['first'],
                        (random.randint(1, 99999) * idx),
                        random.choice(email_domains)
                    ),
                    'name_customer': '{} {}'.format(user['name']['first'], user['name']['last']),
                    'description_customer': user['location']['timezone']['description'],
                    'gender': user['gender'],
                    'country': user['location']['country']
                })

        customers_schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('campaign_id', StringType(), False),
            StructField('email', StringType(), False),
            StructField('name_customer', StringType(), False),
            StructField('description_customer', StringType(), False),
            StructField('gender', StringType(), False),
            StructField('country', StringType(), False)
        ])

        df = spark_object.createDataFrame(customer_list, customers_schema)

        url = ('jdbc:postgresql://{}:{}/{}'.
               format(params.host, params.port, params.database))

        kafka_options = {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": str(os.getenv("CUSTOMERS_TOPIC"))
        }

        (df.write.
         mode('overwrite').
         format("jdbc").
         option("url", url).
         option("dbtable", 'customers').
         option("user", params.user).
         option("password", params.password).
         option("driver", "org.postgresql.Driver").
         save())

        (df
         .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
         .write
         .format("kafka")
         .options(**kafka_options)
         .save())

        df.unpersist()

        return 'successfully updated database'

    @classmethod
    def _create_campaign_mock_data(cls, params: MockDataParameters, spark_obj):

        Faker.seed(0)
        cryptocurrency_codes = [fake.cryptocurrency_code() for _ in range(5)]
        concatenated_codes = ', '.join(cryptocurrency_codes)

        campaigns_list = [{
            "uuid_campaign": str(uuid4()),
            "name": f"{fake.company()} Marketing Campaign {str(random.randint(i, 100000) * i)}",
            "created_by": fake.name(),
            "description": fake.sentence(),
            "valid_campaign": fake.boolean(),
            "tags": concatenated_codes
        } for i in range(1, 100)]

        try:
            url = ('jdbc:postgresql://{}:{}/{}'.format(params.host, params.port, params.database))

            campaign_schema = StructType([
                StructField('uuid_campaign', StringType(), False),
                StructField('name', StringType(), False),
                StructField('created_by', StringType(), False),
                StructField('description', StringType(), False),
                StructField('valid_campaign', BooleanType(), False),
                StructField('tags', StringType(), True)
            ])

            df = spark_obj.createDataFrame(campaigns_list, campaign_schema)

            (df.write.
             mode('overwrite').
             format("jdbc").
             option("url", url).
             option("dbtable", 'campaign').
             option("user", params.user).
             option("password", params.password).
             option("driver", "org.postgresql.Driver").
             save())

            df.unpersist()

        except Exception as e:

            params.logger.exception(e)
            return e

        campaign_resume = list(map(
                lambda x: {"campaign_id": x["uuid_campaign"],
                           "product_id": random.randint(1, 21),
                           "linked_by": x["created_by"]},
                campaigns_list))

        campaign_product_schema = StructType([
            StructField('product_id', IntegerType(), False),
            StructField('campaign_id', StringType(), False),
            StructField('linked_by', StringType(), False)
        ])

        df_interactions = spark_obj.createDataFrame(campaign_resume, campaign_product_schema)

        (df_interactions.write.
         mode('overwrite').
         format("jdbc").
         option("url", url).
         option("dbtable", 'campaign_product_interactions').
         option("user", params.user).
         option("password", params.password).
         option("driver", "org.postgresql.Driver").
         save())

        campaign_id_list = df_interactions.select("campaign_id").rdd.flatMap(lambda x: x).collect()

        df_interactions.unpersist()

        return campaign_id_list

    @classmethod
    def create_mock_data(cls, mock_params: MockDataParameters):
        try:
            # Create an instance of MockDataSettings to access instance attributes
            mock_data_settings = cls(mock_params)  # Pass appropriate arguments here

            # Access mock_data_object through the instance
            mock_data_settings._create_product_df(mock_data_settings._generate_product_data(),
                                                  mock_data_settings._spark, mock_params)

            campaigns = mock_data_settings._create_campaign_mock_data(mock_params, mock_data_settings._spark)

            mock_data_settings._create_customers_data(mock_params, campaigns, mock_data_settings._spark)

            mock_data_settings._spark.stop()
            return 'successfully created'

        except Exception as e:
            # Access logger through the instance
            mock_params.logger.error(e)
