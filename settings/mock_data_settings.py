import requests
import logging
from dataclasses import dataclass

# Pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

BASE_URL_PRODUCTS = 'https://fakestoreapi.com/products'
BASE_URL_CUSTOMERS = 'https://randomuser.me/api/?nat=gb'


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
              master("local[1]").
              config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").
              config("spark.jars", "/home/santiago/PycharmProjects/marketing_analytics/packages/postgresql-42.7.1.jar").
              config("spark.sql.adaptive.enabled", "false").
              getOrCreate())

    @classmethod
    def _generate_product_data(cls):
        response = requests.get(BASE_URL_PRODUCTS)
        if response.status_code == 200:
            data = response.json()
            return data
        return None

    @classmethod
    def _create_product_df(cls, product_data: list[dict], spark_object, mock_params: MockDataParameters):
        if product_data is not None:
            for product in product_data:
                if 'price' in product:
                    try:
                        product['price'] = float(product['price'])
                    except ValueError:
                        # Handle cases where the price value cannot be converted to float
                        pass

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

            return 'successfully created'

        return 'something was wrong please try again'

    @classmethod
    def _create_customers_data(cls, params, spark_object):

        response = requests.get(BASE_URL_CUSTOMERS + f'&results={str(params.customers_count)}')
        if response.status_code != 200:
            return f'Error fetching data {response.reason}'

        user_data = response.json()['results']

        customer_list = [
            {
                'id': idx,
                'name_customer': '{} {}'.format(user['name']['first'], user['name']['last']),
                'description_customer': user['location']['timezone']['description'],
                'gender': user['gender'],
                'country': user['location']['country']
            }
            for idx, user in enumerate(user_data)
        ]

        customers_schema = StructType([
            StructField('id', IntegerType(), False),
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
            "topic": "customers"
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

        return 'successfully updated database'

    @classmethod
    def create_mock_data(cls, mock_params: MockDataParameters):
        try:
            # Create an instance of MockDataSettings to access instance attributes
            mock_data_settings = cls(mock_params)  # Pass appropriate arguments here

            # Access mock_data_object through the instance
            mock_data_settings._create_product_df(mock_data_settings._generate_product_data(),
                                                  mock_data_settings._spark, mock_params)

            mock_data_settings._create_customers_data(mock_params, mock_data_settings._spark)

            return 'successfully created'

        except Exception as e:
            # Access logger through the instance
            mock_params.logger.error(e)


