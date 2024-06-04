from bin.settings.db_settings.tables_settings import Tables

from bin.settings.mock_data_settings import MockDataSettings, MockDataParameters

import psycopg2
import logging


from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')


if __name__ == '__main__':

    connection_string = 'host={} dbname={} user={} password={}'.format(host, database, user, password)

    logger.info(connection_string)
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    # At this point is created all the necessary tables for the architecture
    tables = Tables(conn, cursor)
    tables.create_tables()

    # Populate the tables with data
    customers_count = 100
    mock_data_params = MockDataParameters(host=host, port=port, database=database,
                                          user=user, password=password, logger=logger,
                                          customers_count=customers_count)

    response = MockDataSettings.create_mock_data(mock_params=mock_data_params)



