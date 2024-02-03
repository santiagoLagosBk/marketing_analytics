from settings import tables
import psycopg2
import os


host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
user = os.environ.get('USER')
password = os.environ.get('PASSWORD')



if __name__ == '__main__':

    connection_string = 'host={} dbname={} user={} password={}'.format(host, database, user, password)
    conn = psycopg2.connect(connection_string)