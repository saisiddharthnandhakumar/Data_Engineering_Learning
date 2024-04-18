import os
import argparse
import pandas as pd
from time import time

from sqlalchemy import create_engine
from sqlalchemy import text

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    #download the csv
    os.execute(f"wget {url} -O {csv_name}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    connection = engine.connect()

    df_iter = pd.read_csv(csv_name, iterator = True, chunksize = 100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name = table_name, con = engine, if_exists = "replace")

    df.to_sql(name = table_name, con = engine, if_exists = "append")

    while True:
        t_start = time()
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name = table_name, con = engine, if_exists = "append")

        t_end = time()
        print("Inserted another chunk..., took %.3f seconds" %(t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    #user, password, host, port, database name , table name
    #URL of the csv

    parser.add_argument('--user', help='username for Postgres')
    parser.add_argument('--pass', help='password for Postgres')
    parser.add_argument('--host', help='host for Postgres')
    parser.add_argument('--port', help='port for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table where we will write results to ')
    parser.add_argument('--url', help='url of the csv')

    args = parser.parse_args()

    main(args)

