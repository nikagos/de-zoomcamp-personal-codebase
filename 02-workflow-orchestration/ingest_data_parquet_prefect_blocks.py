#!/usr/bin/env python
# coding: utf-8


import argparse, os, sys
from time import time
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True)
def download_data(url: str) -> pd.DataFrame:
   # Get the name of the file from url
   file_name = url.rsplit('/', 1)[-1].strip()
   print(f'Downloading {file_name} ...')
   # Download file from url
   os.system(f'curl {url.strip()} -o {file_name}')
   print('\n')

   #get the current working directory
   cwd = os.getcwd()


   file_path = "%s/%s" % (cwd, file_name)
   df = pd.read_parquet(file_path, engine='fastparquet')

   return df


@task(log_prints=True)
def transform_data(df):

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    return df



@task(log_prints=True)
def load_data(table_name: str, df: pd.DataFrame) -> None:
    
    database_block = SqlAlchemyConnector.load("postgres-connector")

    with database_block.get_connection(begin=False) as engine:

        print('Creating table in the database: %s' % table_name)
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        print("Table created.")

        t_start = time()
        print(t_start)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()  
        print(t_end)
        print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds')

    return


@flow(name="Ingest Parquet Data into Postgres database", log_prints=True)
def main(args):
    table_name = args.table_name
    url = args.url

    df = download_data(url)
    df_transformed = transform_data(df)
    load_data(table_name, df_transformed)
    print("Process completed.")



if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

   parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
   parser.add_argument('--url', required=True, help='url of the parquet file')

   args = parser.parse_args()

   main(args)
