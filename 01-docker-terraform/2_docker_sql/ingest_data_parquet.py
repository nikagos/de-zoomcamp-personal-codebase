#!/usr/bin/env python
# coding: utf-8


import argparse, os, sys
from time import time
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq




def main(params):
   user = params.user
   password = params.password
   host = params.host
   port = params.port
   db = params.db
   table_name = params.table_name
   url = params.url


   # Get the name of the file from url
   file_name = url.rsplit('/', 1)[-1].strip()
   print(f'Downloading {file_name} ...')
   # Download file from url
   os.system(f'curl {url.strip()} -o {file_name}')
   print('\n')
  
   engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
   print(engine)


   #get the current working directory
   cwd = os.getcwd()


   file_path = "%s/%s" % (cwd, file_name)
   df = pd.read_parquet(file_path, engine='fastparquet')


   df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
   df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


   print('Creating table in the database: %s' % table_name)
   df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
   print("Table created.")


   t_start = time()
   print(t_start)
   df.to_sql(name=table_name, con=engine, if_exists='append')
   t_end = time()  
   print(t_end)
   print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds')




if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')


   parser.add_argument('--user', required=True, help='user name for postgres')
   parser.add_argument('--password', required=True, help='password for postgres')
   parser.add_argument('--host', required=True, help='host for postgres')
   parser.add_argument('--port', required=True, help='port for postgres')
   parser.add_argument('--db', required=True, help='database name for postgres')
   parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
   parser.add_argument('--url', required=True, help='url of the parquet file')


   args = parser.parse_args()


   main(args)
