import argparse
import os
import pandas as pd
import wget
import time
from sqlalchemy import create_engine

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    csv_file_url = params.csv_file_url

    if csv_file_url.endswith('csv.gz'):
        csv_file_name = "output.csv.gz"
    else:
        csv_file_name = "output.csv"

    # download csv file
    print("Downloading csv file...\n")
    wget.download(csv_file_url, out=csv_file_name)
  
    # connect to database
    engine =  create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    df_iter = pd.read_csv(csv_file_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(table_name, engine, if_exists='replace', index=False)

    df.to_sql(table_name, engine, if_exists='append', index=False)

    try:
        # insert data in chunks
        print("\nIngesting data to PostgreSQL Started...")
        for df in df_iter:
            start_time = time.time()
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(table_name, engine, if_exists='append', index=False)
            end_time = time.time()
            print("\nChunk Data inserted..., time elapsed: ", end_time - start_time)
    
        print("\nData Ingested to PostgreSQL")
        os.remove(csv_file_name)
    except Exception as e:
        print("Error: ", e)
        print("Data Ingestion Failed")
   
if __name__ == '__main__': 
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL')

    parser.add_argument('--user', help='user name for database')
    parser.add_argument('--password', help='password for database')
    parser.add_argument('--host', help='host for database')
    parser.add_argument('--port', help='port for database')
    parser.add_argument('--database', help='database name')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--csv_file_url', help='csv file to ingest')

    args = parser.parse_args()

    main(args)