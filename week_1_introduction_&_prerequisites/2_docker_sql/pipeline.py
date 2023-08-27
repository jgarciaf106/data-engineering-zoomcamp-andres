import pandas as pd
import wget
import gzip
import time
from sqlalchemy import create_engine

wget.download('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz')

# Unzip data
with gzip.open('yellow_tripdata_2021-01.csv.gz', 'rb') as f_in:
    with open('yellow_tripdata_2021-01.csv', 'wb') as f_out:
        f_out.write(f_in.read())

# delete all gz files

# read data
df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.sample(5)

engine =  create_engine('postgresql://root:root@localhost:5432/ny_taxi')


engine.connect()

table_ddl = pd.io.sql.get_schema(df, 'yellow_taxi_data')

df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql('yellow_taxi_data', engine, if_exists='replace', index=False)

# insert data in chunks
for df in df_iter:
    start_time = time.time()
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql('yellow_taxi_data', engine, if_exists='append', index=False)
    end_time = time.time()
    print("Chunk Data inserted..., time elapsed: ", end_time - start_time)