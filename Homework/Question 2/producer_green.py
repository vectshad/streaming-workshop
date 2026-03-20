import json
import os
import pandas as pd
from kafka import KafkaProducer
from time import time


def json_serializer(data):
    return json.dumps(data, default=str).encode('utf-8')


def safe_str(val):
    if pd.isna(val):
        return None
    return val.strftime('%Y-%m-%d %H:%M:%S')


def safe_float(val):
    if pd.isna(val):
        return None
    return float(val)


def safe_int(val):
    if pd.isna(val):
        return None
    return int(val)


server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print("Connected:", producer.bootstrap_connected())

# Read local parquet file (placed in workspace root)
local_path = os.path.join(os.path.dirname(__file__), '..', '..', 'green_tripdata_2025-10.parquet')
local_path = os.path.abspath(local_path)
print(f"Reading {local_path} ...")

df = pd.read_parquet(
    local_path,
    columns=[
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount',
        'total_amount',
    ]
)

print(f"Loaded {len(df)} rows")

topic_name = 'green-trips'

t0 = time()

for _, row in df.iterrows():
    message = {
        'lpep_pickup_datetime': safe_str(row['lpep_pickup_datetime']),
        'lpep_dropoff_datetime': safe_str(row['lpep_dropoff_datetime']),
        'PULocationID': safe_int(row['PULocationID']),
        'DOLocationID': safe_int(row['DOLocationID']),
        'passenger_count': safe_float(row['passenger_count']),
        'trip_distance': safe_float(row['trip_distance']),
        'tip_amount': safe_float(row['tip_amount']),
        'total_amount': safe_float(row['total_amount']),
    }
    producer.send(topic_name, value=message)

producer.flush()

t1 = time()
took = t1 - t0
print(f'Took {took:.2f} seconds to send {len(df)} messages')
