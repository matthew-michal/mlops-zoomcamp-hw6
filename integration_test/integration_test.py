import pandas as pd
import os
from datetime import datetime
import boto3
import s3fs
from pyarrow.fs import S3FileSystem

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')


s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,  # Default LocalStack S3 endpoint
        aws_access_key_id='test',             # LocalStack default credentials
        aws_secret_access_key='test',
        region_name='us-east-1',              # LocalStack default region
        verify=False                          # Disable SSL verification for local setup
    )

fs = S3FileSystem(
    access_key='test',
    secret_key='test',
    endpoint_override=S3_ENDPOINT_URL
)

input_file = 'nyc-duration/in/2023-01.parquet'

data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    filesystem=fs
)