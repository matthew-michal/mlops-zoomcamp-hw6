#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import os
import pandas as pd
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
import s3fs

def read_data(filename):
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

    if S3_ENDPOINT_URL is not None:
        options = {
            'client_kwargs': {
                'endpoint_url': S3_ENDPOINT_URL,
                'aws_access_key_id': 'test',
                'aws_secret_access_key': 'test',
            }
        }
        s3_fs = s3fs.S3FileSystem(
        client_kwargs={
            'endpoint_url': S3_ENDPOINT_URL,
                'aws_access_key_id': 'test',
                'aws_secret_access_key': 'test'
        },
        secret_key='test',
        access_key='test'
    )
        s3fs_instance = S3FileSystem(
            access_key='test',
            secret_key='test',
            endpoint_override=S3_ENDPOINT_URL
        )
        # df = pd.read_parquet(filename, storage_options=options)
        # df = pd.read_parquet(filename, storage_options={'s3fs': s3_fs, 'anon':False})
        df = pq.ParquetDataset(
            filename[5:], #this instance wants a path so remove the s3://
            filesystem=s3fs_instance,
            # storage_options={"anon":False}
        ).read_pandas().to_pandas()
    else:
        df = pd.read_parquet(filename)



    
    return df

def prepare_data(df, categorical):    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    categorical = ['PULocationID', 'DOLocationID']


    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)


    df = read_data(input_file)
    df = prepare_data(df, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred


    df_result.to_parquet(output_file, engine='pyarrow', index=False)

if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    main(year, month)