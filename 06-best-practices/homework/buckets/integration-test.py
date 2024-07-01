# export AWS_ACCESS_KEY_ID="test"
# export AWS_SECRET_ACCESS_KEY="test"
# export AWS_DEFAULT_REGION="us-east-1"
# export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
# export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
# export S3_ENDPOINT_URL="http://localhost:4566"

from datetime import datetime
import pandas as pd
import os

def prepare_data(df, categorical):
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def to_bucket():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    df_input = prepare_data(df, ['PULocationID', 'DOLocationID'])

    options = {
        'client_kwargs': {
            'endpoint_url': os.environ['S3_ENDPOINT_URL']
        }
    }


    df_input.to_parquet(
        get_input_path(2023, 1),
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

if __name__ == '__main__':
    to_bucket()
    print("successfully finished!")


# aws s3 ls s3://nyc-duration --endpoint-url=http://localhost:4566 --recursive --human-readable --summarize
# 2024-07-01 02:48:51    3.9 KiB in/2023-01.parquet

# Total Objects: 1
#    Total Size: 3.9 KiB