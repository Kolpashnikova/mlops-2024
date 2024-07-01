import pandas as pd
import os

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
    # rest of the main function ... 

options = {
    'client_kwargs': {
        'endpoint_url': os.environ('S3_ENDPOINT_URL')
    }
}

df = pd.read_parquet('s3://bucket/file.parquet', storage_options=options)


# export AWS_ACCESS_KEY_ID="test"
# export AWS_SECRET_ACCESS_KEY="test"
# export AWS_DEFAULT_REGION="us-east-1"
# export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
# export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
# export S3_ENDPOINT_URL="http://localhost:4566"