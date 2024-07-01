from datetime import datetime
import pandas as pd

def prepare_data(df, categorical):
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def test_prepare_data():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    df = prepare_data(df, ['PULocationID', 'DOLocationID'])

    df_dict = df.to_dict(orient='list')

    expected_data = {
        'PULocationID': ['-1', '1'],
        'DOLocationID': ['-1', '1'],
        'tpep_pickup_datetime': ['2023-01-01 01:01:00', '2023-01-01 01:02:00'],
        'tpep_dropoff_datetime': ['2023-01-01 01:10:00', '2023-01-01 01:10:00'],
        'duration': [9.0, 8.0]
    }

    expected_df = pd.DataFrame(expected_data)


    expected_df['tpep_pickup_datetime'] = pd.to_datetime(expected_df['tpep_pickup_datetime'])
    expected_df['tpep_dropoff_datetime'] = pd.to_datetime(expected_df['tpep_dropoff_datetime'])

    expected_df_dict = expected_df.to_dict(orient='list')

    assert df_dict == expected_df_dict