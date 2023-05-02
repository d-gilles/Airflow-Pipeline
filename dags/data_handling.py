import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from sqlalchemy import create_engine
import datetime
import time

# Required columns and data types

required_columns = {
'VendorID': 'int64',
'tpep_pickup_datetime': 'datetime64[ns]',
'tpep_dropoff_datetime': 'datetime64[ns]',
'passenger_count': 'float64',
'trip_distance': 'float64',
'RatecodeID': 'float64',
'store_and_fwd_flag': 'object',
'PULocationID': 'int64',
'DOLocationID': 'int64',
'payment_type': 'int64',
'fare_amount': 'float64',
'extra': 'float64',
'mta_tax': 'float64',
'tip_amount': 'float64',
'tolls_amount': 'float64',
'improvement_surcharge': 'float64',
'total_amount': 'float64',
'congestion_surcharge': 'float64',
'airport_fee': 'float64'
}


def read_data(input_fn,path_to_local_home, ti):
    """Read data into a dataframe"""
    df = pd.read_parquet(f'{path_to_local_home}/{input_fn}')
    pd.to_pickle(df, f'{path_to_local_home}/{input_fn}_task1.pkl')
    print(f"Read {len(df)} rows and {len(df.columns)} columns from {input_fn}")

    #os.remove(f'{path_to_local_home}/{input_fn}')
    ti.xcom_push(key='path', value=f'{path_to_local_home}/{input_fn}_task1.pkl')
    return {'path':f'{path_to_local_home}/{input_fn}'}

def convert_date(ti):
    """Convert string to date format is not so already"""
    file_path = ti.xcom_pull(key='path', task_ids='02_read_file')
    print(f"Reading from {file_path}")

    df = pd.read_pickle(file_path)
    if df['tpep_pickup_datetime'].dtype == 'datetime64[ns]' and df['tpep_dropoff_datetime'].dtype == 'datetime64[ns]':
        print("Dates already converted")
    else:
        print("Converting dates")
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    new_file_path = file_path.replace('task1','task2')
    print(f"Saving to {new_file_path}")
    pd.to_pickle(df, new_file_path)
    os.remove(file_path)
    ti.xcom_push(key='path', value=new_file_path)
    return

def check_dtypes_match(ti):
    """Check that the data types of the dataframe match the required schema"""
    file_path = ti.xcom_pull(key='path', task_ids='03_convert_date')

    print(f"Reading from {file_path}")
    df = pd.read_pickle(file_path)

    changes = 0
    print(df.dtypes)
    for column, dtype in required_columns.items():
        print(f"Checking {column} is {dtype}")

        if df[column].dtype != dtype:
            # change dytpes to required
            df.loc[:, column] = df[column].astype(dtype)
            print(f"Column {column} converted to {dtype}")
            changes += 1
    if changes == 0:
        print("No changes required, all dtypes match")

    new_file_path = file_path.replace('task2','task3')

    print(f"Saving to {new_file_path}")
    pd.to_pickle(df, new_file_path)
    print(f'Deleting {file_path}')
    os.remove(file_path)
    print(f'XCOM push {new_file_path}')
    ti.xcom_push(key='path', value=new_file_path)
    return


def save_parquet(ti):
    """Save a dataframe as a parquet file"""
    file_path = ti.xcom_pull(key='path', task_ids='04_check_dtypes')
    print(f"Reading from {file_path}")
    df = pd.read_pickle(file_path)

    new_file_path_parquet = file_path.replace('task3.pkl','final.parquet')
    new_file_path_csv = file_path.replace('task3.pkl','final.csv')

    table = pa.Table.from_pandas(df)
    pq.write_table(table, new_file_path_parquet)
    df.to_csv(new_file_path_csv)

    os.remove(file_path)
    print(f"Saved {len(df)} rows and {len(df.columns)} columns to {new_file_path_parquet}")
    print(f"Saved {len(df)} rows and {len(df.columns)} columns to {new_file_path_csv}")

    ti.xcom_push(key='path', value=new_file_path_parquet)
    ti.xcom_push(key='path_csv', value=new_file_path_csv)
    return

def ingest_local(pg_host, pg_user, pg_password, pg_db, pg_table, ti):
    """Ingest data to local running postgres database"""

    path_parquet = ti.xcom_pull(key='path', task_ids='04_save_file')
    print(f"Reading from {path_parquet}")
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:5432/{pg_db}')

    initial_time = datetime.datetime.now()
    df = pd.read_parquet(path_parquet)

    #creates a table in the database (engine) with no date in it
    df.head(0).to_sql(name=pg_table, con=engine, if_exists='replace')


    for i in range(0, len(df), 100000):
        start_time = datetime.datetime.now()
        df_part = df.iloc[i:i+100000]
        #time.sleep(1)
        df_part.to_sql(name=pg_table, con=engine, if_exists='append')
        end_time = datetime.datetime.now()
        print(f'Next chunk added in {int((end_time - start_time).total_seconds())} seconds, now at',i+100000,'rows','of',len(df),'in total')
    print(f"Saved {len(df)} rows and {len(df.columns)} columns to local postgres database")
    total_time = datetime.datetime.now() - initial_time
    print('Total time taken for parquet: %.2f minutes' % (total_time.total_seconds()/60) )
    os.remove(path_parquet)

    return
