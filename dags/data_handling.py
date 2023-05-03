import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from sqlalchemy import create_engine
import datetime
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


def read_data(input_fn,path_to_local_home, ti):
    """Read data into a dataframe"""
    file_path = f'{path_to_local_home}/{input_fn}'

    df = pd.read_parquet(file_path)

    new_file_path = file_path.replace('parquet','task1.pkl')
    pd.to_pickle(df, new_file_path)
    print(f"Read {len(df)} rows and {len(df.columns)} columns from {file_path}")

    os.remove(file_path)
    ti.xcom_push(key='path', value=new_file_path)
    ti.xcom_push(key='filename', value=input_fn)
    return

def convert_date(ti, timestamp_columns):
    """Convert string to date format is not so already"""
    file_path = ti.xcom_pull(key='path', task_ids='02_read_file')
    print(f"Reading from {file_path}")

    df = pd.read_pickle(file_path)
    if df[timestamp_columns[0]].dtype == 'datetime64[ns]' and df[timestamp_columns[1]].dtype == 'datetime64[ns]':
        print("Dates already converted")
    else:
        print("Converting dates")
        df[timestamp_columns[0]] = pd.to_datetime(df[timestamp_columns[0]])
        df[timestamp_columns[1]] = pd.to_datetime(df[timestamp_columns[1]])

    new_file_path = file_path.replace('task1','task2')
    print(f"Saving to {new_file_path}")
    pd.to_pickle(df, new_file_path)
    os.remove(file_path)
    ti.xcom_push(key='path', value=new_file_path)
    return

def check_dtypes_match(ti, required_columns):
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

    new_file_path = file_path.replace('task3.pkl','final.parquet')
    #new_file_path_csv = file_path.replace('task3.pkl','final.csv')

    table = pa.Table.from_pandas(df)
    pq.write_table(table, new_file_path)
    #df.to_csv(new_file_path_csv)

    os.remove(file_path)
    print(f"Saved {len(df)} rows and {len(df.columns)} columns to {new_file_path}")
    #print(f"Saved {len(df)} rows and {len(df.columns)} columns to {new_file_path_csv}")

    ti.xcom_push(key='path', value=new_file_path)
    #ti.xcom_push(key='path_csv', value=new_file_path_csv)
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

def upload_to_gcs(bucket, ti):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param ti: task instance
    :return:
    """
    path_parquet = ti.xcom_pull(key='path', task_ids='04_save_file')
    object_name = ti.xcom_pull(key='filename', task_ids='02_read_file')
    print(f"Reading from {path_parquet}")

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    #storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    #storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    print(f"Uploading to {bucket}/{object_name}")
    client = storage.Client()
    bucket_ = client.bucket(bucket)

    blob = bucket_.blob(object_name)
    blob.upload_from_filename(path_parquet)
    print(f"Uploaded to {bucket}/{object_name}")
    print(f'Deleting {path_parquet}')
    os.remove(path_parquet)
    ti.xcom_push(key='gcs', value=f"gs://{bucket}/{object_name}")
    return

def create_bigquery_external_table(PROJECT_ID,BIGQUERY_DATASET,ti,**context):
    # ti = context['ti']
    bucket_file = ti.xcom_pull(task_ids='05_local_to_gcs_task', key='gcs')
    print(f'{bucket_file} is the bucket file')
    execution_date = context['execution_date']

    bucket_source = ''.join((bucket_file.split('-')[0],'*'))
    print(f'{bucket_source} is the bucket source')
    year = execution_date.strftime('%Y')
    print(f'{year} is the year')
    table_name = ''.join(('external_',bucket_source.split('/')[-1].split('.')[0])).replace('*','')
    print(f'{table_name} is the table name')

    if int(execution_date.strftime('%m')) == 12:
        print('This is December')
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": table_name,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [bucket_source],
                },
            },
        )

        bigquery_external_table_task.execute(context)
    else:
        print('This is not December')
