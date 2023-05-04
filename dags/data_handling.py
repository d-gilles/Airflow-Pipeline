import os
import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage
from sqlalchemy import create_engine
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator, BigQueryDeleteTableOperator


def read_data(input_fn, path_to_local_home, ti):
    """
    Convert specified columns in the DataFrame to datetime format if they are not already.

    :param ti: Airflow TaskInstance object.
    :type ti: airflow.models.TaskInstance
    :param timestamp_columns: A list of column names to be converted to datetime format.
    :type timestamp_columns: list
    """

    file_path = f'{path_to_local_home}/{input_fn}'
    df = pd.read_parquet(file_path)

    new_file_path = file_path.replace('parquet', 'task1.pkl')
    pd.to_pickle(df, new_file_path)
    print(f"Read {len(df)} rows and {len(df.columns)} columns from {file_path}")

    os.remove(file_path)
    ti.xcom_push(key='path', value=new_file_path)
    ti.xcom_push(key='filename', value=input_fn)
    return


def convert_date(ti, timestamp_columns):
    """
    Convert string to date format if not already in that format.
    """
    file_path = ti.xcom_pull(key='path', task_ids='02_read_file')
    print(f"Reading from {file_path}")

    df = pd.read_pickle(file_path)
    if df[timestamp_columns[0]].dtype == 'datetime64[ns]' and df[timestamp_columns[1]].dtype == 'datetime64[ns]':
        print("Dates already converted")
    else:
        print("Converting dates")
        df[timestamp_columns[0]] = pd.to_datetime(df[timestamp_columns[0]])
        df[timestamp_columns[1]] = pd.to_datetime(df[timestamp_columns[1]])

    new_file_path = file_path.replace('task1', 'task2')
    print(f"Saving to {new_file_path}")
    pd.to_pickle(df, new_file_path)
    os.remove(file_path)
    ti.xcom_push(key='path', value=new_file_path)
    return


def check_dtypes_match(ti, required_columns):
    """
    Check that the data types of the DataFrame match the required schema and make necessary conversions.

    :param ti: Airflow TaskInstance object.
    :type ti: airflow.models.TaskInstance
    :param required_columns: A dictionary of required column names and their corresponding data types.
    :type required_columns: dict
    """

    file_path = ti.xcom_pull(key='path', task_ids='03_convert_date')
    print(f"Reading from {file_path}")

    df = pd.read_pickle(file_path)
    changes = 0

    for column, dtype in required_columns.items():
        print(f"Checking {column} is {dtype}")

        if df[column].dtype != dtype:
            # change dytpes to required
            df.loc[:, column] = df[column].astype(dtype)
            print(f"Column {column} converted to {dtype}")
            changes += 1
    if changes == 0:
        print("No changes required, all dtypes match")

    new_file_path = file_path.replace('task2', 'task3')
    print(f"Saving to {new_file_path}")
    pd.to_pickle(df, new_file_path)
    os.remove(file_path)
    ti.xcom_push(key='path', value=new_file_path)
    return


def save_parquet(ti):
    """
    Save a pandas DataFrame as a Parquet file and push the file path to XCom.

    :param ti: Airflow TaskInstance object.
    :type ti: airflow.models.TaskInstance
    """

    file_path = ti.xcom_pull(key='path', task_ids='04_check_dtypes')
    print(f"Reading from {file_path}")
    df = pd.read_pickle(file_path)

    new_file_path = file_path.replace('task3.pkl', 'final.parquet')
    table = pa.Table.from_pandas(df)
    pq.write_table(table, new_file_path)

    os.remove(file_path)
    print(f"Saved {len(df)} rows and {len(df.columns)} columns to {new_file_path}")

    ti.xcom_push(key='path', value=new_file_path)
    return


def ingest_local(pg_host, pg_user, pg_password, pg_db, pg_table, ti):
    """
    Ingest data from a parquet file into a local PostgreSQL database.

    :param pg_host: The PostgreSQL host address.
    :type pg_host: str
    :param pg_user: The PostgreSQL user.
    :type pg_user: str
    :param pg_password: The PostgreSQL user's password.
    :type pg_password: str
    :param pg_db: The PostgreSQL database name.
    :type pg_db: str
    :param pg_table: The PostgreSQL table to store the data.
    :type pg_table: str
    :param ti: Airflow TaskInstance object.
    :type ti: airflow.models.TaskInstance
    """

    path_parquet = ti.xcom_pull(key='path', task_ids='04_save_file')
    print(f"Reading from {path_parquet}")
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:5432/{pg_db}')

    initial_time = datetime.datetime.now()
    df = pd.read_parquet(path_parquet)

    # creates a table in the database (engine) with no data in it
    df.head(0).to_sql(name=pg_table, con=engine, if_exists='replace')

    for i in range(0, len(df), 100000):
        start_time = datetime.datetime.now()
        df_part = df.iloc[i:i + 100000]
        df_part.to_sql(name=pg_table, con=engine, if_exists='append')
        end_time = datetime.datetime.now()
        print(f'Next chunk added in {int((end_time - start_time).total_seconds())} seconds, '
              f'now at {i + 100000} rows of {len(df)} in total')

    print(f"Saved {len(df)} rows and {len(df.columns)} columns to local postgres database")
    total_time = datetime.datetime.now() - initial_time
    print('Total time taken for parquet: %.2f minutes' % (total_time.total_seconds() / 60))
    os.remove(path_parquet)
    return



def upload_to_gcs(bucket, ti):
    """
    Upload a file to Google Cloud Storage (GCS) and push the GCS path to XCom.

    :param bucket: The name of the GCS bucket to upload the file to.
    :type bucket: str
    :param ti: Airflow TaskInstance object.
    :type ti: airflow.models.TaskInstance
    """

    path_parquet = ti.xcom_pull(key='path', task_ids='04_save_file')
    object_name = ti.xcom_pull(key='filename', task_ids='02_read_file')
    print(f"Reading from {path_parquet}")

    print(f"Uploading to {bucket}/{object_name}")
    client = storage.Client()
    bucket_ = client.bucket(bucket)

    blob = bucket_.blob(object_name)
    blob.upload_from_filename(path_parquet)
    print(f"Uploaded to {bucket}/{object_name}")
    os.remove(path_parquet)
    ti.xcom_push(key='gcs', value=f"gs://{bucket}/{object_name}")
    return


def create_bigquery_external_table(PROJECT_ID, BIGQUERY_DATASET, ti, datecolumn,  **context):
    """
    Create an external table in BigQuery based on a Google Cloud Storage Parquet file from one year,
    than creates a partitioned table based on this and removes the external table at the end.

    :param PROJECT_ID: The Google Cloud project ID.
    :type PROJECT_ID: str
    :param BIGQUERY_DATASET: The BigQuery dataset name where the external table will be created.
    :type BIGQUERY_DATASET: str
    :param ti: Airflow TaskInstance object.
    :type ti: airflow.models.TaskInstance
    :param datecolumn: The name of the column used for partitioning.
    :type datecolumn: str
    :param context: Airflow context dictionary that contains information like execution_date.
    :type context: dict
    """
    bucket_file = ti.xcom_pull(task_ids='05_local_to_gcs_task', key='gcs')
    print(f'{bucket_file} is the bucket file')
    execution_date = context['execution_date']

    bucket_source = ''.join((bucket_file.split('-')[0], '*'))
    print(f'{bucket_source} is the bucket source')
    table_name = ''.join(('external_', bucket_source.split('/')[-1].split('.')[0])).replace('*', '')
    partitioned_table_name = table_name + "_partitioned"
    year = int(execution_date.strftime('%Y'))

    if int(execution_date.strftime('%m')) == 12:
        print(f'This is December. Creating external table for the year {year} ')
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

        print(f'Creating partitioned table from the external table named {PROJECT_ID}.{BIGQUERY_DATASET}.{partitioned_table_name}')
        bigquery_partitioned_table_task = BigQueryInsertJobOperator(
            task_id='bigquery_partitioned_table_task',
            configuration={
                'query': {
                    'query': f"""

                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{partitioned_table_name}`
                        PARTITION BY
                            DATE({datecolumn}) AS
                        SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}`;
                    """,
                    'useLegacySql': False,
                }
            },
        )

        bigquery_partitioned_table_task.execute(context)

        bigquery_delete_external_table_task = BigQueryDeleteTableOperator(
            task_id='bigquery_delete_external_table_task',
            deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}",
            ignore_if_missing=True,
        )

        bigquery_delete_external_table_task.execute(context)
    else:
        print('This is not December. Not creating external table')

    return
