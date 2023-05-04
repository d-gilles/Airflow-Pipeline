import os
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from data_handling import read_data, convert_date, check_dtypes_match, save_parquet, ingest_local

# Required columns and dtypes
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

# Timestamp columns
timestamp_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

ingest_local_dag = DAG(
    'ingest_local_dag',
    description='Pipeline to ingest data to local postgres',
    schedule_interval='0 16 5 * *',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2022, 4, 1),
    tags=['ingest', 'local'],
    concurrency=1,
    max_active_runs=2
)

# Download file variables
url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
download_date = '{{(execution_date + macros.timedelta(days=-122)).strftime(\'%Y-%m\')}}'
filename = f'yellow_tripdata_{download_date}.parquet'
dataset_url = url_prefix + filename

# Save file to local variables
path_to_local_home = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "data")
output_path = f'{path_to_local_home}/{filename}'

# Postgres connection variables
pg_host = os.environ.get('DATA_POSTGRES_HOST')
pg_user = os.environ.get('DATA_POSTGRES_USER')
pg_password = os.environ.get('DATA_POSTGRES_PASSWORD')
pg_db = os.environ.get('DATA_POSTGRES_DB')
pg_table = os.environ.get('DATA_POSTGRES_TABLE')

if not os.path.exists(path_to_local_home):
    os.mkdir(path_to_local_home)

with ingest_local_dag:
    download = BashOperator(
        task_id='01_download',
        bash_command=f'curl -sSL {url_prefix}{filename} > {output_path} ',
        do_xcom_push=False,
    )

    read_file = PythonOperator(
        task_id='02_read_file',
        python_callable=read_data,
        op_kwargs={
            "input_fn": filename,
            "path_to_local_home": path_to_local_home,
        },
    )

    convert_file = PythonOperator(
        task_id='03_convert_date',
        python_callable=convert_date,
        op_kwargs={
            'timestamp_columns': timestamp_columns,
        }
    )

    check_dtypes = PythonOperator(
        task_id='04_check_dtypes',
        python_callable=check_dtypes_match,
        op_kwargs={
            'required_columns': required_columns,
        }
    )

    save_file = PythonOperator(
        task_id='04_save_file',
        python_callable=save_parquet,
        op_kwargs={
            "input_fn": filename,
            "output_fn": f"o-{filename}",
            "path_to_local_home": path_to_local_home,
        },
    )

    ingest_to_db = PythonOperator(
        task_id='05_ingest_parquet',
        python_callable=ingest_local,
        op_kwargs={
            "output_fn": f"o-{filename}",
            "pg_host": pg_host,
            "pg_user": pg_user,
            "pg_password": pg_password,
            "pg_db": pg_db,
            "pg_table": f'yellow_{download_date}',
            "path_to_local_home": path_to_local_home,
        },
    )

# Define the task sequence
download >> read_file >> convert_file >> check_dtypes >> save_file >> ingest_to_db
