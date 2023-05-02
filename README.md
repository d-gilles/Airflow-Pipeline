# Airflow Data Ingestion Pipeline

This project demonstrates a data ingestion pipeline for processing New York Taxi data using Apache Airflow. One DAG uses Google Cloud Storage to store the date and Google BigQuery. The pipeline downloads taxi trip data, checks if it is consistent, uploads it to Google Cloud Storage, and creates an external table in BigQuery. An other DAG does the same steps, but saves the date in a local Postgres database. All needed processes are running in separate Docker containers.

## Overview

The project is organized into the following components:

### Apache Airflow

Apache Airflow is used for orchestrating the data ingestion pipeline. The `dags/data_ingestion_gcp_dag.py` file contains the DAG definitions for the data ingestion pipeline, which includes the following tasks:


1. `download_dataset_task`: Downloads the taxi trip data in Parquet format from a public URL, and stores it in Google Cloud Storage.
2. `check_parquet_task`: Reads the files into a Pandas data frame, checks if the dtypes are correct, converts dates into datetime and saves it again to Parquet format using the PyArrow library.
3. `local_to_gcs_task`: Uploads the Parquet file to Google Cloud Storage.
4. `bigquery_external_table_task`: Creates an external table in BigQuery that references the Parquet file in Google Cloud Storage.

### PostgreSQL and pgAdmin

A PostgreSQL database is used for storing Airflow metadata. The `postgres/docker-compose.yaml` file defines the PostgreSQL and pgAdmin services. The `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` environment variables are set in the `.env` file to configure the PostgreSQL settings.

### Google Cloud Storage

Google Cloud Storage is used for storing the Parquet files generated during the pipeline execution. The `GCP_PROJECT_ID` and `GCP_GCS_BUCKET` environment variables in the `.env` file need to be set to the corresponding values of your Google Cloud project and storage bucket.

### Google BigQuery

Google BigQuery is used for creating an external table that references the Parquet files stored in Google Cloud Storage. The `BIGQUERY_DATASET` environment variable in the `.env` file should be set to the desired BigQuery dataset name.

## Setup

1. Clone the repository to your local machine.
2. Configure your Google Cloud credentials by setting the `GOOGLE_APPLICATION_CREDENTIALS` variable in the `.env` file to the path of your Google Cloud JSON key file.
3. Update the environment variables in the `.env` file to match your Google Cloud project settings and PostgreSQL settings.
4. Install the required Python packages by running:

```bash
pip install -r requirements.txt
