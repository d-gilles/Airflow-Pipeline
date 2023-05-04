# New York Taxi Rides Data Ingestion Pipeline

After setting up an Airflow environment with Docker. We can now build some pipelines.

I this folder you find some Airflow-based data ETL (Extract, Transform, Load) pipeline for handling New York Taxi rides data. The pipeline downloads, processes, and uploads the data to Google Cloud Storage (GCS), and then creates an external table in BigQuery that points to the GCS bucket containing the Parquet files. The Airflow DAGs (Directed Acyclic Graphs) in this project are designed to automate the data ingestion process and facilitate the analysis of the taxi rides data.

## Airflow Components

This project uses the following Airflow components:

1. **DAG**: Directed Acyclic Graph, which represents a sequence of tasks that must be executed in a specific order.
2. **BashOperator**: An operator that executes a bash command.
3. **PythonOperator**: An operator that calls a Python function.
4. **Task**: A single unit of work that can be executed within a DAG.
5. **Python:** A Python file containing the methods used by the DAGs

## DAGs Overview

This project contains four DAG. All of them do similar work. They download data from New York Taxi Trips and store them in a database.

1. ingest_local_dag: This DAG downloads Yellow Taxi trip data, processes it, and ingests it into a local PostgreSQL database.
    - Download data from the source URL
    - Read the file
    - Convert date columns to the appropriate data type
    - Check if data types match the required schema
    - Save the processed file in Parquet format
    - Ingest the processed file into a local PostgreSQL database

 2. taxi_to_bq_dag (yellow, green, fhv): These DAGs downloads taxi trip data, performs the same data processing as DAG 1, but instead of ingesting in a local Postgres DB,  uploads the data to Google Cloud Storage. They then creates an external table for each year in Google BigQuery to query the data directly from the storage.



## Usage

1. Set up Airflow and configure the environment variables found in the `.env` file.( [As described here](./README.md))
2. Place the DAG and the `data_handling.py` file in the `dags` folder of your Airflow installation.
3. Trigger the DAG manually or let it run on its schedule.
