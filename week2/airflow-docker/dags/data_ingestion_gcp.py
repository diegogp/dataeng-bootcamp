import os

from datetime import datetime
from airflow.utils.dates import days_ago

from airflow import DAG
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingestion import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

YELLOW_TAXI_URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YELLOW_TAXI_URL = YELLOW_TAXI_URL_PREFIX + 'yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_OUTPUT_PATH = AIRFLOW_HOME + '/yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_OUTPUT_PATH = 'raw/yellow_tripdata/{{ data_interval_start.strftime(\'%Y\') }}/yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_TABLE_NAME = 'yellow_taxi_data'

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    print(local_file)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
    }

with DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSLf {YELLOW_TAXI_URL} > {YELLOW_TAXI_OUTPUT_PATH}'
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": YELLOW_TAXI_GCS_OUTPUT_PATH, #DEST
            "local_file": YELLOW_TAXI_OUTPUT_PATH, #SRC

        }
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table"
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/{YELLOW_TAXI_GCS_OUTPUT_PATH}"],
    #         },
    #     },
    # )

    rm_temp_files_task = BashOperator(
        task_id="rm_temp_files",
        bash_command=f'rm {YELLOW_TAXI_OUTPUT_PATH}'

    )

    wget_task >> upload_to_gcs_task >> rm_temp_files_task # bigquery_external_table_task >>