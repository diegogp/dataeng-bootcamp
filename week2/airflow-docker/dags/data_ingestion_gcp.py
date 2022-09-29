import os

from datetime import datetime
from airflow.utils.dates import days_ago

from airflow import DAG
from google.cloud import storage
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from ingestion import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

def upload_to_gcs(bucket, object_name, local_file):
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


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        print("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)

def download_upload_dag(
    dag,
    url,
    local_path,
    local_parquet_path,
    gcs_path
    ):
    with dag:

        wget_task = BashOperator(
            task_id='wget',
            bash_command=f'curl -sSLf {url} > {local_path}'
        )
        
        format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": local_path,
                    "dest_file": local_parquet_path
                },
        )

        upload_to_gcs_task = PythonOperator(
            task_id='upload_to_gcs',
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path, #DEST
                "local_file": local_parquet_path, #SRC

            }
        )
        
        rm_temp_files_task = BashOperator(
            task_id="rm_temp_files",
            bash_command=f'rm {local_parquet_path}'

        )

        wget_task >> format_to_parquet_task >> upload_to_gcs_task >> rm_temp_files_task 

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YELLOW_TAXI_URL = URL_PREFIX + 'yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_OUTPUT_PATH = AIRFLOW_HOME + '/yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_OUTPUT_PATH = 'raw/yellow_tripdata/{{ data_interval_start.strftime(\'%Y\') }}/yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_TABLE_NAME = 'yellow_taxi_data'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
    }

yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'])

download_upload_dag(
    dag=yellow_taxi_data_dag,
    url=YELLOW_TAXI_URL,
    local_path=YELLOW_TAXI_OUTPUT_PATH,
    local_parquet_path=YELLOW_TAXI_OUTPUT_PATH,
    gcs_path=YELLOW_TAXI_GCS_OUTPUT_PATH
)

GREEN_TAXI_URL = URL_PREFIX + 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_OUTPUT_PATH = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_GCS_OUTPUT_PATH = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


green_taxi_data_dag = DAG(
    dag_id="green_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'])

download_upload_dag(
    dag=green_taxi_data_dag,
    url=GREEN_TAXI_URL,
    local_path=GREEN_TAXI_OUTPUT_PATH,
    local_parquet_path=GREEN_TAXI_OUTPUT_PATH,
    gcs_path=GREEN_TAXI_GCS_OUTPUT_PATH
)


FHV_TAXI_URL = URL_PREFIX + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_OUTPUT_PATH = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_GCS_OUTPUT_PATH = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'])

download_upload_dag(
    dag=fhv_taxi_data_dag,
    url=FHV_TAXI_URL,
    local_path=FHV_TAXI_OUTPUT_PATH,
    local_parquet_path=FHV_TAXI_OUTPUT_PATH,
    gcs_path=FHV_TAXI_GCS_OUTPUT_PATH
)



ZONES_URL = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_CSV_FILE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_OUTPUT_PATH = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
ZONES_GCS_OUTPUT_PATH = "raw/taxi_zone/taxi_zone_lookup.parquet"



zones_data_dag = DAG(
    dag_id="zones_data_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=zones_data_dag,
    url=ZONES_URL,
    local_path=ZONES_CSV_FILE,
    local_parquet_path=ZONES_OUTPUT_PATH,
    gcs_path=ZONES_GCS_OUTPUT_PATH
)