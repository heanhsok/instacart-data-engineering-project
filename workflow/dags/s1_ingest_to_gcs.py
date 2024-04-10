from datetime import datetime, timedelta
from textwrap import dedent
import time
import pendulum
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import os
import logging
from google.cloud import storage
import zipfile
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    " ": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

input_files_zip = [
    "order_products__prior.csv.zip",
    "order_products__train.csv.zip",
    "orders.csv.zip",
    "aisles.csv.zip",
    "products.csv.zip",
    "departments.csv.zip",
]

input_files_csv = [
    "order_products__prior.csv",
    "order_products__train.csv",
    "orders.csv",
    "aisles.csv",
    "products.csv",
    "departments.csv",
]


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
    blob.upload_from_filename(local_file)


def unzip_file(zip_file, OUTPUT_FILE_PATH):
    # check if zipfile exists
    if not os.path.exists(zip_file):
        raise FileNotFoundError(f"File {zip_file} not found")

    # unzip the zipfile
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(OUTPUT_FILE_PATH)
        print(f"File {zip_file} unzipped to {OUTPUT_FILE_PATH}")


def unzip_instacart_data():
    zip_file = "/opt/airflow/data/instacart-market-basket-analysis.zip"
    OUTPUT_FILE_PATH = "/opt/airflow/data/tmp"

    # unzip the main file
    unzip_file(zip_file, OUTPUT_FILE_PATH)

    # unzip individual files
    for file in input_files_zip:
        zip_file = f"/opt/airflow/data/tmp/{file}"
        print(zip_file)
        unzip_file(zip_file, OUTPUT_FILE_PATH)


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


with DAG(
    "s1_ingest_to_gcs",
    default_args=default_args,
    description="ingest instacart data into google cloud storage",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["instacart"],
) as dag:

    upzip_instacart_data = PythonOperator(
        task_id="upzip_instacart_data",
        python_callable=unzip_instacart_data,
    )

    with TaskGroup("format_to_parquet") as format_to_parquet_tg:
        for i, file in enumerate(input_files_csv):
            file_name = file.replace(".csv", "")
            task = PythonOperator(
                task_id=f"{file_name}",
                python_callable=format_to_parquet,
                op_kwargs={"src_file": f"/opt/airflow/data/tmp/{file}"},
            )

    with TaskGroup("upload_local_to_gcs") as local_to_gcs_tg:
        for i, file in enumerate(input_files_csv):
            parquet_file = file.replace(".csv", ".parquet")
            task = PythonOperator(
                task_id=f'{file.replace(".csv","")}',
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": f"raw/{parquet_file}",
                    "local_file": f"/opt/airflow/data/tmp/{parquet_file}",
                },
            )

    clean_up_local_files = BashOperator(
        task_id="clean_up_local_files",
        bash_command=f"rm -rf /opt/airflow/data/tmp",
    )

    (
        upzip_instacart_data
        >> format_to_parquet_tg
        >> local_to_gcs_tg
        >> clean_up_local_files
    )
