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

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator,
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.task_group import TaskGroup


# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
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
    OUTPUT_FILE_PATH = "/opt/airflow/data"

    # unzip the main file
    unzip_file(zip_file, OUTPUT_FILE_PATH)

    files = [
        "order_products__prior.csv.zip",
        "order_products__train.csv.zip",
        "orders.csv.zip",
        "aisles.csv.zip",
        "products.csv.zip",
        "departments.csv.zip",
    ]

    # unzip individual files
    for file in files:
        zip_file = f"/opt/airflow/data/{file}"
        print(zip_file)
        unzip_file(zip_file, OUTPUT_FILE_PATH)


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


input_files = [
    "order_products__prior.csv",
    "order_products__train.csv",
    "orders.csv",
    "aisles.csv",
    "products.csv",
    "departments.csv",
]

input_files_for_external_table = [
    ("orders.csv", "stg_orders"),
    ("aisles.csv", "dim_aisles"),
    ("products.csv", "dim_products"),
    ("departments.csv", "dim_departments"),
]


input_files_for_dim_table = [
    ("aisles.parquet", "dim_aisle"),
    ("products.parquet", "dim_product"),
    ("departments.parquet", "dim_department"),
]

with DAG(
    "s2_ingest_to_bigquery",
    default_args=default_args,
    description="ingest instacart data into bigquery",
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["instacart"],
) as dag:

    with TaskGroup("load_dim_table") as load_dim_tables_tg:
        for idx, item in enumerate(input_files_for_dim_table):
            file_name = item[0]
            table_name = item[1]
            load_gcs_to_bigquery = GCSToBigQueryOperator(
                task_id=f"{table_name}",
                bucket=BUCKET,
                source_objects=[
                    f"raw/{file_name}",
                ],
                destination_project_dataset_table=f"{BIGQUERY_DATASET}.{table_name}",
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                source_format="PARQUET",
                autodetect=True,
            )

    # upzip_instacart_data = PythonOperator(
    #     task_id="upzip_instacart_data",
    #     python_callable=unzip_instacart_data,
    # )

    # with TaskGroup("format_to_parquet") as format_to_parquet_tg:
    #     for i, file in enumerate(input_files):
    #         file_name = file.replace(".csv", "")
    #         task = PythonOperator(
    #             task_id=f"{file_name}",
    #             python_callable=format_to_parquet,
    #             op_kwargs={"src_file": f"/opt/airflow/data/{file}"},
    #         )

    # with TaskGroup("upload_local_to_gcs") as local_to_gcs_tg:
    #     for i, file in enumerate(input_files):
    #         parquet_file = file.replace(".csv", ".parquet")
    #         task = PythonOperator(
    #             task_id=f'{file.replace(".csv","")}',
    #             python_callable=upload_to_gcs,
    #             op_kwargs={
    #                 "bucket": BUCKET,
    #                 "object_name": f"raw/{parquet_file}",
    #                 "local_file": f"/opt/airflow/data/{parquet_file}",
    #             },
    #         )

    # upzip_instacart_data >> format_to_parquet_tg >> local_to_gcs_tg

    with TaskGroup(
        "create_external_table_stg"
    ) as create_bigquery_external_table_stg_tg:
        bigquery_external_order_task = BigQueryCreateExternalTableOperator(
            task_id=f"stg_order",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"stg_order",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        f"gs://{BUCKET}/raw/orders.parquet",
                    ],
                },
            },
        )

        bigquery_external_order_product_task = BigQueryCreateExternalTableOperator(
            task_id=f"stg_order_product",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"stg_order_product",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        f"gs://{BUCKET}/raw/order_products__prior.parquet",
                        f"gs://{BUCKET}/raw/order_products__train.parquet",
                    ],
                },
            },
        )

    # fact_order_product
    # delete_fact_order_product_table = BigQueryDeleteTableOperator(
    #     task_id="delete_fact_order_product_table",
    #     deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.fact_order_product",
    # )

    create_order_products_partitioned_table = BigQueryCreateEmptyTableOperator(
        task_id="create_order_product_partitioned_table",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id="fact_order_product",
        table_resource={
            "rangePartitioning": {
                "field": "order_id",
                "range": {"start": 0, "end": 40000000, "interval": 100000},
            },
            "clustering": {
                "fields": ["add_to_cart_order"],  # Specify the columns to cluster on
            },
            "id": "order_products-partition-table",
            "schema": {
                "fields": [
                    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
                    {
                        "name": "add_to_cart_order",
                        "type": "INTEGER",
                        "mode": "REQUIRED",
                    },
                    {"name": "reordered", "type": "INTEGER", "mode": "REQUIRED"},
                ]
            },
        },
    )

    ingest_order_product_fact_table = BigQueryInsertJobOperator(
        task_id="ingest_order_product_fact_table",
        configuration={
            "query": {
                "query": f"""
    				INSERT INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_order_product`
    					(order_id, product_id, add_to_cart_order, reordered)
    				SELECT order_id, product_id, add_to_cart_order, reordered
    					FROM {PROJECT_ID}.{BIGQUERY_DATASET}.stg_order_product
    			""",
                "useLegacySql": False,
            }
        },
    )

    # order table

    # delete_fact_order_table = BigQueryDeleteTableOperator(
    #     task_id="delete_fact_order_table",
    #     deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.fact_order",
    # )

    create_order_partitioned_table = BigQueryCreateEmptyTableOperator(
        task_id="create_order_partitioned_table",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id="fact_order",
        table_resource={
            "rangePartitioning": {
                "field": "user_id",
                "range": {"start": 0, "end": 40000000, "interval": 100000},
            },
            "clustering": {
                "fields": ["order_id"],  # Specify the columns to cluster on
            },
            "id": "order_products-partition-table",
            "schema": {
                "fields": [
                    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "user_id", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "eval_set", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "order_number", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "order_dow", "type": "INTEGER", "mode": "REQUIRED"},
                    {
                        "name": "order_hour_of_day",
                        "type": "INTEGER",
                        "mode": "REQUIRED",
                    },
                    {
                        "name": "days_since_prior_order",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                ]
            },
        },
    )

    truncate_fact_order_table_task = BigQueryExecuteQueryOperator(
        task_id="truncate_fact_order_table_task",
        sql=f"TRUNCATE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_order`",
        use_legacy_sql=False,  # Important for standard SQL syntax
    )

    truncate_fact_order_product_table_task = BigQueryExecuteQueryOperator(
        task_id="truncate_fact_order_product_table_task",
        sql=f"TRUNCATE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_order_product`",
        use_legacy_sql=False,  # Important for standard SQL syntax
    )

    ingest_order_fact_table = BigQueryInsertJobOperator(
        task_id="ingest_order_fact_table",
        configuration={
            "query": {
                "query": f"""
    				INSERT INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_order`
    					(order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order)
    				SELECT order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order
    					FROM {PROJECT_ID}.{BIGQUERY_DATASET}.stg_order
    			""",
                "useLegacySql": False,
            }
        },
    )

    (
        load_dim_tables_tg
        >> create_bigquery_external_table_stg_tg
        >> [
            create_order_products_partitioned_table,
            create_order_partitioned_table,
        ]
    )
    (
        create_order_products_partitioned_table
        >> truncate_fact_order_product_table_task
        >> ingest_order_product_fact_table
    )
    (
        create_order_partitioned_table
        >> truncate_fact_order_table_task
        >> ingest_order_fact_table
    )
