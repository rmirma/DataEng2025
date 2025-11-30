from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import DateType, StringType, BooleanType

from clickhouse_driver import Client

import boto3
from botocore.exceptions import ClientError

def ensure_bucket_exists(bucket_name):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1"
    )

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' already exists.")
    except ClientError:
        print(f"S3 bucket '{bucket_name}' not found. Creating...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' created.")


def create_namespace_if_not_exists(catalog, namespace):
    try:
        catalog.create_namespace(namespace)
        print(f"Namespace '{namespace}' created.")
    except Exception as e:
        print(f"Namespace '{namespace}' already exists or failed to create: {e}")


 
def load_csv_to_iceberg():

    catalog = load_catalog(
        "rest",
        uri="http://iceberg_rest:8181",
        warehouse="s3://warehouse",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_endpoint="http://minio:9000",
        s3_region="us-east-1",
        s3_url_style="path",
        s3_use_ssl=False
    )

    namespace = "bronze"
    table_identifier = "bronze.iceberg"

    create_namespace_if_not_exists(catalog, namespace)

    schema = Schema(
        NestedField(1, "Date", DateType(), required=True),
        NestedField(2, "WeekDay", StringType()),
        NestedField(3, "HolidayInd", BooleanType()),
        NestedField(4, "HolidayDesc", StringType())
    )

    try:
        table = catalog.create_table(identifier=table_identifier, schema=schema)
        print(f"Table '{table_identifier}' created.")
    except Exception:
        table = catalog.load_table(table_identifier)
        print(f"Table '{table_identifier}' already exists, loaded instead.")

    df = pd.read_csv("/opt/airflow/data/2024-dates.csv")
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df["HolidayInd"] = df["HolidayInd"].astype(bool)

    arrow_table = pa.Table.from_pandas(
        df,
        schema=pa.schema([
            pa.field("Date", pa.date32(), nullable=False),
            pa.field("WeekDay", pa.string(), nullable=True),
            pa.field("HolidayInd", pa.bool_(), nullable=True),
            pa.field("HolidayDesc", pa.string(), nullable=True),
        ])
    )

    table.append(arrow_table)
    print("Data appended successfully.")
 
    return table_identifier


 
def create_clickhouse_iceberg_table(ti):
    table_identifier = ti.xcom_pull(task_ids="load_csv_to_iceberg")

    client = Client(
        host="clickhouse-server",
        port=9000,
        user="clickhouse",
        password="clickhouse",
        database="default"
    )

 
    s3_path = f"s3://warehouse/{table_identifier.split('.')[-1]}"

    sql = f"""
    CREATE TABLE IF NOT EXISTS clickhouse_iceberg_readonly
    ENGINE = IcebergS3(
        '{s3_path}',
        s3_endpoint='http://minio:9000',
        s3_access_key_id='minioadmin',
        s3_secret_access_key='minioadmin',
        s3_region='us-east-1',
        format='parquet'
    )
    """

    client.execute(sql)
    print(f"ClickHouse table for Iceberg '{table_identifier}' created.")

 
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iceberg_to_clickhouse",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["iceberg", "clickhouse"]
) as dag:
    ensure_bucket_exists = PythonOperator(
        task_id="ensure_bucket_exists",
        python_callable=ensure_bucket_exists,
        op_kwargs={"bucket_name": "warehouse"},
    )
    load_csv_task = PythonOperator(
        task_id="load_csv_to_iceberg",
        python_callable=load_csv_to_iceberg
    )

    create_clickhouse_task = PythonOperator(
        task_id="create_clickhouse_iceberg_table",
        python_callable=create_clickhouse_iceberg_table
    )

    ensure_bucket_exists >> load_csv_task >> create_clickhouse_task
