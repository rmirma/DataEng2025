from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import subprocess
from datetime import timedelta


DATA_FILE_PATH = "/opt/airflow/data/Tallinn-Harku-2004-2024.xlsx"

CH_HOST = "clickhouse-server"
CH_PORT = 9000
CH_DB   = "default"
CH_USER = "clickhouse"
CH_PASS = "clickhouse"



def dbt_transform(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)

    cmd = [
        "dbt", "run",
        "--project-dir", "/opt/airflow/project_root/dbt_project",
        "--profiles-dir", "/opt/airflow/project_root/dbt_project",
        "--select", "gold"
    ]
    env = os.environ.copy()
    env["DBT_SCHEMA"] = "_gold"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        raise AirflowFailException(f"dbt run failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transformation_dag",
    default_args=default_args,
    description="Transform data from the bronze layer to the gold layer",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    dbt_transform_task = PythonOperator(
        task_id="dbt_transform",
        python_callable=dbt_transform,
        provide_context=True,
    )

    dbt_transform_task
