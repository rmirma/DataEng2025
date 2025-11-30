from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

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
    description="Transform data from the bronze layer to the gold layer using dbt",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    # Run dbt transformations
    # The dbt project is mounted at /opt/airflow/dbt
    dbt_transform_task = BashOperator(
        task_id="dbt_transform",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt",
    )

    dbt_transform_task
