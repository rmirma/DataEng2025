from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import requests
from datetime import datetime, timedelta
import pandas as pd

from clickhouse_driver import Client

API_URL = "https://api.riigikogu.ee/api/votings"

CH_HOST = "clickhouse-server"
CH_PORT = 9000
CH_DB   = "default"
CH_USER = "clickhouse"
CH_PASS = "clickhouse"

def _ch() -> Client:
    return Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS)



def create_schema(**context):
    client = _ch()
    client.execute("CREATE DATABASE IF NOT EXISTS parliament_data")
    print("Database parliament_data created or already exists.")

def create_tables(**context):
    ddl = """
    CREATE TABLE IF NOT EXISTS parliament_data.votings
    (
        uuid             String,
        voting_number    Nullable(Int32),
        type_code        Nullable(String),
        type_value       Nullable(String),
        description      Nullable(String),
        start_date_time  Nullable(DateTime),
        end_date_time    Nullable(DateTime),
        present          Nullable(Int32),
        absent           Nullable(Int32),
        in_favor         Nullable(Int32),
        against          Nullable(Int32),
        neutral          Nullable(Int32),
        abstained        Nullable(Int32),
        sitting_title    Nullable(String),
        sitting_date     Nullable(Date),
        created_at       DateTime DEFAULT now(),
        updated_at       DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(sitting_date)
    ORDER BY (sitting_date, uuid)
    SETTINGS allow_nullable_key = 1, index_granularity = 8192;
    """
    client = _ch()
    client.execute(ddl)
    print("Tables created or already exist.")



def fetch_votings_data(**context):
    start_date_str = Variable.get("parliament_start_date", default_var="2024-01-01")
    end_date_str   = Variable.get("parliament_end_date",   default_var="2024-12-31")
    start_date = pd.to_datetime(start_date_str).date()
    end_date   = pd.to_datetime(end_date_str).date()

    params = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "lang": "et",
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        votings = response.json()
        context["ti"].xcom_push(key="votings_data", value=votings)
        print(f"Fetched {len(votings)} sittings from API for period {start_date} to {end_date}.")
        return votings
    except requests.RequestException as e:
        print(f"Error fetching votings: {e}")
        raise



def store_votings_data(**context):
    sittings = context["ti"].xcom_pull(key="votings_data", task_ids="fetch_votings")
    if not sittings:
        raise ValueError("No sittings data received from previous task.")

    start_date_str = Variable.get("parliament_start_date", default_var="2024-01-01")
    end_date_str   = Variable.get("parliament_end_date",   default_var="2024-12-31")
    start_date = pd.to_datetime(start_date_str).date()
    end_date   = pd.to_datetime(end_date_str).date()

    client = Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS, database=CH_DB)

    # Idempotency: delete existing rows for the date window
    client.execute(
        """
        ALTER TABLE parliament_data.votings
        DELETE WHERE sitting_date >= %(start)s AND sitting_date <= %(end)s
        """,
        {"start": start_date, "end": end_date},
    )

    rows = []
    for sitting in sittings:
        sitting_title = sitting.get("title")
        sitting_date_str = sitting.get("sittingDateTime")
        sitting_date = None
        if sitting_date_str:
            try:
                sitting_date = datetime.fromisoformat(sitting_date_str.replace("Z", "+00:00")).date()
            except ValueError:
                pass

        for voting in sitting.get("votings", []):
            voting_uuid = voting.get("uuid")
            if not voting_uuid:
                print(f"Skipping voting without uuid: {voting}")
                continue

            voting_number = voting.get("votingNumber")
            voting_type   = voting.get("type", {}) or {}
            type_code  = voting_type.get("code")
            type_value = voting_type.get("value")
            description = voting.get("description")

            start_dt = None
            end_dt   = None
            start_dt_str = voting.get("startDateTime")
            end_dt_str   = voting.get("endDateTime")
            try:
                if start_dt_str:
                    start_dt = datetime.fromisoformat(start_dt_str.replace("Z", "+00:00"))
            except ValueError:
                print(f"Invalid start_date_time format: {start_dt_str}")
            try:
                if end_dt_str:
                    end_dt = datetime.fromisoformat(end_dt_str.replace("Z", "+00:00"))
            except ValueError:
                print(f"Invalid end_date_time format: {end_dt_str}")

            present   = voting.get("present")
            absent    = voting.get("absent")
            in_favor  = voting.get("inFavor")
            against   = voting.get("against")
            neutral   = voting.get("neutral")
            abstained = voting.get("abstained")

            rows.append((
                str(voting_uuid),
                voting_number,
                type_code,
                type_value,
                description,
                start_dt,
                end_dt,
                present,
                absent,
                in_favor,
                against,
                neutral,
                abstained,
                sitting_title,
                sitting_date,
                # created_at -> default now()
                # updated_at -> default now()
            ))

    if rows:
        insert_sql = """
        INSERT INTO parliament_data.votings
            (uuid, voting_number, type_code, type_value, description,
            start_date_time, end_date_time, present, absent, in_favor,
            against, neutral, abstained, sitting_title, sitting_date)
        VALUES
        """
        client.execute(insert_sql, rows)
        print(f"Inserted {len(rows)} votings into ClickHouse.")
    else:
        print("No rows to insert.")



def data_quality_check(**context):
    client = Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS, database=CH_DB)

    null_count = client.execute("""
        SELECT
            sum(if(isNull(uuid) OR isNull(voting_number) OR isNull(sitting_date), 1, 0)) AS null_count
        FROM parliament_data.votings
    """)[0][0]
    if null_count and null_count > 0:
        raise ValueError(f"Data quality check failed: {null_count} records have null values in critical fields.")

    dups = client.execute("""
        SELECT uuid, count() AS cnt
        FROM parliament_data.votings
        GROUP BY uuid
        HAVING cnt > 1
        LIMIT 10
    """)
    if dups:
        dup_list = [row[0] for row in dups]
        raise ValueError(f"Data quality check failed: Found duplicate UUIDs (sample): {dup_list}")

    invalid_dates = client.execute("""
        SELECT count()
        FROM parliament_data.votings
        WHERE sitting_date < toDate('2012-01-01') OR sitting_date > today()
    """)[0][0]
    if invalid_dates and invalid_dates > 0:
        raise ValueError(f"Data quality check failed: {invalid_dates} records have invalid dates.")

    total = client.execute("SELECT count() FROM parliament_data.votings")[0][0]
    print(f"Data quality checks passed. Total records: {total}")



default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="parliamentary_ingestion_dag",
    default_args=default_args,
    description="Ingest parliamentary voting data from Riigikogu API into ClickHouse (bronze)",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["parliament", "ingestion"],
) as dag:

    create_schema_task = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
        provide_context=True,
    )

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
        provide_context=True,
    )

    fetch_task = PythonOperator(
        task_id="fetch_votings",
        python_callable=fetch_votings_data,
        provide_context=True,
    )

    store_task = PythonOperator(
        task_id="store_votings",
        python_callable=store_votings_data,
        provide_context=True,
    )

    quality_check_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        provide_context=True,
    )

    create_schema_task >> create_tables_task >> fetch_task >> store_task >> quality_check_task
