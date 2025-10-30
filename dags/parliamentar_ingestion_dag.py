from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import json
import os
import requests
from datetime import datetime, timedelta


API_URL = "https://api.riigikogu.ee/api/votings"
DB_CONN_ID = "postgres_default" 

def create_schema(**context):
    """Create the parliament_data schema if it doesn't exist."""
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)
    create_schema_sql = "CREATE SCHEMA IF NOT EXISTS parliament_data;"
    pg.run(create_schema_sql)
    print("Schema parliament_data created or already exists.")

def create_tables(**context):
    """Create database tables if they don't exist."""
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS parliament_data.votings (
        uuid VARCHAR(50) PRIMARY KEY,
        voting_number INTEGER,
        type_code VARCHAR(20),
        type_value TEXT,
        description TEXT,
        start_date_time TIMESTAMP,
        end_date_time TIMESTAMP,
        present INTEGER,
        absent INTEGER,
        in_favor INTEGER,
        against INTEGER,
        neutral INTEGER,
        abstained INTEGER,
        sitting_title TEXT,
        sitting_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg.run(create_table_sql)
    print("Tables created or already exist.")

def fetch_votings_data(**context):
    """Fetch votings data from API."""

    start_date = datetime(2024, 1, 1).date()
    end_date = datetime(2024, 1, 31).date()

    params = {
        'startDate': start_date.isoformat(),
        'endDate': end_date.isoformat(),
        'lang': 'et'
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        votings = response.json()
        # Store raw data in XCom
        context['ti'].xcom_push(key='votings_data', value=votings)
        print(f"Fetched {len(votings)} sittings from API for period {start_date} to {end_date}.")
        return votings
    except requests.RequestException as e:
        print(f"Error fetching votings: {e}")
        raise

def store_votings_data(**context):
    """Store votings data in database with upsert logic."""
    sittings = context['ti'].xcom_pull(key='votings_data', task_ids='fetch_votings')
    if not sittings:
        raise ValueError("No sittings data received from previous task.")

    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)
    inserted_count = 0
    updated_count = 0

    for sitting in sittings:
        sitting_title = sitting.get('title')
        sitting_date_str = sitting.get('sittingDateTime')
        if sitting_date_str:
            sitting_date = datetime.fromisoformat(sitting_date_str.replace('Z', '+00:00')).date()
        else:
            sitting_date = None

        votings = sitting.get('votings', [])
        for voting in votings:
            voting_uuid = voting.get('uuid')
            voting_number = voting.get('votingNumber')
            voting_type = voting.get('type', {})
            type_code = voting_type.get('code')
            type_value = voting_type.get('value')
            description = voting.get('description')
            start_dt_str = voting.get('startDateTime')
            end_dt_str = voting.get('endDateTime')
            present = voting.get('present')
            absent = voting.get('absent')
            in_favor = voting.get('inFavor')
            against = voting.get('against')
            neutral = voting.get('neutral')
            abstained = voting.get('abstained')

            if not voting_uuid:
                print(f"Skipping voting without uuid: {voting}")
                continue

            start_dt = None
            end_dt = None
            try:
                if start_dt_str:
                    start_dt = datetime.fromisoformat(start_dt_str.replace('Z', '+00:00'))
            except ValueError:
                print(f"Invalid start_date_time format: {start_dt_str}")
            try:
                if end_dt_str:
                    end_dt = datetime.fromisoformat(end_dt_str.replace('Z', '+00:00'))
            except ValueError:
                print(f"Invalid end_date_time format: {end_dt_str}")

            # Upsert logic
            upsert_sql = """
            INSERT INTO parliament_data.votings (uuid, voting_number, type_code, type_value, description, start_date_time, end_date_time, present, absent, in_favor, against, neutral, abstained, sitting_title, sitting_date, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (uuid) DO UPDATE SET
                voting_number = EXCLUDED.voting_number,
                type_code = EXCLUDED.type_code,
                type_value = EXCLUDED.type_value,
                description = EXCLUDED.description,
                start_date_time = EXCLUDED.start_date_time,
                end_date_time = EXCLUDED.end_date_time,
                present = EXCLUDED.present,
                absent = EXCLUDED.absent,
                in_favor = EXCLUDED.in_favor,
                against = EXCLUDED.against,
                neutral = EXCLUDED.neutral,
                abstained = EXCLUDED.abstained,
                sitting_title = EXCLUDED.sitting_title,
                sitting_date = EXCLUDED.sitting_date,
                updated_at = CURRENT_TIMESTAMP;
            """
            pg.run(upsert_sql, parameters=(voting_uuid, voting_number, type_code, type_value, description, start_dt, end_dt, present, absent, in_favor, against, neutral, abstained, sitting_title, sitting_date))

            # Assume all are inserts since uuid is unique
            inserted_count += 1

    print(f"Inserted {inserted_count} votings into database.")

def data_quality_check(**context):
    """Perform basic data quality checks."""
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)

    # Check for nulls in critical fields
    null_check_sql = """
    SELECT COUNT(*) as null_count
    FROM parliament_data.votings
    WHERE uuid IS NULL OR voting_number IS NULL OR sitting_date IS NULL;
    """
    result = pg.get_first(null_check_sql)
    null_count = result[0] if result else 0

    if null_count > 0:
        raise ValueError(f"Data quality check failed: {null_count} records have null values in critical fields.")

    # Check for duplicates
    dup_check_sql = """
    SELECT uuid, COUNT(*) as count
    FROM parliament_data.votings
    GROUP BY uuid
    HAVING COUNT(*) > 1;
    """
    duplicates = pg.get_records(dup_check_sql)
    if duplicates:
        raise ValueError(f"Data quality check failed: Found duplicate UUIDs: {[row[0] for row in duplicates]}")

    # Check date ranges for sitting_date
    date_check_sql = """
    SELECT COUNT(*) as invalid_date_count
    FROM parliament_data.votings
    WHERE sitting_date < '2012-01-01' OR sitting_date > CURRENT_DATE;
    """
    result = pg.get_first(date_check_sql)
    invalid_date_count = result[0] if result else 0

    if invalid_date_count > 0:
        raise ValueError(f"Data quality check failed: {invalid_date_count} records have invalid dates.")

    total_records = pg.get_first("SELECT COUNT(*) FROM parliament_data.votings;")[0]
    print(f"Data quality checks passed. Total records: {total_records}")


default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="parliamentary_ingestion_dag", 
    default_args=default_args,
    description="Ingest parliamentary voting data from Riigikogu API",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['parliament', 'ingestion'],
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

    # Task dependencies
    create_schema_task >> create_tables_task >> fetch_task >> store_task >> quality_check_task
