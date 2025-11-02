from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import pandas as pd
from datetime import timedelta
from clickhouse_driver import Client

DATA_FILE_PATH = "/opt/airflow/data/Tallinn-Harku-2004-2024.xlsx"

CH_HOST = "clickhouse-server"
CH_PORT = 9000
CH_DB   = "default"
CH_USER = "clickhouse"
CH_PASS = "clickhouse"

def _ch_default() -> Client:
    return Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS)

def _ch() -> Client:
    return Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS, database=CH_DB)

def create_schema(**context):
    _ch_default().execute("CREATE DATABASE IF NOT EXISTS weather_data")
    print("Database weather_data created or already exists.")

def create_tables(**context):
    ddl = """
    CREATE TABLE IF NOT EXISTS weather_data.historic
    (
        id               UInt64 DEFAULT cityHash64(date, coalesce(time, '')),
        date             Date        NOT NULL,
        temperature      Nullable(Decimal(5,2)),
        min_temperature  Nullable(Decimal(5,2)),
        max_temperature  Nullable(Decimal(5,2)),
        humidity         Nullable(Decimal(5,2)),
        wind_speed       Nullable(Decimal(5,2)),
        max_wind_speed   Nullable(Decimal(5,2)),
        precipitation    Nullable(Decimal(5,2)),
        time             Nullable(String),
        created_at       DateTime DEFAULT now()
        -- PG had UNIQUE(date, time). CH doesn't enforce UNIQUE; we ensure idempotency by deleting the window before insert.
    )
    ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, time)
    SETTINGS allow_nullable_key = 1, index_granularity = 8192;
    """
    _ch().execute(ddl)
    print("Tables created or already exist.")

def fetch_weather_data(**context):
    start_date_str = Variable.get("weather_start_date", default_var="2024-01-01")
    end_date_str   = Variable.get("weather_end_date",   default_var="2024-12-31")
    start_date = pd.to_datetime(start_date_str).date()
    end_date   = pd.to_datetime(end_date_str).date()

    df = pd.read_excel(DATA_FILE_PATH, header=1)

    df = df.iloc[1:].reset_index(drop=True)

    df["date"] = pd.to_datetime(
        df[["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"]].astype(str).agg("-".join, axis=1),
        format="%Y-%m-%d",
        errors="coerce",
    )
    df["time"]            = pd.to_datetime(df["Unnamed: 3"], format="%H:%M:%S", errors="coerce")
    df["precipitation"]   = pd.to_numeric(df["Tallinn-Harku.3"],  errors="coerce")
    df["humidity"]        = pd.to_numeric(df["Tallinn-Harku.4"],  errors="coerce")
    df["temperature"]     = pd.to_numeric(df["Tallinn-Harku.5"],  errors="coerce")
    df["min_temperature"] = pd.to_numeric(df["Tallinn-Harku.6"],  errors="coerce")
    df["max_temperature"] = pd.to_numeric(df["Tallinn-Harku.7"],  errors="coerce")
    df["wind_speed"]      = pd.to_numeric(df["Tallinn-Harku.9"],  errors="coerce")
    df["max_wind_speed"]  = pd.to_numeric(df["Tallinn-Harku.10"], errors="coerce")

    # Filter to valid records & date window
    df = df.dropna(subset=["date", "temperature", "humidity", "wind_speed", "precipitation"])
    df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)]
    df = df[df["time"].notna()]

    # Make XCom-friendly
    df["date"] = df["date"].dt.date
    df["time"] = df["time"].dt.strftime("%H:%M:%S")

    weather_data = df[
        ["date","time","temperature","humidity","wind_speed","precipitation",
         "min_temperature","max_temperature","max_wind_speed"]
    ].to_dict("records")

    context["ti"].xcom_push(key="weather_data", value=weather_data)
    print(f"Fetched {len(weather_data)} valid records from Excel for {start_date}..{end_date}.")
    return weather_data

def store_weather_data(**context):
    weather_data = context["ti"].xcom_pull(key="weather_data", task_ids="fetch_weather_data")
    if not weather_data:
        raise ValueError("No weather data received from previous task.")

    start_date_str = Variable.get("weather_start_date", default_var="2024-01-01")
    end_date_str   = Variable.get("weather_end_date",   default_var="2024-12-31")
    client = _ch()

    client.execute(
        """
        ALTER TABLE weather_data.historic
        DELETE WHERE date >= %(start)s AND date <= %(end)s
        """,
        {"start": start_date_str, "end": end_date_str},
    )

    rows = []
    for r in weather_data:
        rows.append((
            r["date"],                   # date
            r["temperature"],            # temperature
            r["min_temperature"],        # min_temperature
            r["max_temperature"],        # max_temperature
            r["humidity"],               # humidity
            r["wind_speed"],             # wind_speed
            r["max_wind_speed"],         # max_wind_speed
            r["precipitation"],          # precipitation
            r["time"],                   # time (String)
            # created_at defaults to now()
        ))

    if rows:
        insert_sql = """
        INSERT INTO weather_data.historic
            (date, temperature, min_temperature, max_temperature, humidity,
            wind_speed, max_wind_speed, precipitation, time)
        VALUES
        """
        client.execute(insert_sql, rows)
        print(f"Stored {len(rows)} records into ClickHouse.")
    else:
        print("No rows to insert.")

def data_quality_check(**context):
    client = _ch()

    null_count = client.execute("""
        SELECT
          sum( if(isNull(date)
               OR isNull(temperature)
               OR isNull(humidity)
               OR isNull(wind_speed)
               OR isNull(precipitation)
               OR isNull(time), 1, 0) ) AS null_count
        FROM weather_data.historic
    """)[0][0]
    if null_count and null_count > 0:
        raise ValueError(f"Data quality check failed: {null_count} records have nulls in critical fields.")

    total = client.execute("SELECT count() FROM weather_data.historic")[0][0]
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
    dag_id="weather_ingestion_dag",
    default_args=default_args,
    description="Ingest weather data from .xlsx file into ClickHouse (bronze)",
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=True,
    max_active_runs=1,
    tags=["weather", "ingestion"],
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
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
        provide_context=True,
    )

    store_task = PythonOperator(
        task_id="store_weather_data",
        python_callable=store_weather_data,
        provide_context=True,
    )

    quality_check_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        provide_context=True,
    )

    create_schema_task >> create_tables_task >> fetch_task >> store_task >> quality_check_task
