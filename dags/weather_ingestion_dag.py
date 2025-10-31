from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import pandas as pd
from datetime import timedelta


DATA_FILE_PATH = "/opt/airflow/data/Tallinn-Harku-2004-2024.xlsx"
DB_CONN_ID = "postgres_default" 

def create_schema(**context):
    """Create the weather_data schema if it doesn't exist."""
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)
    create_schema_sql = "CREATE SCHEMA IF NOT EXISTS weather_data;"
    pg.run(create_schema_sql)
    print("Schema weather_data created or already exists.")

def create_tables(**context):
    """Create database tables if they don't exist."""
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_data.historic (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        temperature NUMERIC(5, 2),
        humidity NUMERIC(5, 2),
        wind_speed NUMERIC(5, 2),
        precipitation NUMERIC(5, 2),
        time TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (date, time)
    );
    """
    pg.run(create_table_sql)
    print("Tables created or already exist.")

def fetch_weather_data(**context):
    """Fetch weather data from Excel file."""
    # Get date range from Airflow Variables
    start_date_str = Variable.get("weather_start_date", default_var="2024-01-01")
    end_date_str = Variable.get("weather_end_date", default_var="2024-12-31")
    
    start_date = pd.to_datetime(start_date_str).date()
    end_date = pd.to_datetime(end_date_str).date()


    df = pd.read_excel(DATA_FILE_PATH, header=1)
    
    # Skip the header row that contains column descriptions
    df = df.iloc[1:].reset_index(drop=True)
    
    # Convert columns to appropriate types
    df['date'] = pd.to_datetime(df[['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2']].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d', errors='coerce')
    df['time'] = pd.to_datetime(df['Unnamed: 3'], format='%H:%M:%S', errors='coerce')
    df['precipitation'] = pd.to_numeric(df['Tallinn-Harku.3'], errors='coerce')
    df['humidity'] = pd.to_numeric(df['Tallinn-Harku.4'], errors='coerce')
    df['temperature'] = pd.to_numeric(df['Tallinn-Harku.5'], errors='coerce')
    df['wind_speed'] = pd.to_numeric(df['Tallinn-Harku.9'], errors='coerce')


    # Filter to valid records
    df = df.dropna(subset=['date', 'temperature', 'humidity', 'wind_speed', 'precipitation'])

    # Filter by date range
    df = df[(df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)]
    
    # Filter out invalid time values (NaT)
    df = df[df['time'].notna()]
    
    # Convert to serializable format for XCom
    df['date'] = df['date'].dt.date
    df['time'] = df['time'].dt.strftime('%H:%M:%S')

    weather_data = df[['date', 'time', 'temperature', 'humidity', 'wind_speed', 'precipitation']].to_dict('records')
    context['ti'].xcom_push(key='weather_data', value=weather_data)
    print(f"Fetched {len(weather_data)} valid records from Excel file for date range {start_date} to {end_date}.")
    return weather_data

def store_weather_data(**context):
    """Store weather data in database."""
    weather_data = context['ti'].xcom_pull(key='weather_data', task_ids='fetch_weather_data')
    if not weather_data:
        raise ValueError("No weather data received from previous task.")

    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)
    stored_count = 0
    
    for record in weather_data:
        upsert_sql = """
        INSERT INTO weather_data.historic (date, time, temperature, humidity, wind_speed, precipitation)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, time) DO NOTHING;  -- Skip duplicates
        """
        try:
            pg.run(upsert_sql, parameters=(
                record['date'],
                record['time'],
                record['temperature'],
                record['humidity'],
                record['wind_speed'],
                record['precipitation']
            ))
            stored_count += 1
        except Exception as e:
            print(f"Error storing record {record}: {e}")
            continue
    
    print(f"Stored/updated {stored_count} records into database.")


def data_quality_check(**context):
    """Perform basic data quality checks."""
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID)

    # Check for nulls in critical fields
    null_check_sql = """
    SELECT COUNT(*) as null_count
    FROM weather_data.historic
    WHERE date IS NULL OR temperature IS NULL OR humidity IS NULL OR wind_speed IS NULL OR time IS NULL OR precipitation IS NULL;
    """
    result = pg.get_first(null_check_sql)
    null_count = result[0] if result else 0

    if null_count > 0:
        raise ValueError(f"Data quality check failed: {null_count} records have null values in critical fields.")

    total_records = pg.get_first("SELECT COUNT(*) FROM weather_data.historic;")[0]
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
    dag_id="weather_ingestion_dag",
    default_args=default_args,
    description="Ingest weather data from .xlsx file",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'ingestion'],
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

    # Task dependencies
    create_schema_task >> create_tables_task >> fetch_task >> store_task >> quality_check_task
