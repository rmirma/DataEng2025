FROM apache/airflow:2.8.1
RUN pip install --no-cache-dir airflow-clickhouse-plugin pandas requests clickhouse_driver openpyxl