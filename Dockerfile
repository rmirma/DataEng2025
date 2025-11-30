FROM apache/airflow:2.10.2-python3.11

RUN pip install --no-cache-dir \
    airflow-clickhouse-plugin \
    pandas \
    requests \
    clickhouse_driver \
    openpyxl \
    dbt-clickhouse==1.8.4 \
    pyarrow \
    pyiceberg