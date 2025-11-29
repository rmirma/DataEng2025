
def run_dbt_gold(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)

    tables_to_drop = [
        "dim_director",
        "dim_genre",
        "dim_movie",
        "dim_production",
        "dim_date",
        "fact_movie_performance"
    ]

    for table in tables_to_drop:
        try:
            client.command(f"DROP TABLE IF EXISTS _gold.{table} SYNC")
            print(f"Dropped table _gold.{table}")
        except Exception as e:
            print(f"Could not drop {table}: {e}")

    cmd = [
        "dbt", "run",
        "--project-dir", "/opt/airflow/project_root/dbt",
        "--profiles-dir", "/opt/airflow/project_root/dbt",
        "--select", "gold"
    ]
    env = os.environ.copy()
    env["DBT_SCHEMA"] = "_gold"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        raise AirflowFailException(f"dbt run failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout
