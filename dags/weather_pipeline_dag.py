from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'agathe',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='Pipeline ETL météo — 10 villes',
    schedule_interval='@hourly',
    start_date=datetime(2026, 4, 23),
    catchup=False,
    tags=['etl', 'météo', 'pipeline'],
) as dag:

    run_etl = BashOperator(
        task_id='run_etl',
        bash_command='cd /opt/airflow/project && python src/main.py'
    )