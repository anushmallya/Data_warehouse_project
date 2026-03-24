from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_setup():
    subprocess.run(
        ["python", "/opt/airflow/project/ingestion/pipeline.py"],
        check=True
    )

with DAG(
    dag_id="setup_database_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    setup_task = PythonOperator(
        task_id="setup_database_task",
        python_callable=run_setup
    )