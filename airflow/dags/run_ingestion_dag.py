from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys

PROJECT_PATH = "/opt/airflow/project"
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

from ingestion.run_ingestion import load_bronze, load_silver, validate_gold

with DAG(
    dag_id="run_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-warehouse", "bronze-silver-gold"]
) as dag:

    load_bronze_task = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver
    )

    validate_gold_task = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold
    )

    load_bronze_task >> load_silver_task >> validate_gold_task