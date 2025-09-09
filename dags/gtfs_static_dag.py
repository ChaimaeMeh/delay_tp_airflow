from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow")
from scripts.download_gtfs import download_gtfs
from scripts.validate_data import validate_data
from scripts.utils import alert_if_failed

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gtfs_static_dag",
    default_args=default_args,
    description="Téléchargement et validation du GTFS statique",
    schedule="0 4 * * *",  # tous les jours à 4h
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=["gtfs", "static"],
) as dag:

    t1_download = PythonOperator(
        task_id="download_gtfs_static",
        python_callable=download_gtfs,
        on_failure_callback=alert_if_failed
    )

    t2_validate = PythonOperator(
        task_id="validate_gtfs_static",
        python_callable=validate_data, 
        on_failure_callback=alert_if_failed
    )

  
    t1_download >> t2_validate
