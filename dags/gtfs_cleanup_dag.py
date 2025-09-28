from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow")
from scripts.utils import alert_if_failed, cleanup_old_files

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="gtfs_cleanup_dag",
    default_args=default_args,
    description="Nettoyage et archivage des anciens fichiers GTFS",
    schedule="59 23 * * *",  # chaque jour à 23h59
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=["gtfs", "cleanup"],
) as dag:

    t1_cleanup = PythonOperator(
        task_id="cleanup_old_files",
        python_callable=cleanup_old_files,
        op_kwargs={"days": 7},  # supprimer les fichiers > 7 jours
        on_failure_callback=alert_if_failed
    )
