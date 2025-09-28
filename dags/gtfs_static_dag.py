from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import sys
sys.path.append(str(Path("/opt/airflow").resolve()))
from scripts.download_gtfs import download_gtfs_static
from scripts.validate_data import validate_data
from scripts.utils import alert_if_failed, cleanup_old_files


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="gtfs_static_dag_taskflow",
    default_args=DEFAULT_ARGS,
    description="Téléchargement et validation du GTFS statique (TaskFlow)",
    schedule="0 4 * * *",
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=["gtfs", "static"],
)
def gtfs_static_dag():

    @task(on_failure_callback=alert_if_failed)
    def download_task() -> str:
        data_dir = Path("/opt/airflow/data")
        url = "https://chouette.enroute.mobi/api/v1/datas/OpendataRLA/gtfs.zip"
        extracted_path = download_gtfs_static(url=url, data_dir=data_dir)
        return str(extracted_path)

    @task(on_failure_callback=alert_if_failed)
    def validate_task(extracted_path: str) -> bool:
        return validate_data(static_dir=extracted_path, rt_dir=None)

    @task(on_failure_callback=alert_if_failed)
    def cleanup_task() -> str:
        return cleanup_old_files(days=7)

    path = download_task()
    valid = validate_task(path)
    cleanup_task() 

gtfs_static_dag_instance = gtfs_static_dag()
