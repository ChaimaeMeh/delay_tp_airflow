from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys

sys.path.append(str(Path("/opt/airflow").resolve()))
from scripts.parse_trip_updates import parse_trip_updates_to_csv
from scripts.parse_vehicule_positions import parse_vehicle_positions_to_csv
from scripts.validate_data import validate_data
from scripts.utils import alert_if_failed

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="gtfs_realtime_parsing_taskflow",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=["gtfs", "realtime"],
)
def gtfs_realtime_parsing():

    @task(on_failure_callback=alert_if_failed)
    def parse_trip_updates_task() -> str:
        data_dir = Path("/opt/airflow/data/gtfs_rt")
        export_dir = Path("/opt/airflow/exports/gtfs_rt")
        csv_path = parse_trip_updates_to_csv(
            url="https://ara-api.enroute.mobi/rla/gtfs/trip-updates",
            data_dir=data_dir,
            export_dir=export_dir
        )
        return str(csv_path) 

    @task(on_failure_callback=alert_if_failed)
    def parse_vehicle_positions_task() -> str:
        data_dir = Path("/opt/airflow/data/gtfs_rt")
        export_dir = Path("/opt/airflow/exports/gtfs_rt")
        csv_path = parse_vehicle_positions_to_csv(
            url="https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions",
            data_dir=data_dir,
            export_dir=export_dir
        )
        return str(csv_path)  

    @task(on_failure_callback=alert_if_failed)
    def validate_realtime(trip_csv: str, vehicle_csv: str) -> str:
        """
        Validation adaptée aux fichiers timestampés.
        """
        # Utiliser le répertoire contenant les CSV temps réel
        rt_dir = Path(trip_csv).parent
        
        # Chercher le dernier dossier statique
        static_root = Path("/opt/airflow/data/gtfs_static")
        latest_static = max(static_root.glob("*"), key=lambda p: p.stat().st_mtime)
        
        return validate_data(
            static_dir=latest_static,
            rt_dir=rt_dir
        )

    @task
    def log_files(trip_csv: str, vehicle_csv: str):
        logging.info("Trip Updates CSV : %s", trip_csv)
        logging.info("Vehicle Positions CSV : %s", vehicle_csv)
        return True

    # Orchestration
    trip_csv = parse_trip_updates_task()
    vehicle_csv = parse_vehicle_positions_task()
    validate_realtime(trip_csv, vehicle_csv)
    log_files(trip_csv, vehicle_csv)

gtfs_realtime_parsing_dag = gtfs_realtime_parsing()
