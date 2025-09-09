from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow")
from scripts.parse_trip_updates import parse_trip_updates
from scripts.parse_vehicle_positions import parse_vehicle_positions
from scripts.fused_gtfs import fuse_gtfs
from scripts.validate_data import validate_data
from scripts.utils import alert_if_failed

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="gtfs_realtime_dag",
    default_args=default_args,
    description="GTFS-RT Trip Updates + Vehicle Positions + Fusion",
    schedule="*/5 * * * *",  # toutes les 5 minutes
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=["gtfs", "realtime"],
) as dag:

    t1_trip_updates = PythonOperator(
        task_id="parse_trip_updates",
        python_callable=parse_trip_updates,
        on_failure_callback=alert_if_failed
    )

    t2_vehicle_positions = PythonOperator(
        task_id="parse_vehicle_positions",
        python_callable=parse_vehicle_positions,
        on_failure_callback=alert_if_failed
    )

    t3_fuse = PythonOperator(
        task_id="fuse_gtfs",
        python_callable=fuse_gtfs,
        on_failure_callback=alert_if_failed
    )

    t4_validate = PythonOperator(
        task_id="validate_gtfs_realtime",
        python_callable=validate_data,
        on_failure_callback=alert_if_failed
    )

    [t1_trip_updates, t2_vehicle_positions] >> t3_fuse >> t4_validate
