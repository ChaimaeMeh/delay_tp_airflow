from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys
sys.path.append("/opt/airflow")
import pandas as pd

from scripts.fused_gtfs import fuse_gtfs_to_csv
from scripts.utils import alert_if_failed
from scripts.duckdb_utils import (
    load_static_to_duckdb,
    load_rt_to_duckdb,
    export_parquet,
    cleanup_old_files
)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="gtfs_fusion_taskflow",
    default_args=DEFAULT_ARGS,
    schedule="*/15 * * * *",
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=["gtfs", "fusion"],
)
def gtfs_fusion_pipeline():

    @task(on_failure_callback=alert_if_failed)
    def get_latest_trip_updates() -> str:
        rt_dir = Path("/opt/airflow/exports/gtfs_rt")
        rt_dir.mkdir(parents=True, exist_ok=True)
        trip_files = sorted(rt_dir.glob("trip_updates_*.csv"), reverse=True)
        if not trip_files:
            raise FileNotFoundError("Aucun fichier Trip Updates trouvé")
        latest_file = trip_files[0]
        logging.info(f"Dernier Trip Updates : {latest_file}")
        return str(latest_file)

    @task(on_failure_callback=alert_if_failed)
    def get_latest_vehicle_positions() -> str:
        rt_dir = Path("/opt/airflow/exports/gtfs_rt")
        rt_dir.mkdir(parents=True, exist_ok=True)
        vehicle_files = sorted(rt_dir.glob("vehicle_positions_*.csv"), reverse=True)
        if not vehicle_files:
            raise FileNotFoundError("Aucun fichier Vehicle Positions trouvé")
        latest_file = vehicle_files[0]
        logging.info(f"Dernier Vehicle Positions : {latest_file}")
        return str(latest_file)

    @task(on_failure_callback=alert_if_failed)
    def fuse_gtfs_task(trip_updates_csv: str) -> str:
        static_base_dir = Path("/opt/airflow/data/gtfs_static")
        exports_dir = Path("/opt/airflow/exports/gtfs_fused")
        exports_dir.mkdir(parents=True, exist_ok=True)

        fused_csv = fuse_gtfs_to_csv(
            static_base_dir=static_base_dir,
            trip_updates_csv=Path(trip_updates_csv),
            exports_dir=exports_dir
        )
        logging.info(f"Fusion GTFS terminée : {fused_csv}")
        return str(fused_csv)

    @task(on_failure_callback=alert_if_failed)
    def validate_fusion(fused_csv: str) -> str:
        fused_path = Path(fused_csv)
        if not fused_path.exists() or fused_path.stat().st_size == 0:
            raise RuntimeError(f"Fichier fusionné manquant ou vide : {fused_path}")
        try:
            pd.read_csv(fused_path, nrows=5)  # lecture test
        except Exception as e:
            raise RuntimeError(f"Erreur lecture fichier fusionné : {e}")
        logging.info("Fusion GTFS validée avec succès")
        return "OK"

    @task(on_failure_callback=alert_if_failed)
    def prepare_duckdb_task(fused_csv: str, trip_updates_csv: str, vehicle_positions_csv: str):
        fused_path = Path(fused_csv)
        logging.info(f"Début traitement DuckDB : {fused_path}")

        static_dir = Path("/opt/airflow/data/gtfs_static")
        db_path = Path("/opt/airflow/duckdb/gtfs.duckdb")

        # Charger les CSV statiques
        load_static_to_duckdb(static_base_dir=static_dir, db_path=db_path)

        # Charger Trip Updates et Vehicle Positions
        load_rt_to_duckdb(
            trip_updates_csv=Path(trip_updates_csv),
            vehicle_positions_csv=Path(vehicle_positions_csv),
            db_path=db_path
        )

        # Charger le CSV fusionné
        df = pd.read_csv(fused_csv)

        # Export vers Parquet (Power BI)
        exports_dir = Path("/opt/airflow/exports/duckdb_parquet")
        exports_dir.mkdir(parents=True, exist_ok=True)
        export_parquet(df=df, exports_dir=exports_dir)

        # Nettoyer les vieux fichiers
        cleanup_old_files(directory=exports_dir, days=7)

        logging.info("Traitement DuckDB terminé")
        return True

    # Orchestration
    latest_trip_csv = get_latest_trip_updates()
    latest_vehicle_csv = get_latest_vehicle_positions()
    fused_csv = fuse_gtfs_task(latest_trip_csv)
    validate_fusion(fused_csv)
    prepare_duckdb_task(fused_csv, latest_trip_csv, latest_vehicle_csv)

gtfs_fusion_pipeline_dag = gtfs_fusion_pipeline()
