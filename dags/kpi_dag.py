from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
import logging



sys.path.append("/opt/airflow")

from scripts.kpi.kpi1_avg_delay import export_kpi1
from scripts.kpi.kpi2_bus_map import export_bus_map
from scripts.fused_gtfs import fuse_gtfs_to_csv
from scripts.kpi.kpi3_bus_stops import export_kpi3_bus_stops
from scripts.kpi.kpi4_routes_delay import export_kpi4_routes_delay
from scripts.kpi.kpi5_heatmap import export_kpi5_heatmap
from scripts.kpi.kpi6_punctuality import export_kpi6_punctuality
from scripts.kpi.kpi7_evolution_stops import compute_delay_evolution_by_stop
from scripts.kpi.kpi8_travel_time_comparison import compute_travel_time_comparison
from scripts.kpi.kpi9_delay_distribution import compute_delay_distribution
from scripts.kpi.kpi10_problematic_stops import compute_top_problematic_stops


# --- Dossiers ---
TRIP_UPDATES_DIR = Path("/opt/airflow/exports/gtfs_rt")
STATIC_GTFS_DIR = Path("/opt/airflow/data/gtfs_static")
FUSED_EXPORTS_DIR = Path("/opt/airflow/exports/gtfs_fused")
FUSED_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# --- DAG args ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Fonctions PythonOperator ---
def fuse_gtfs_task(ti=None):
    try:
        csv_files = list(TRIP_UPDATES_DIR.glob("trip_updates_*.csv"))
        if not csv_files:
            logging.warning("Aucun CSV trip_updates trouvé")
            return None
        latest_csv = max(csv_files, key=lambda f: f.stat().st_mtime)
        logging.info(f"Fusion du CSV le plus récent : {latest_csv}")
        fused_csv = fuse_gtfs_to_csv(STATIC_GTFS_DIR, latest_csv, FUSED_EXPORTS_DIR)
        # Stocker le path dans XCom pour les tâches suivantes
        if ti:
            ti.xcom_push(key="fused_csv", value=str(fused_csv))
        return str(fused_csv)
    except Exception as e:
        logging.error(f"Erreur lors de la fusion GTFS : {e}")
        return None

def export_kpi1_task(ti=None, granularity="T"):
    # Récupérer le CSV fusionné depuis XCom
    fused_csv = ti.xcom_pull(task_ids="fuse_gtfs", key="fused_csv")
    if not fused_csv:
        logging.warning("Aucun fichier fusionné trouvé, KPI1 ignoré")
        return None
    logging.info(f"Calcul KPI1 à partir de : {fused_csv}")
    return export_kpi1(Path(fused_csv), granularity=granularity)


def export_kpi4_task():
    """Wrapper KPI4 pour s'assurer que route_id est correct."""
    path = export_kpi4_routes_delay()
    if path:
        import pandas as pd
        df = pd.read_csv(path)
        # Vérifier si route_id est vide
        for col in ['route_id', 'route_id_x', 'route_id_y']:
            if col in df.columns and df[col].notna().sum() > 0:
                df.rename(columns={col: 'route_id'}, inplace=True)
                df.to_csv(path, index=False)
                logging.info(f"[KPI4] Colonne route_id utilisée : {col}")
                break
    return path

def export_kpi7_task(ti=None):
    # Récupérer le CSV fusionné depuis XCom
    fused_csv = ti.xcom_pull(task_ids="fuse_gtfs", key="fused_csv")
    if not fused_csv:
        logging.warning("Aucun fichier fusionné trouvé, KPI7 ignoré")
        return None

    logging.info(f"Calcul KPI7 à partir de : {fused_csv}")
    df = compute_delay_evolution_by_stop(Path(fused_csv), top_n=5)

    #  Créer le sous-dossier exports/kpi7
    output_dir = Path("/opt/airflow/exports/kpi7")
    output_dir.mkdir(parents=True, exist_ok=True)

    #  Fichier de sortie
    output_file = output_dir / "delay_evolution_by_stop.csv"
    df.to_csv(output_file, index=False)

    logging.info(" KPI7 exporté : %s", output_file)
    return str(output_file)


def export_kpi8_task(ti=None):
    try:
        fused_csv = ti.xcom_pull(task_ids="fuse_gtfs", key="fused_csv")
        if not fused_csv:
            logging.warning("Aucun fichier fusionné trouvé, KPI8 ignoré")
            return None
        logging.info(f"Calcul KPI8 à partir de : {fused_csv}")
        result_df = compute_travel_time_comparison(Path(fused_csv))

        # Sauvegarde
        output_dir = Path("/opt/airflow/exports/kpi8")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "travel_time_comparison.csv"
        result_df.to_csv(output_file, index=False)
        logging.info(f" KPI8 exporté : {output_file}")
        return str(output_file)
    except Exception as e:
        logging.error(f"Erreur KPI8 : {e}")
        return None

def run_kpi9(**context):
    latest_csv = max(FUSED_EXPORTS_DIR.glob("fused_trip_delays_*.csv"), key=lambda f: f.stat().st_mtime)
    output_path = compute_delay_distribution(latest_csv)
    return str(output_path) if output_path else None


def export_kpi10_task(ti=None):
    fused_csv = ti.xcom_pull(task_ids="fuse_gtfs", key="fused_csv")
    if not fused_csv:
        logging.warning("Aucun fichier fusionné trouvé, KPI10 ignoré")
        return None
    
    logging.info(f"Calcul KPI10 à partir de : {fused_csv}")
    return str(compute_top_problematic_stops(Path(fused_csv), top_n=10))


# --- DAG ---
with DAG(
    "kpi_dag",
    default_args=default_args,
    description="KPI Retards moyens par jour",
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["kpi", "delays"],
) as dag:

    # Tâches
    fuse_gtfs = PythonOperator(
        task_id="fuse_gtfs",
        python_callable=fuse_gtfs_task,
    )

    kpi1_task = PythonOperator(
        task_id="export_kpi1_avg_delay",
        python_callable=export_kpi1_task,
        op_kwargs={"granularity": "T"}  # minutely
    )

    bus_map_task = PythonOperator(
        task_id="export_bus_map",
        python_callable=export_bus_map,
    )

    kpi3_task = PythonOperator(
    task_id="export_kpi3_bus_stops",
    python_callable=export_kpi3_bus_stops,
    )

    kpi4_task = PythonOperator(
            task_id="export_kpi4_routes_delay",
            python_callable=export_kpi4_task,
    )

    kpi5_task = PythonOperator(
    task_id="export_kpi5_heatmap",
    python_callable=export_kpi5_heatmap,
    )

    kpi6_task = PythonOperator(
    task_id="export_kpi6_punctuality",
    python_callable=export_kpi6_punctuality,
)
 
    kpi7_task = PythonOperator(
    task_id="export_kpi7_delay_evolution_by_stop",
    python_callable=export_kpi7_task,
)
    
    kpi8_task = PythonOperator(
    task_id="compute_travel_time_comparison",
    python_callable=export_kpi8_task,
)

    kpi9_task = PythonOperator(
        task_id="compute_delay_distribution",
        python_callable=run_kpi9,
    )

    kpi10_task = PythonOperator(
    task_id="compute_top_problematic_stops",
    python_callable=export_kpi10_task,
)

    # Orchestration
    fuse_gtfs >> kpi1_task >> bus_map_task
    fuse_gtfs >> kpi3_task
    fuse_gtfs >> kpi4_task
    fuse_gtfs >> kpi5_task
    fuse_gtfs >> kpi6_task
    fuse_gtfs >> kpi7_task
    fuse_gtfs >> kpi8_task
    fuse_gtfs >> kpi9_task
    fuse_gtfs >> kpi10_task

