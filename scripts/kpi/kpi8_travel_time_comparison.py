import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

EXPORTS_DIR = Path("/opt/airflow/exports/kpi8")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

def compute_travel_time_comparison(fused_csv: Path) -> pd.DataFrame:
    logging.info(f"Lecture du fichier : {fused_csv}")
    
    # Forcer les colonnes horaires en datetime
    df = pd.read_csv(fused_csv, parse_dates=["arrival_time_rt", "scheduled_arrival_dt"])
    
    # Colonnes utiles
    df = df[["trip_id", "stop_id", "stop_name", "stop_sequence", "arrival_time_rt", "scheduled_arrival_dt"]].copy()
    
    # Vérifier que c'est bien datetime
    df["arrival_time_rt"] = pd.to_datetime(df["arrival_time_rt"], errors="coerce")
    df["scheduled_arrival_dt"] = pd.to_datetime(df["scheduled_arrival_dt"], errors="coerce")
    
    # Trier par trip_id et stop_sequence
    df = df.sort_values(by=["trip_id", "stop_sequence"])
    
    # --- Calcul temps de parcours entre deux arrêts ---
    df["actual_segment_time"] = df.groupby("trip_id")["arrival_time_rt"].diff().dt.total_seconds() / 60
    df["scheduled_segment_time"] = df.groupby("trip_id")["scheduled_arrival_dt"].diff().dt.total_seconds() / 60
    
    # Retard segmentaire
    df["delay_segment"] = df["actual_segment_time"] - df["scheduled_segment_time"]
    
    logging.info(f"Données calculées : {len(df)} lignes")
    return df

if __name__ == "__main__":
    # Exemple de lecture dernier fichier fused
    fused_dir = Path("/opt/airflow/exports/gtfs_fused")
    fused_files = sorted(fused_dir.glob("fused_trip_delays_*.csv"))
    if not fused_files:
        logging.error("Aucun fichier fused_trip_delays trouvé")
        exit(1)
    latest_fused_csv = fused_files[-1]
    
    result_df = compute_travel_time_comparison(latest_fused_csv)
    
    output_file = EXPORTS_DIR / "travel_time_comparison.csv"
    result_df.to_csv(output_file, index=False)
    logging.info(f"✅ KPI8 exporté : {output_file}")
