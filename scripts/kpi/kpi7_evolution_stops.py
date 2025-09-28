import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FUSED_CSV_DIR = Path("/opt/airflow/exports/kpi")  # dossier contenant les fused_trip_delays_*.csv
EXPORTS_DIR = Path("/opt/airflow/exports/kpi")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

def get_latest_fused_csv(fused_dir: Path) -> Path:
    files = sorted(fused_dir.glob("fused_trip_delays_*.csv"))
    if not files:
        raise FileNotFoundError("Aucun fichier fused_trip_delays trouvé")
    return files[-1]

def compute_delay_evolution_by_stop(fused_csv: Path, top_n: int = 5) -> pd.DataFrame:
    df = pd.read_csv(fused_csv, parse_dates=["arrival_time_rt"])
    
    # Nettoyage : garder juste les colonnes utiles
    df = df[["arrival_time_rt", "stop_id", "stop_name", "delay_min"]].copy()
    df.rename(columns={"arrival_time_rt": "event_ts"}, inplace=True)
    
    # Supprimer les valeurs aberrantes (retards extrêmes, ex: > 180 min ou < -30 min)
    df = df[df["delay_min"].between(-30, 180)]
    
    # Sélection des top N arrêts par fréquence (basée sur stop_name)
    top_stops = (
        df["stop_name"]
        .value_counts()
        .nlargest(top_n)
        .index
    )
    df = df[df["stop_name"].isin(top_stops)]
    
    logging.info("Top %d arrêts sélectionnés : %s", top_n, list(top_stops))
    logging.info("Données préparées : %d lignes, %d arrêts", len(df), df["stop_name"].nunique())
    return df

if __name__ == "__main__":
    fused_csv = get_latest_fused_csv(FUSED_CSV_DIR)
    logging.info("Dernier fichier fusionné utilisé : %s", fused_csv)
    
    kpi_df = compute_delay_evolution_by_stop(fused_csv, top_n=5)
    
    output_file = EXPORTS_DIR / "kpi7_delay_evolution_by_stop.csv"
    kpi_df.to_csv(output_file, index=False)
    logging.info("✅ KPI 7 exporté : %s", output_file)
