import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

KPI5_DIR = Path("/opt/airflow/exports/kpi5")
KPI5_DIR.mkdir(parents=True, exist_ok=True)

def export_kpi5_heatmap():
    """Exporte un CSV pour KPI5 : heatmap heures × jours."""
    fused_dir = Path("/opt/airflow/exports/gtfs_fused")
    fused_csv_files = sorted(fused_dir.glob("fused_trip_delays_*.csv"))
    if not fused_csv_files:
        logging.warning("[KPI5] Aucun fichier fused_trip_delays trouvé")
        return None

    latest_csv = fused_csv_files[-1]
    logging.info(f"[KPI5] Lecture du fichier : {latest_csv}")

    df = pd.read_csv(latest_csv, encoding='utf-8-sig')

    if "arrival_time_rt" not in df.columns:
        logging.warning("[KPI5] Colonne arrival_time_rt manquante")
        return None

    # Convertir en datetime
    df["arrival_time_rt"] = pd.to_datetime(df["arrival_time_rt"], errors="coerce")

    # Extraire heure et jour
    df["hour"] = df["arrival_time_rt"].dt.hour
    df["day_name"] = df["arrival_time_rt"].dt.weekday.map({
        0: "Lundi",
        1: "Mardi",
        2: "Mercredi",
        3: "Jeudi",
        4: "Vendredi",
        5: "Samedi",
        6: "Dimanche"
    })

    # Grouper par jour × heure
    df_grouped = df.groupby(["day_name", "hour"], as_index=False).agg(
        mean_delay_min=("delay_min", "mean")
    )

    # Export CSV KPI5
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = KPI5_DIR / f"kpi5_heatmap_{ts}.csv"
    df_grouped.to_csv(csv_path, index=False)
    logging.info(f"[KPI5] Export CSV: {csv_path}")

    return str(csv_path)


if __name__ == "__main__":
    export_kpi5_heatmap()
