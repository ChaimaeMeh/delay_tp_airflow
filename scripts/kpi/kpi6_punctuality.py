import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

KPI6_DIR = Path("/opt/airflow/exports/kpi6")
KPI6_DIR.mkdir(parents=True, exist_ok=True)

def export_kpi6_punctuality():
    """Exporte un CSV pour KPI6 : Gauge plot du taux de ponctualité."""
    fused_dir = Path("/opt/airflow/exports/gtfs_fused")
    fused_csv_files = sorted(fused_dir.glob("fused_trip_delays_*.csv"))
    if not fused_csv_files:
        logging.warning("[KPI6] Aucun fichier fused_trip_delays trouvé")
        return None

    latest_csv = fused_csv_files[-1]
    logging.info(f"[KPI6] Lecture du fichier : {latest_csv}")

    df = pd.read_csv(latest_csv, encoding='utf-8-sig')

    # Calcul ponctualité
    total_buses = len(df)
    punctual_buses = len(df[df['delay_min'] <= 5])
    punctuality_rate = (punctual_buses / total_buses) * 100 if total_buses else 0

    logging.info(f"[KPI6] Taux de ponctualité : {punctuality_rate:.2f}%")

    # Export CSV KPI6
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = KPI6_DIR / f"kpi6_punctuality_{ts}.csv"

    df_result = pd.DataFrame({
        "punctuality_rate": [punctuality_rate]
    })

    df_result.to_csv(csv_path, index=False)
    logging.info(f"[KPI6] Export CSV: {csv_path}")

    return str(csv_path)


if __name__ == "__main__":
    export_kpi6_punctuality()
