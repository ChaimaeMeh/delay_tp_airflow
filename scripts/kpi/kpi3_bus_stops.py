import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

KPI3_DIR = Path("/opt/airflow/exports/kpi3")
KPI3_DIR.mkdir(parents=True, exist_ok=True)

def delay_category_color(delay_min):
    """Retourne la catégorie et la couleur en fonction du retard moyen."""
    if pd.isna(delay_min):
        return "unknown", "#AAAAAA"
    elif delay_min <= 0:
        return "on_time", "#00FF00"  # vert
    elif delay_min <= 5:
        return "moderate", "#FFA500"  # orange
    else:
        return "late", "#FF0000"      # rouge

def export_kpi3_bus_stops():
    """Exporte un CSV pour la carte des arrêts avec état de service."""
    # Chercher le dernier CSV fused_trip_delays
    fused_dir = Path("/opt/airflow/exports/gtfs_fused")
    fused_csv_files = sorted(fused_dir.glob("fused_trip_delays_*.csv"))
    if not fused_csv_files:
        logging.warning("[KPI3] Aucun fichier fused_trip_delays trouvé")
        return None
    latest_csv = fused_csv_files[-1]
    logging.info(f"[KPI3] Lecture du fichier : {latest_csv}")

    # Charger le CSV
    df = pd.read_csv(latest_csv, encoding='utf-8-sig')

    # Agréger retard moyen par arrêt
    df_grouped = df.groupby(['stop_id', 'stop_name', 'stop_lat', 'stop_lon'], as_index=False).agg(
        mean_delay_min=('delay_min', 'mean'),
        bus_count=('trip_id', 'count')
    )

    # Ajouter catégorie et couleur
    df_grouped[["delay_category", "color_hex"]] = df_grouped.apply(
        lambda row: pd.Series(delay_category_color(row["mean_delay_min"])),
        axis=1
    )

    # Export CSV final KPI3
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = KPI3_DIR / f"kpi3_bus_stops_{ts}.csv"
    df_grouped.to_csv(csv_path, index=False)
    logging.info(f"[KPI3] Export CSV: {csv_path}")

    return str(csv_path)

if __name__ == "__main__":
    export_kpi3_bus_stops()
