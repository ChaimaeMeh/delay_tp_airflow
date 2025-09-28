from pathlib import Path
from datetime import datetime
import pandas as pd
import logging

EXPORT_DIR = Path("/opt/airflow/exports/gtfs_fused")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

def delay_category_placeholder(delay_min):
    """Retourne la catégorie et la couleur en fonction du retard."""
    if pd.isna(delay_min):
        return "unknown", "#AAAAAA"
    elif delay_min <= 0:
        return "on_time", "#00FF00"  # vert
    elif delay_min <= 5:
        return "moderate", "#FFA500"  # orange
    else:
        return "late", "#FF0000"      # rouge

def export_bus_map():
    """Exporte un CSV pour la carte des bus avec retards et couleurs."""
    # Chercher le dernier CSV fused_trip_delays
    fused_csv_files = sorted(EXPORT_DIR.glob("fused_trip_delays_*.csv"))
    if not fused_csv_files:
        logging.warning("[BusMap] Aucun fichier fused_trip_delays trouvé")
        return None
    latest_csv = fused_csv_files[-1]
    logging.info(f"[BusMap] Lecture du fichier : {latest_csv}")

    # Charger le CSV
    df = pd.read_csv(latest_csv, encoding='utf-8-sig')

    # Garder uniquement les colonnes utiles
    df_latest = df[['trip_id', 'stop_id', 'stop_lat', 'stop_lon', 'delay_min']].copy()

    # Supprimer les lignes sans coordonnée
    df_latest = df_latest.dropna(subset=['stop_lat', 'stop_lon'])

    # Ajouter catégorie et couleur
    df_latest[["delay_category", "color_hex"]] = df_latest.apply(
        lambda row: pd.Series(delay_category_placeholder(row["delay_min"])),
        axis=1
    )

    # Export CSV final pour la carte
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = EXPORT_DIR / f"bus_map_{ts}.csv"
    df_latest.to_csv(csv_path, index=False)
    logging.info(f"[BusMap] Export CSV: {csv_path}")

    return str(csv_path)

if __name__ == "__main__":
    export_bus_map()
