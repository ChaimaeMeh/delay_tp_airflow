import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

KPI4_DIR = Path("/opt/airflow/exports/kpi4")
KPI4_DIR.mkdir(parents=True, exist_ok=True)

def export_kpi4_routes_delay():
    """Exporte un CSV pour KPI4 : bar chart horizontal des lignes les plus en retard."""
    fused_dir = Path("/opt/airflow/exports/gtfs_fused")
    fused_csv_files = sorted(fused_dir.glob("fused_trip_delays_*.csv"))
    if not fused_csv_files:
        logging.warning("[KPI4] Aucun fichier fused_trip_delays trouvé")
        return None

    latest_csv = fused_csv_files[-1]
    logging.info(f"[KPI4] Lecture du fichier : {latest_csv}")

    df = pd.read_csv(latest_csv, encoding='utf-8-sig')

    # Choisir la bonne colonne route_id
    for col in ['route_id', 'route_id_y', 'route_id_x']:
        if col in df.columns and df[col].notna().sum() > 0:
            df['route_id_used'] = df[col]
            break
    else:
        logging.warning("[KPI4] Aucune route_id disponible dans le CSV")
        return None

    # Créer colonne retard flag
    df['is_delayed'] = df['delay_min'] > 0

    # Agréger par ligne (route_id)
    df_grouped = df.groupby('route_id_used', as_index=False).agg(
        mean_delay_min=('delay_min', 'mean'),
        percent_delayed=('is_delayed', 'mean')
    )

    # Convertir percent_delayed en %
    df_grouped['percent_delayed'] = df_grouped['percent_delayed'] * 100

    #catégorie de retard
    def categorize_delay(p):
        if p < 20:
            return "Faible"
        elif p < 50:
            return "Moyen"
        else:
            return "Élevé"

    df_grouped['delay_category'] = df_grouped['percent_delayed'].apply(categorize_delay)

    # Export CSV KPI4
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = KPI4_DIR / f"kpi4_routes_delay_{ts}.csv"
    df_grouped.to_csv(csv_path, index=False)
    logging.info(f"[KPI4] Export CSV: {csv_path}")

    return str(csv_path)


if __name__ == "__main__":
    export_kpi4_routes_delay()
