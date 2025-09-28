from pathlib import Path
import pandas as pd
from datetime import datetime

# Chemins
FUSED_CSV_PATH = Path("/opt/airflow/exports/gtfs_fused")  # dossier contenant les CSV fusionnés
EXPORTS_DIR = Path("/opt/airflow/exports/kpi")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

def compute_delays_from_fused_csv(fused_csv: Path) -> pd.DataFrame:
    """
    Calcule les retards moyens par trip_id à partir du CSV fusionné.
    """
    df = pd.read_csv(fused_csv)

    # Vérifie la présence de la colonne delay_min
    if 'delay_min' not in df.columns:
        raise ValueError("Colonne 'delay_min' manquante dans le CSV fusionné")

    # Si la colonne scheduled_arrival_dt existe, convertir en datetime et la renommer
    if 'scheduled_arrival_dt' in df.columns:
        df['scheduled_ts'] = pd.to_datetime(df['scheduled_arrival_dt'], errors='coerce')

    # Moyenne et écart-type par trip_id
    delays = df.groupby('trip_id').agg(
        avg_delay_min=('delay_min', 'mean'),
        std_delay_min=('delay_min', 'std'),
        n_stops=('delay_min', 'count'),
        scheduled_ts=('scheduled_ts', 'first')  # garde une référence temporelle
    ).reset_index()

    return delays


def export_kpi1(fused_csv: Path, granularity: str = None):
    """
    Exporte le retard moyen des voyages depuis le CSV fusionné.

    - Si granularity est None : calcul retard moyen global
    - Sinon : calcul retard moyen temporel par bucket (ex: 'T' pour minute, 'H' pour heure)
    """
    df_trips = compute_delays_from_fused_csv(fused_csv)
    if df_trips.empty:
        print("[KPI1] Aucune donnée pour la journée")
        return None

    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_file = EXPORTS_DIR / f"kpi1_avg_delay_{timestamp_str}.csv"
    parquet_file = EXPORTS_DIR / f"kpi1_avg_delay_{timestamp_str}.parquet"

    # --- Cas 1 : retard moyen global ---
    if granularity is None or 'scheduled_ts' not in df_trips.columns:
        df_avg = pd.DataFrame({
            "avg_delay_all_trips_min": [df_trips["avg_delay_min"].mean()]
        })
    # --- Cas 2 : retard moyen par intervalle temporel ---
    else:
        df_trips["event_bucket"] = df_trips["scheduled_ts"].dt.floor(granularity)
        df_avg = df_trips.groupby("event_bucket")["avg_delay_min"].mean().reset_index()
        df_avg.rename(columns={"avg_delay_min": "avg_delay_all_trips_min"}, inplace=True)

    # Export CSV + Parquet
    df_avg.to_csv(csv_file, index=False)
    df_avg.to_parquet(parquet_file, index=False)

    print(f"[KPI1] Exporté CSV : {csv_file}")
    print(f"[KPI1] Exporté Parquet : {parquet_file}")
    return str(csv_file)

if __name__ == "__main__":
    # Exemples : prend le dernier fichier fusionné
    fused_files = sorted(FUSED_CSV_PATH.glob("fused_trip_delays_*.csv"), reverse=True)
    if not fused_files:
        print("[KPI1] Aucun fichier fusionné trouvé")
    else:
        latest_fused = fused_files[0]
        export_kpi1(latest_fused)
