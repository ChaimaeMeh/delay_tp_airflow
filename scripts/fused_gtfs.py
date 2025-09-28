import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_latest_trip_updates_csv(export_dir: Path) -> Path:
    csv_files = sorted(export_dir.glob("trip_updates_*.csv"))
    if not csv_files:
        raise FileNotFoundError("Aucun fichier trip_updates trouvé dans le dossier")
    return csv_files[-1]

def get_latest_static_dir(base_dir: Path) -> Path:
    subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
    if not subdirs:
        raise FileNotFoundError(f"Aucun sous-dossier trouvé dans {base_dir}")
    return max(subdirs, key=lambda d: d.stat().st_mtime)

def _force_str_ids(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:

    """Force certaines colonnes en string propre (sans .0, ni espaces)."""

    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)  # supprime les '1001.0'
                .replace({"nan": pd.NA, "None": pd.NA})  # nettoie les valeurs nulles
            ).astype("string")  # <-- forcer le dtype string
    return df

def fuse_gtfs_to_csv(static_base_dir: Path, trip_updates_csv: Path, exports_dir: Path) -> Path:

    """
    Fusionne GTFS statique + GTFS-RT (trip updates) et génère un CSV horodaté.
    Gestion robuste des colonnes manquantes et des types.
    """

    static_dir =get_latest_static_dir(static_base_dir)
    logging.info("Dernier dossier statique : %s", static_dir)

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    exports_dir.mkdir(parents=True, exist_ok=True)
    output_path = exports_dir / f"fused_trip_delays_{run_ts}.csv"

    try:
        # --- Vérifier fichiers GTFS ---
        required_files = ["stop_times.txt", "trips.txt", "stops.txt", "routes.txt"]
        for file_name in required_files:
            file_path = static_dir / file_name
            if not file_path.exists():
                raise FileNotFoundError(f"Fichier GTFS manquant : {file_path}")

        # --- Charger GTFS statiques ---
        stop_times = pd.read_csv(static_dir / "stop_times.txt", encoding='utf-8-sig')
        trips = pd.read_csv(static_dir / "trips.txt", encoding='utf-8-sig')
        stops = pd.read_csv(static_dir / "stops.txt", encoding='utf-8-sig')
        routes = pd.read_csv(static_dir / "routes.txt", encoding='utf-8-sig')

        # --- Charger RT ---
        trip_updates = pd.read_csv(trip_updates_csv)

        # --- Forcer types string pour IDs ---
        for df in [trip_updates, stop_times, trips, stops, routes]:
            _force_str_ids(df, ['trip_id', 'stop_id', 'route_id'])

        # --- stop_times propre ---
        stop_times['stop_sequence'] = pd.to_numeric(
            stop_times.get('stop_sequence', 0), errors='coerce'
        ).fillna(0)
        stop_times_small = stop_times.drop_duplicates(
            subset=['trip_id', 'stop_id'], keep='first'
        )

        # --- Merge trip_updates + stop_times ---
        merged = pd.merge(
            trip_updates,
            stop_times_small[["trip_id", "stop_id", "arrival_time", "departure_time"]],
            on=['trip_id', 'stop_id'],
            how='left',
            validate='m:1',
            suffixes=('', '_scheduled')
        )

        # Vérif si correspondances manquantes
        missing_stop_times = merged[merged['arrival_time_scheduled'].isna()]
        if not missing_stop_times.empty:
            logging.warning("⚠️ Lignes sans correspondance stop_times : %d", len(missing_stop_times))

        # --- Convertir horaires RT (timestamps en secondes) ---
        merged["arrival_time_rt"] = pd.to_datetime(
            merged["arrival_time"], unit="s", utc=True, errors="coerce"
        )
        merged["departure_time_rt"] = pd.to_datetime(
            merged["departure_time"], unit="s", utc=True, errors="coerce"
        )

        ## --- Convertir horaires planifiés (HH:MM:SS → timedelta) ---
        merged["arrival_time_scheduled"] = pd.to_timedelta(
            merged["arrival_time_scheduled"], errors="coerce"
        )
        merged["departure_time_scheduled"] = pd.to_timedelta(
            merged["departure_time_scheduled"], errors="coerce"
        )

        # Construire datetime planifié basé sur la date de l'événement RT
        paris_tz = pytz.timezone("Europe/Paris")
        # on prend la date du RT mais en Europe/Paris pour aligner le planning
        merged["scheduled_arrival_dt"] = (
            merged["arrival_time_rt"].dt.tz_convert(paris_tz).dt.normalize()  # date locale
            + merged["arrival_time_scheduled"]
        )
        merged["scheduled_arrival_dt"] = merged["scheduled_arrival_dt"].dt.tz_localize(None).dt.tz_localize(paris_tz).dt.tz_convert("UTC")

        # RT est déjà UTC
        merged["arrival_time_rt"] = merged["arrival_time_rt"].dt.tz_convert("UTC")



        # Calcul du retard en minutes
        merged['delay_min'] = (
            (merged['arrival_time_rt'] - merged['scheduled_arrival_dt'])
            .dt.total_seconds() / 60
)


        # --- Enrichir avec trips ---
        if 'route_id' not in trips.columns:
            trips['route_id'] = pd.NA
        trips = trips.drop_duplicates(subset=['trip_id'], keep='first')

        merged = pd.merge(
            merged,
            trips[['trip_id', 'route_id']].astype({"route_id": "string"}),  # <-- cast explicite
            on='trip_id',
            how='left'
        )

        # --- Enrichir avec stops ---
        stops_cols = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
        for col in stops_cols:
            if col not in stops.columns:
                stops[col] = pd.NA
        merged = pd.merge(
            merged,
            stops[stops_cols],
            on='stop_id',
            how='left'
        )

        # --- Enrichir avec routes ---
        if 'route_id' not in merged.columns:
            merged['route_id'] = pd.NA

        routes_cols = ['route_id', 'route_short_name', 'route_long_name']
        for col in routes_cols:
            if col not in routes.columns:
                routes[col] = pd.NA
        merged = pd.merge(
            merged,
            routes[routes_cols].astype({"route_id": "string"}),  # <-- cast explicite
            on='route_id',
            how='left'
        )

        # --- Trier et exporter ---
        merged = merged.sort_values(by=["trip_id", "arrival_time_rt"])
        merged.to_csv(output_path, index=False)
        logging.info("✅ Fusion réussie, fichier généré : %s", output_path)

        return output_path

    except Exception as e:
        logging.error("Erreur fusion GTFS : %s", e, exc_info=True)
        raise RuntimeError(f"Fusion impossible : {e}")
