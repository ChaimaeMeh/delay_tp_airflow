import duckdb
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, timedelta, date
from typing import Optional

# Chemins par défaut
DB_PATH = Path("/opt/airflow/duckdb/gtfs.duckdb")
EXPORTS_DIR = Path("/opt/airflow/exports/duckdb")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO)

def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Retourne une connexion DuckDB persistante"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def load_static_to_duckdb(static_base_dir: Path, db_path: Path = DB_PATH):

    """Charge les fichiers GTFS statiques dans DuckDB (stops, trips, stop_times, routes)."""

    # --- Récupérer le dernier sous-dossier statique ---
    subdirs = [d for d in Path(static_base_dir).iterdir() if d.is_dir()]
    if not subdirs:
        raise FileNotFoundError(f"Aucun sous-dossier trouvé dans {static_base_dir}")
    static_dir = max(subdirs, key=lambda d: d.stat().st_mtime)
    con = get_connection(db_path)

    logging.info("Chargement des GTFS statiques depuis %s", static_dir)

    # stops, trips, routes
    for table_name, file_name in [
        ("stops", "stops.txt"),
        ("trips", "trips.txt"),
        ("routes", "routes.txt")
    ]:
        file_path = static_dir / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Fichier manquant : {file_path}")
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{file_path}', HEADER=TRUE);
        """)

    # stop_times
    stop_times_file = static_dir / "stop_times.txt"
    if not stop_times_file.exists():
        raise FileNotFoundError(f"Fichier manquant : {stop_times_file}")

    df_sample = pd.read_csv(stop_times_file, nrows=5)
    types = {}
    for col in ["arrival_time", "departure_time", "stop_id", "trip_id", "stop_sequence"]:
        if col in df_sample.columns:
            types[col] = "VARCHAR"

    if types:
        logging.info("Colonnes forcées en VARCHAR : %s", types)
        types_str = "{" + ", ".join(f"'{k}':'{v}'" for k, v in types.items()) + "}"
        con.execute(f"""
            CREATE OR REPLACE TABLE stop_times AS
            SELECT * FROM read_csv_auto('{stop_times_file}', HEADER=TRUE, types={types_str});
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE stop_times AS
            SELECT * FROM read_csv_auto('{stop_times_file}', HEADER=TRUE);
        """)

    logging.info("GTFS statiques chargés dans DuckDB avec succès")


def load_rt_to_duckdb(trip_updates_csv: Path, vehicle_positions_csv: Path, db_path: Path = DB_PATH):

    """Charge les fichiers GTFS-RT parsés (trip_updates + vehicle_positions)."""

    con = get_connection(db_path)

    for table_name, file_path in [
        ("trip_updates", trip_updates_csv),
        ("vehicle_positions", vehicle_positions_csv)
    ]:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Fichier manquant : {file_path}")
        logging.info("Chargement du %s CSV : %s", table_name, file_path)

        types_override = {'stop_id': 'VARCHAR'} if table_name == "trip_updates" else {}
        if types_override:
            types_str = "{" + ", ".join(f"'{k}':'{v}'" for k, v in types_override.items()) + "}"
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{file_path}', HEADER=TRUE, types={types_str});
            """)
        else:
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{file_path}', HEADER=TRUE);
            """)

    logging.info("GTFS temps réel chargés dans DuckDB avec succès")


def export_parquet(df: pd.DataFrame, exports_dir: Path = EXPORTS_DIR, prefix: str = "gtfs_metrics") -> Path:
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    exports_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = exports_dir / f"{prefix}_{run_ts}.parquet"
    df.to_parquet(parquet_path, index=False)
    logging.info("Export Parquet effectué : %s", parquet_path)
    return parquet_path



def cleanup_old_files(directory: Path, days: int = 7) -> list[str]:

    """Supprime les fichiers plus vieux que X jours."""
    
    cutoff = datetime.utcnow() - timedelta(days=days)
    removed = []

    for file in Path(directory).glob("*"):
        if file.is_file() and datetime.utcfromtimestamp(file.stat().st_mtime) < cutoff:
            file.unlink()
            removed.append(file.name)

    if removed:
        logging.info("Fichiers supprimés (%s jours) : %s", days, removed)
    else:
        logging.info("Aucun fichier ancien à supprimer")

    return removed
