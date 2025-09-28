import logging
import os
from pathlib import Path
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime

def parse_vehicle_positions_to_csv(
    url: str,
    data_dir: Path,
    export_dir: Path,
    timeout: int = 10
) -> Path:
    """
    Télécharge le flux GTFS-RT Vehicle Positions, parse les positions des véhicules, génère un CSV.

    Params:
      - url : URL du flux GTFS-RT
      - data_dir : dossier pour stocker le fichier .pb
      - export_dir : dossier pour sauvegarder le CSV
    Returns:
      - chemin Path du CSV généré
    """
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    data_dir.mkdir(parents=True, exist_ok=True)
    export_dir.mkdir(parents=True, exist_ok=True)

    pb_path = data_dir / f"vehicle_positions_{run_ts}.pb"
    csv_path = export_dir / f"vehicle_positions_{run_ts}.csv"

    logging.info("Téléchargement du flux GTFS‑RT depuis %s", url)
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        pb_content = resp.content
        logging.info("Téléchargement OK ; %d octets reçus", len(pb_content))
    except Exception as e:
        logging.error("Erreur lors du téléchargement du flux GTFS‑RT : %s", e, exc_info=True)
        raise

    try:
        pb_path.write_bytes(pb_content)
        logging.info("Flux enregistré dans %s", pb_path)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(pb_content)
    except Exception as e:
        logging.error("Erreur lors du parsing du flux GTFS‑RT : %s", e, exc_info=True)
        raise

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        trip_id = vp.trip.trip_id if vp.trip and vp.trip.trip_id else None
        stop_id = vp.stop_id if vp.stop_id else None
        lat = vp.position.latitude if vp.position else None
        lon = vp.position.longitude if vp.position else None
        timestamp = vp.timestamp if vp.timestamp else None
        rows.append([trip_id, stop_id, lat, lon, timestamp])

    df = pd.DataFrame(rows, columns=["trip_id", "stop_id", "lat", "lon", "timestamp"])
    if df.empty:
        logging.warning("Aucune position de véhicule détectée dans le flux GTFS‑RT (%s)", pb_path)

    try:
        df.to_csv(csv_path, index=False)
        logging.info("Positions de véhicules sauvegardées dans %s (%d lignes)", csv_path, len(df))
    except Exception as e:
        logging.error("Erreur lors de l'écriture du CSV : %s", e, exc_info=True)
        raise

    return csv_path
