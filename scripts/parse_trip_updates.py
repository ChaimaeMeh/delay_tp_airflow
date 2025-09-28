import logging
import os
from pathlib import Path
import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime


def parse_trip_updates_to_csv(
    url: str,
    data_dir: Path,
    export_dir: Path,
    timeout: int = 30
) -> Path:
    """
    Télécharge le flux GTFS-RT Trip Updates, parse toutes les infos disponibles,
    génère un CSV complet.

    Params:
      - url : URL du feed GTFS-RT
      - data_dir : dossier pour stocker le fichier .pb
      - export_dir : dossier pour sauvegarder le CSV parse
    Returns:
      - chemin Path du CSV généré
    """
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    data_dir.mkdir(parents=True, exist_ok=True)
    export_dir.mkdir(parents=True, exist_ok=True)

    pb_path = data_dir / f"trip_updates_{run_ts}.pb"
    csv_path = export_dir / f"trip_updates_{run_ts}.csv"

    logging.info("Téléchargement du flux GTFS-RT depuis %s", url)
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        pb_content = resp.content
        logging.info("Téléchargement OK ; %d octets reçus", len(pb_content))
    except Exception as e:
        logging.error("Erreur lors du téléchargement du feed GTFS-RT : %s", e, exc_info=True)
        raise

    try:
        pb_path.write_bytes(pb_content)
        logging.info("Flux enregistré dans %s", pb_path)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(pb_content)
    except Exception as e:
        logging.error("Erreur during parsing protobuf : %s", e, exc_info=True)
        raise

    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip_id = tu.trip.trip_id if tu.trip and tu.trip.trip_id else None
        route_id = tu.trip.route_id if tu.trip and tu.trip.route_id else None
        direction_id = tu.trip.direction_id if tu.trip and tu.trip.HasField("direction_id") else None
        start_time = tu.trip.start_time if tu.trip and tu.trip.start_time else None
        start_date = tu.trip.start_date if tu.trip and tu.trip.start_date else None
        schedule_rel_trip = tu.trip.schedule_relationship if tu.trip else None

        for stu in tu.stop_time_update:
            stop_id = stu.stop_id if stu.stop_id else None
            stop_seq = stu.stop_sequence if stu.HasField("stop_sequence") else None
            schedule_rel_stop = stu.schedule_relationship

            # Arrival
            arr_time = getattr(stu.arrival, "time", None) if stu.HasField("arrival") else None
            arr_delay = stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None
            arr_uncertainty = stu.arrival.uncertainty if stu.HasField("arrival") and stu.arrival.HasField("uncertainty") else None

            # Departure
            dep_time = getattr(stu.departure, "time", None) if stu.HasField("departure") else None
            dep_delay = stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None
            dep_uncertainty = stu.departure.uncertainty if stu.HasField("departure") and stu.departure.HasField("uncertainty") else None

            rows.append([
                trip_id, route_id, direction_id, start_time, start_date, schedule_rel_trip,
                stop_id, stop_seq, schedule_rel_stop,
                arr_time, arr_delay, arr_uncertainty,
                dep_time, dep_delay, dep_uncertainty
            ])

    df = pd.DataFrame(rows, columns=[
        "trip_id", "route_id", "direction_id", "start_time", "start_date", "trip_schedule_relationship",
        "stop_id", "stop_sequence", "stop_schedule_relationship",
        "arrival_time", "arrival_delay", "arrival_uncertainty",
        "departure_time", "departure_delay", "departure_uncertainty"
    ])

    df['arrival_time'] = pd.to_numeric(df['arrival_time'], errors='coerce')
    df['departure_time'] = pd.to_numeric(df['departure_time'], errors='coerce')


    if df.empty:
        logging.warning(" Aucun TripUpdate trouvé dans le flux (%s)", pb_path)

    try:
        df.to_csv(csv_path, index=False)
        logging.info("Trip updates sauvegardés dans %s (%d lignes)", csv_path, len(df))
    except Exception as e:
        logging.error("Erreur lors de l'écriture du CSV : %s", e, exc_info=True)
        raise

    return csv_path
