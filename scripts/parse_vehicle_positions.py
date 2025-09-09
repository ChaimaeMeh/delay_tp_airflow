import os
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
import logging

URL_VP = "https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions"

DATA_DIR = "data"
EXPORTS_DIR = "exports"
PB_PATH = os.path.join(DATA_DIR, "gtfs_rt_vehicle_positions.pb")
CSV_PATH = os.path.join(EXPORTS_DIR, "vehicle_positions.csv")


def parse_vehicle_positions():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    try:
        logging.info("Téléchargement du flux GTFS-RT Vehicle Positions...")
        response = requests.get(URL_VP, timeout=10)
        response.raise_for_status()

        with open(PB_PATH, "wb") as f:
            f.write(response.content)

        feed = gtfs_realtime_pb2.FeedMessage()
        with open(PB_PATH, "rb") as f:
            feed.ParseFromString(response.content)


        rows = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                vp = entity.vehicle
                trip_id = vp.trip.trip_id
                stop_id = vp.stop_id if vp.stop_id else None
                lat = vp.position.latitude if vp.position else None
                lon = vp.position.longitude if vp.position else None
                timestamp = vp.timestamp if vp.timestamp else None

                rows.append([trip_id, stop_id, lat, lon, timestamp])

        df = pd.DataFrame(rows, columns=["trip_id", "stop_id", "lat", "lon", "timestamp"])
        df.to_csv(CSV_PATH, index=False)

        logging.info(f"Vehicle positions sauvegardés dans {CSV_PATH}")
        return f"{len(df)} vehicle positions parsés"

    except Exception as e:
        logging.error(f"Erreur parsing Vehicle Positions : {e}")
        raise RuntimeError(f"Impossible de parser le GTFS-RT vehicle_positions : {e}")
