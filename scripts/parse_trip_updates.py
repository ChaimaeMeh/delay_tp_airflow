
import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
import os

# URL du flux temps réel Trip Updates
URL_RT = "https://ara-api.enroute.mobi/rla/gtfs/trip-updates"
DATA_DIR = "data"
EXPORTS_DIR = "exports"

def parse_trip_updates():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)

    PB_PATH = os.path.join(DATA_DIR, "gtfs_rt_trip_update.pb")
    CSV_PATH = os.path.join(EXPORTS_DIR, "trip_updates1.csv")

    # Télécharger le flux
    try:
        print("[INFO] Téléchargement du flux GTFS-RT...")
        response = requests.get(URL_RT, timeout=30)
        response.raise_for_status()
        with open(PB_PATH, "wb") as f:
            f.write(response.content)

        # Parser le flux protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        with open(PB_PATH, "rb") as f:
            feed.ParseFromString(f.read())

        # Extraire les trip updates
        rows = []
        for entity in feed.entity:
            if entity.HasField("trip_update"):
                trip_update = entity.trip_update
                trip_id = trip_update.trip.trip_id

                for stop_time_update in trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    delay_sec = None

                    # Vérifie arrival.delay
                    if stop_time_update.HasField("arrival") and stop_time_update.arrival.HasField("delay"):
                        delay_sec = stop_time_update.arrival.delay
                    # Si pas d'arrival.delay, regarde departure.delay
                    elif stop_time_update.HasField("departure") and stop_time_update.departure.HasField("delay"):
                        delay_sec = stop_time_update.departure.delay

                    rows.append([trip_id, stop_id, delay_sec])


        # Créer un DataFrame
        df = pd.DataFrame(rows, columns=["trip_id", "stop_id", "delay_sec"])


        # Simulation de retards si vide (utile pour tester la fusion et Power BI)
        if df["delay_sec"].notna().sum() == 0:
            print("[WARN] Aucun retard détecté dans le flux GTFS-RT pour le moment.")
        #     import numpy as np
        #     df["delay_sec"] = np.random.choice([0, 60, 120, 300], size=len(df))
        
        # Sauvegarder en CSV
        df.to_csv(CSV_PATH, index=False)

        print(f"[OK] Trip updates sauvegardés dans {CSV_PATH}")
        print(df.head())

    except Exception as e:
        print(f"[ERREUR] Impossible de parser le GTFS-RT : {e}")





