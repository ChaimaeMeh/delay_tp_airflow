import pandas as pd
import os
import logging

EXPORTS_DIR = "exports"
DATA_DIR = "/opt/airflow/data/gtfs_static"
CSV_PATH = os.path.join(EXPORTS_DIR, "trip_updates1.csv")
OUTPUT_PATH = os.path.join(EXPORTS_DIR, "fused_trip_delays.csv")



def fuse_gtfs():
    # GTFS statique
    try:
        logging.info("Lecture des fichiers GTFS statiques...")
        stop_times = pd.read_csv(os.path.join(DATA_DIR, "stop_times.txt"))
        trips = pd.read_csv(os.path.join(DATA_DIR, "trips.txt"))
        stops = pd.read_csv(os.path.join(DATA_DIR, "stops.txt"))
        routes = pd.read_csv(os.path.join(DATA_DIR, "routes.txt"))
        

        # GTFS-RT après parsing
        trip_updates = pd.read_csv(CSV_PATH)

        # Convertir les IDs en string pour éviter les problèmes de merge
        for df in [trip_updates, stop_times, stops]:
            df["stop_id"] = df["stop_id"].astype(str)


        # Créer une colonne 'delay_min'
        trip_updates['delay_min'] = trip_updates['delay_sec'] / 60


        # On garde uniquement les colonnes utiles de stop_times
        stop_times_small = stop_times[['trip_id', 'stop_id', 'arrival_time', 'departure_time']]


        # Fusion avec trip_updates sur trip_id et stop_id et route_id et nom et coordonées des arrêts , nom de la ligne
        merged = pd.merge(trip_updates, stop_times_small, on=['trip_id', 'stop_id'], how='left') # Fusion avec stop_times
        merged = pd.merge(merged, trips[['trip_id', 'route_id']], on='trip_id', how='left')
        merged = pd.merge(merged, stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], on='stop_id', how='left') #infos arrêts
        merged = pd.merge(merged, routes[['route_id', 'route_short_name', 'route_long_name']], on='route_id', how='left') #infos lignes

        # Nettoyage
        merged = merged.sort_values(by=["trip_id", "arrival_time"])
        merged.fillna({"delay_sec": 0, "delay_min": 0}, inplace=True)

        merged.to_csv(OUTPUT_PATH, index=False)
        logging.info(f"Fusion réussie, fichier généré : {OUTPUT_PATH}")
        return f"Fusion réussie : {OUTPUT_PATH}"

    except Exception as e:
        logging.error(f"Erreur fusion GTFS : {e}")
        raise RuntimeError(f"Fusion impossible : {e}")

