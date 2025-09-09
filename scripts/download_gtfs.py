import requests
import zipfile
import os
import pandas as pd
import logging

# URL GTFS statique Lignes d’Azur
URL = "https://chouette.enroute.mobi/api/v1/datas/OpendataRLA/gtfs.zip"
DATA_DIR = "data"
ZIP_PATH = os.path.join(DATA_DIR, "gtfs_static.zip")
OUTPUT_DIR = os.path.join(DATA_DIR, "gtfs_static")

def download_gtfs():
    # dossier local
    os.makedirs(DATA_DIR, exist_ok=True)

    # téléchargement
    try:
        logging.info("Téléchargement du GTFS statique...")
        response = requests.get(URL, timeout=30)
        response.raise_for_status()

        with open(ZIP_PATH, "wb") as f:
            f.write(response.content)

    # extraction
        logging.info("Extraction du ZIP...")
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall(OUTPUT_DIR)

        logging.info(f"Fichiers extraits dans {OUTPUT_DIR}/")



   # Aperçu rapide (facultatif)
        stop_times_path = os.path.join(OUTPUT_DIR, "stop_times.txt")
        stop_times_df = pd.read_csv(stop_times_path, nrows=5)
        logging.info("Aperçu stop_times.txt :\n%s", stop_times_df.head())

        return f"Téléchargement réussi : {OUTPUT_DIR}"

    except Exception as e:
        logging.error(f"Erreur téléchargement/extraction GTFS : {e}")
        raise RuntimeError(f"Impossible de télécharger ou d'extraire le GTFS : {e}")