import os
import logging

DATA_DIR = "data/gtfs_static"
EXPORTS_DIR = "exports"


def validate_data():
    """Valide la présence et la cohérence des fichiers GTFS statiques et temps réel"""
    # Fichiers statiques
    required_static = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
    missing_static = []

    for f in required_static:
        file_path = os.path.join(DATA_DIR, f)
        if not os.path.exists(file_path):
            missing_static.append(f)
        elif os.path.getsize(file_path) == 0:
            logging.error(f"Le fichier statique {f} est vide")
            raise ValueError(f"Le fichier statique {f} est vide")

    if missing_static:
        logging.error(f"Fichiers statiques manquants : {missing_static}")
        raise RuntimeError(f"Fichiers statiques manquants : {missing_static}")

    logging.info("Validation GTFS statique OK")

    # Fichiers temps réel
    required_rt = ["trip_updates1.csv", "vehicle_positions.csv"]
    missing_rt = []

    for f in required_rt:
        file_path = os.path.join(EXPORTS_DIR, f)
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            missing_rt.append(f)

    if missing_rt:
        logging.error(f"Fichiers temps réel manquants ou vides : {missing_rt}")
        raise RuntimeError(f"Fichiers temps réel manquants ou vides : {missing_rt}")

    logging.info("Validation GTFS temps réel OK")
    logging.info("Toutes les données sont présentes et valides")

    return "Validation réussie"
