import logging
import requests
import zipfile
from pathlib import Path
from datetime import datetime

def download_gtfs_static(url: str, data_dir: Path, timeout: int = 30) -> Path:
    """
    Télécharge le zip GTFS statique depuis `url`, l'extrait dans `data_dir/gtfs_static/<YYYYMMDD_HHMMSS>/`,
    et retourne le chemin du dossier extrait. Conserve l'idempotence (ne retélécharge/pass réécrit si déjà présent).
    """
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = data_dir / "gtfs_static" / run_ts
    zip_path = data_dir / "gtfs_static.zip"

    logging.info(" Démarrage téléchargement GTFS statique depuis %s", url)
    out_dir.mkdir(parents=True, exist_ok=True)

    if zip_path.exists():
        logging.info("Le fichier ZIP %s existe déjà — pas de nouveau téléchargement", zip_path)
    else:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            zip_path.write_bytes(resp.content)
            logging.info("Téléchargement réussi dans %s", zip_path)
        except Exception as e:
            logging.error("Erreur lors du téléchargement : %s", e, exc_info=True)
            raise

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(out_dir)
        logging.info("Extraction réussie dans %s", out_dir)
    except Exception as e:
        logging.error("Erreur lors de l'extraction du zip : %s", e, exc_info=True)
        raise

    return out_dir
