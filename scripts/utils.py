import os
import time
import logging
from datetime import datetime

DATA_DIR = "/opt/airflow/data"
EXPORTS_DIR = "/opt/airflow/exports"

def cleanup_old_files(days=7):

    """Supprime les fichiers et dossiers plus vieux que X jours dans data/ et exports/"""
    
    cutoff = time.time() - days * 86400
    removed = []

    for base_dir in [DATA_DIR, EXPORTS_DIR]:
        if not os.path.exists(base_dir):
            logging.warning(f"[CLEANUP] Le dossier {base_dir} n'existe pas")
            continue

        for root, dirs, files in os.walk(base_dir, topdown=False):
            for f in files:
                path = os.path.join(root, f)
                if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed.append(path)

            # Supprimer les dossiers vides
            for d in dirs:
                dir_path = os.path.join(root, d)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    removed.append(dir_path)

    logging.info(f"[CLEANUP] {len(removed)} éléments supprimés : {removed}")
    return f"Cleanup terminé : {len(removed)} éléments supprimés"


def alert_if_failed(**context) -> None:
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id if task_instance else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    execution_date = context.get("execution_date", datetime.now())
    try_number = task_instance.try_number if task_instance else "unknown"

    logging.error(
        f"[ALERTE] Échec détecté\n"
        f" - DAG : {dag_id}\n"
        f" - Tâche : {task_id}\n"
        f" - Date d'exécution : {execution_date}\n"
        f" - Tentative : {try_number}"
    )

    # Log exception si disponible
    exception = context.get("exception")
    if exception:
        logging.error(f" - Exception : {exception}")
