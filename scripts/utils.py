import os
import time
import logging

DATA_DIR = "data"
EXPORTS_DIR = "exports"


def cleanup_old_files(days=7):
    """Supprime les fichiers plus vieux que X jours dans data/ et exports/"""
    cutoff = time.time() - days * 86400
    removed = []

    for base_dir in [DATA_DIR, EXPORTS_DIR]:
        for root, _, files in os.walk(base_dir):
            for f in files:
                path = os.path.join(root, f)
                if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed.append(path)

    logging.info(f"[CLEANUP] {len(removed)} fichiers supprimés : {removed}")
    return f"Cleanup terminé : {len(removed)} fichiers supprimés"


def alert_if_failed(**context) -> None:
    """
    Callback appelé si une tâche échoue.
    Log l'échec avec dag_id et task_id.
    """
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id if task_instance else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"

    logging.error(f"[ALERTE] Tâche échouée dans {dag_id} → {task_id}")
