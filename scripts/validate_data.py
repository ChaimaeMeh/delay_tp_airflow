import logging
from pathlib import Path
from typing import Optional, Union

def validate_data(
    static_dir: Union[str, Path],
    rt_dir: Optional[Union[str, Path]] = None
) -> str:
    """
    Valide la présence et la cohérence des fichiers GTFS statiques et temps réel.

    Params:
        static_dir : dossier contenant les fichiers GTFS statiques (txt)
        rt_dir     : dossier contenant les CSV temps réel (trip updates, vehicle positions)
    
    Returns:
        str : message de validation réussie
    """
    # Convertir en Path si nécessaire
    static_dir = Path(static_dir)
    if rt_dir is not None:
        rt_dir = Path(rt_dir)

    # --- Fichiers statiques ---
    required_static = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
    missing_static = []

    for f in required_static:
        file_path = static_dir / f
        if not file_path.exists():
            missing_static.append(f)
        elif file_path.stat().st_size == 0:
            logging.error(f"Le fichier statique {f} est vide")
            raise ValueError(f"Le fichier statique {f} est vide")

    if missing_static:
        logging.error(f"Fichiers statiques manquants : {missing_static}")
        raise RuntimeError(f"Fichiers statiques manquants : {missing_static}")

    logging.info("Validation GTFS statique OK")

    # --- Fichiers temps réel ---
    if rt_dir is not None:
        required_rt_prefixes = ["trip_updates", "vehicle_positions"]
        missing_rt = []

        for prefix in required_rt_prefixes:
            matching_files = list(rt_dir.glob(f"{prefix}*.csv"))
            if not matching_files:
                missing_rt.append(f"{prefix}.csv")
            else:
                # Vérifie que le premier fichier trouvé n'est pas vide
                if matching_files[0].stat().st_size == 0:
                    missing_rt.append(f"{prefix}.csv")

        if missing_rt:
            logging.error(f"Fichiers temps réel manquants ou vides : {missing_rt}")
            raise RuntimeError(f"Fichiers temps réel manquants ou vides : {missing_rt}")

        logging.info("Validation GTFS temps réel OK")

    logging.info("Toutes les données sont présentes et valides")
    return "Validation réussie"
