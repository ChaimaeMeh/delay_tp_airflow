from pathlib import Path
import pandas as pd
import logging

EXPORTS_DIR = Path("/opt/airflow/exports/kpi9")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

def compute_delay_distribution(fused_csv: Path) -> Path:
    logging.info(f"Lecture du fichier : {fused_csv}")
    df = pd.read_csv(fused_csv)

    # S'assurer que delay_min existe et est bien numérique
    df["delay_min"] = pd.to_numeric(df.get("delay_min"), errors="coerce")

    # Supprimer les valeurs nulles
    df = df.dropna(subset=["delay_min"])

    # Ne garder que les retards (>= 0)
    df = df[df["delay_min"] >= 0]

    # Définir les classes de retard (binning)
    bins = [0, 2, 5, 10, 20, 9999]
    labels = ["0-2 min", "2-5 min", "5-10 min", "10-20 min", ">20 min"]

    df["delay_class"] = pd.cut(df["delay_min"], bins=bins, labels=labels, right=False)

    # Compter le nombre de bus par classe
    distribution = df.groupby("delay_class").size().reset_index(name="count")

    # Sauvegarde CSV
    output_file = EXPORTS_DIR / f"delay_distribution.csv"
    distribution.to_csv(output_file, index=False)
    logging.info(f"Résultat sauvegardé dans : {output_file}")

    return output_file
