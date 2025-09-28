import pandas as pd
from pathlib import Path

EXPORTS_DIR = Path("/opt/airflow/exports/kpi10")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

def compute_top_problematic_stops(fused_csv: Path, top_n=10) -> Path:
    df = pd.read_csv(fused_csv)

    # Ne garder que les retards
    df = df[df["delay_min"] > 0]

    # Agrégation par arrêt
    agg = df.groupby("stop_name").agg(
        nb_passages=("trip_id", "count"),
        avg_delay_min=("delay_min", "mean")
    ).reset_index()

    # Trier par moyenne de retard décroissant
    agg = agg.sort_values("avg_delay_min", ascending=False).head(top_n)

    # Sauvegarde
    output_file = EXPORTS_DIR / "top_problematic_stops.csv"
    agg.to_csv(output_file, index=False)

    return output_file
