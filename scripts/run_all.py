import subprocess
import sys
import os

SCRIPTS_DIR = "scripts"

# Liste des scripts à exécuter dans l'ordre
PIPELINE = [
    "download_gtfs.py",
    "parse_trip_updates.py",
    "fused_gtfs.py"
]

def run_script(script_name):
    print(f"\n[INFO] ---- Lancement de {script_name} ----")
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPTS_DIR, script_name)],
            capture_output=True, text=True, check=True
        )
        print(result.stdout)
        if result.stderr:
            print("[WARN] STDERR :", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"[ERREUR] {script_name} a échoué.")
        print(e.stdout)
        print(e.stderr)
        sys.exit(1)

if __name__ == "__main__":
    print("[INFO] Début du pipeline GTFS 🚍⚡\n")
    for script in PIPELINE:
        run_script(script)
    print("\n[OK] Pipeline complet ✅")
    print("Résultat disponible dans: exports/fused_trip_delays.csv")
