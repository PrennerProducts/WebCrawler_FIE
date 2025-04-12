import subprocess
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

SEASONS = list(range(2004, 2024))  # oder +1 für 2024
SCRIPTS = [
    "01_fetch_competitions.py",
    "02_parse_matches.py",
    "03_parse_athletes.py",
    "04_enrich_matches.py",
    "05_enrich_and_format.py"
]

def run_pipeline_for_season(season):
    try:
        env = os.environ.copy()
        env["SEASON"] = str(season)

        for script in SCRIPTS:
            print(f"[{season}] ▶️ {script}")
            subprocess.run(
                ["python", os.path.join("scripts", script)],
                env=env,
                check=True
            )
        return f"[{season}] ✅ erfolgreich abgeschlossen"
    except subprocess.CalledProcessError as e:
        return f"[{season}] ❌ Fehler bei: {e.cmd}"

def main():
    max_parallel = os.cpu_count() // 2 or 2  # Sicherheitshalber nicht alles gleichzeitig
    print(f"🚀 Starte parallele Verarbeitung mit {max_parallel} Workers...\n")

    with ProcessPoolExecutor(max_workers=max_parallel) as executor:
        futures = [executor.submit(run_pipeline_for_season, s) for s in SEASONS]

        for future in as_completed(futures):
            print(future.result())

if __name__ == "__main__":
    main()
