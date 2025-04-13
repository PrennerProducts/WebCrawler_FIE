import subprocess
import os
from datetime import datetime

SEASONS = list(range(2024, 2003, -1))  # von 2024 bis einschließlich 2004
SCRIPT_DIR = "scripts"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

SCRIPTS = [
    "01_fetch_competitions.py",
    "02_parse_matches.py",
    "03_parse_athletes.py",
    "04_enrich_matches.py",
    "05_enrich_and_format.py"
]

def run_pipeline_for_season(season):
    
    log_file_path = os.path.join(LOG_DIR, f"{season}.log")
    with open(log_file_path, "w") as log_file:
        print(f"\n=== 🔁 Starte Pipeline für {season} ===")
        log_file.write(f"=== Pipeline Start für {season} @ {datetime.now().isoformat()} ===\n\n")

        env = os.environ.copy()
        env["SEASON"] = str(season)

        for script in SCRIPTS:
            script_path = os.path.join(SCRIPT_DIR, script)
            print(f"[{season}] ▶️ {script}")
            log_file.write(f"▶️ {script}\n")

            process = subprocess.Popen(
                ["python", script_path, str(season)],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                # stdout=None,      # direkte Weiterleitung an Terminal
                # stderr=None,       # auch Fehler anzeigen (inkl. tqdm!)
                text=True,
                bufsize=1
            )

            # Echtzeit-Ausgabe an stdout UND log schreiben
            for line in process.stdout:
                print(line, end="")       # Konsole
                log_file.write(line)      # Logdatei

            process.wait()
            if process.returncode != 0:
                print(f"[{season}] ❌ Fehler in {script} (siehe {log_file_path})")
                log_file.write("\n--- Fehler ---\n")
                break
            else:
                log_file.write("✅ Success\n\n")

        log_file.write(f"\n=== Pipeline fertig für {season} @ {datetime.now().isoformat()} ===\n")
        print(f"[{season}] ✅ Pipeline abgeschlossen")

if __name__ == "__main__":
    for season in SEASONS:
        run_pipeline_for_season(season)