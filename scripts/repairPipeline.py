# scripts/repairpipeline.py

import subprocess
import os
from datetime import datetime

SEASONS = list(range(2023, 2003, -1))  # z. B. 2023 bis 2004 absteigend
SCRIPT_DIR = "scripts"
LOG_DIR = "logs_partial"
os.makedirs(LOG_DIR, exist_ok=True)

REPAIR_SCRIPT = "fix_missing_hand_and_rank_from_logs.py"

SCRIPTS = [
    REPAIR_SCRIPT,  # Nur einmal am Anfang ausführen
    "04.5_renrich_matches_with_repaired_info.py",
    "05_enrich_and_format.py",
    "07_merge_enriched_results.py",  # optional
]

def run_pipeline_for_season(season, run_repair=False):
    log_file_path = os.path.join(LOG_DIR, f"{season}_partial.log")
    with open(log_file_path, "w") as log_file:
        print(f"\n=== 🔁 Starte Teil-Pipeline ab 04.5 für {season} ===")
        log_file.write(f"=== Teilpipeline Start für {season} @ {datetime.now().isoformat()} ===\n\n")

        env = os.environ.copy()
        env["SEASON"] = str(season)

        for script in SCRIPTS:
            if script == REPAIR_SCRIPT and not run_repair:
                continue  # Nur einmal zu Beginn ausführen

            script_path = os.path.join(SCRIPT_DIR, script)
            print(f"[{season}] ▶️ {script}")
            log_file.write(f"▶️ {script}\n")

            process = subprocess.Popen(
                ["python", script_path, str(season)],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            for line in process.stdout:
                print(line, end="")
                log_file.write(line)

            process.wait()
            if process.returncode != 0:
                print(f"[{season}] ❌ Fehler in {script} (siehe {log_file_path})")
                log_file.write("\n--- Fehler ---\n")
                break
            else:
                log_file.write("✅ Success\n\n")

        log_file.write(f"\n=== Teilpipeline fertig für {season} @ {datetime.now().isoformat()} ===\n")
        print(f"[{season}] ✅ Teilpipeline abgeschlossen")

if __name__ == "__main__":
    for idx, season in enumerate(SEASONS):
        run_pipeline_for_season(season, run_repair=(idx == 0))
