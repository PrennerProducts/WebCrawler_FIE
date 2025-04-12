# scripts/03_parse_athletes_parallel.py

import pandas as pd
import re
import os
import sys
import time
from tqdm import tqdm
from multiprocessing import Process, cpu_count
from playwright.sync_api import sync_playwright

# === Argumente & Season-Handling ===
if len(sys.argv) != 2:
    print("Verwendung: python 03_parse_athletes_parallel.py <season>")
    sys.exit(1)

SEASON_YEAR = sys.argv[1]
SEASON_LABEL = f"{SEASON_YEAR}/{int(SEASON_YEAR)+1}"
INPUT_FILE = f"outputs/{SEASON_YEAR}/match_data_{SEASON_YEAR}_raw.csv"
OUTPUT_FILE = f"outputs/{SEASON_YEAR}/athlete_info_{SEASON_YEAR}_scraped.csv"
ERROR_LOG_FILE = f"logs/{SEASON_YEAR}_athlete_errors.log"

WAIT_AFTER_LOAD = 200
SLEEP_EVERY = 500
SLEEP_DURATION = 0.1
CHECKPOINT_INTERVAL = 500

N_WORKERS = min(4, cpu_count())  # z.B. 4 Prozesse parallel

# === Hilfsfunktionen ===
def extract_hand_from_text(text):
    pattern = re.compile(r"Hand\s*\n\s*(L|R|Left|Right)", re.IGNORECASE)
    match = pattern.search(text)
    if match:
        val = match.group(1).strip().upper()
        return "L" if val.startswith("L") else "R"
    return ""

def extract_year_end_rank_from_text(text, season=SEASON_LABEL):
    lines = text.splitlines()
    for i in range(len(lines) - 2):
        if lines[i+2].strip() == season and "(S)" in lines[i]:
            match = re.search(r"(\d+)(?:st|nd|rd|th)", lines[i])
            if match:
                return int(match.group(1))
    return None

def scrape_athlete_profile(athlete_id, page):
    try:
        url = f"https://fie.org/athletes/{athlete_id}"
        page.goto(url, timeout=10000)
        page.wait_for_timeout(WAIT_AFTER_LOAD)
        text = page.inner_text("body")

        hand = extract_hand_from_text(text)
        rank = extract_year_end_rank_from_text(text)

        return {"athleteId": athlete_id, "hand": hand, "rank": rank}
    except Exception as e:
        with open(ERROR_LOG_FILE, "a") as log:
            log.write(f"{athlete_id}: {e}\n")
        return {"athleteId": athlete_id, "hand": "", "rank": None}

def scrape_ids_worker(id_list, output_file):
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for i, athlete_id in enumerate(tqdm(id_list, desc=f"Worker {output_file}")):
            result = scrape_athlete_profile(athlete_id, page)
            results.append(result)

            if (i + 1) % CHECKPOINT_INTERVAL == 0:
                pd.DataFrame(results).to_csv(output_file, index=False)

            if i % SLEEP_EVERY == 0:
                time.sleep(SLEEP_DURATION)

        browser.close()
    pd.DataFrame(results).to_csv(output_file, index=False)

# === Main Logic ===
if not os.path.exists(INPUT_FILE):
    print(f"❌ Datei nicht gefunden: {INPUT_FILE}")
    sys.exit(0)

df = pd.read_csv(INPUT_FILE)
if df.empty:
    print(f"ℹ️ Keine Duelle in Datei {INPUT_FILE}, breche ab.")
    sys.exit(0)

all_ids = sorted(set(df["Athlete ID"]).union(df["Opponent ID"]))
all_ids = [i for i in all_ids if i > 0]

chunks = [all_ids[i::N_WORKERS] for i in range(N_WORKERS)]
worker_files = [f"outputs/{SEASON_YEAR}/athlete_info_worker_{i}.csv" for i in range(N_WORKERS)]

processes = []
for chunk, file in zip(chunks, worker_files):
    p = Process(target=scrape_ids_worker, args=(chunk, file))
    p.start()
    processes.append(p)

for p in processes:
    p.join()

# Zusammenführen
all_parts = [pd.read_csv(f) for f in worker_files if os.path.exists(f)]
pd.concat(all_parts, ignore_index=True).to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Alle Ergebnisse gespeichert in: {OUTPUT_FILE}")