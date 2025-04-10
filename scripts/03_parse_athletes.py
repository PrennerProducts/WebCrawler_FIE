import pandas as pd
import re
import os
import time
from tqdm import tqdm
from playwright.sync_api import sync_playwright

# === Konfiguration ===
INPUT_FILE = "match_data_2023_raw.csv"
OUTPUT_FILE = "athlete_info_2023_scraped.csv"
ERROR_LOG_FILE = "athlete_scrape_errors.log"
WAIT_AFTER_LOAD = 500   # ⏱️ Zeit nach Seitenladeende (ms)
SLEEP_EVERY = 100        # 💤 Wie oft pausieren
SLEEP_DURATION = 0.2     # 💤 Dauer pro Pause (Sekunden)
SEASON = "2023/2024"
CHECKPOINT_INTERVAL = 100

# === Parsing-Funktionen ===
def extract_hand_from_text(text):
    pattern = re.compile(r"Hand\s*\n\s*(L|R|Left|Right)", re.IGNORECASE)
    match = pattern.search(text)
    if match:
        val = match.group(1).strip().upper()
        return "L" if val.startswith("L") else "R"
    return ""

def extract_year_end_rank_from_text(text, season=SEASON):
    lines = text.splitlines()
    for i in range(len(lines) - 2):
        if lines[i+2].strip() == season and "(S)" in lines[i]:
            match = re.search(r"(\d+)(?:st|nd|rd|th)", lines[i])
            if match:
                return int(match.group(1))
    return None

# === Scraping-Profil eines Athleten ===
def scrape_athlete_profile(athlete_id, page):
    try:
        url = f"https://fie.org/athletes/{athlete_id}"
        page.goto(url, timeout=10000)
        page.wait_for_timeout(WAIT_AFTER_LOAD)
        text = page.inner_text("body")

        hand = extract_hand_from_text(text)
        rank = extract_year_end_rank_from_text(text)

        print(f"[{athlete_id}] 🧴 Hand: {hand}, 📊 Rank: {rank}")
        return {"athleteId": athlete_id, "hand": hand, "rank": rank}
    except Exception as e:
        with open(ERROR_LOG_FILE, "a") as log:
            log.write(f"{athlete_id}: {e}\n")
        return {"athleteId": athlete_id, "hand": "", "rank": None}

# === Main Logic ===
if __name__ == "__main__":
    df = pd.read_csv(INPUT_FILE)
    all_ids = sorted(set(df["Athlete ID"]).union(df["Opponent ID"]))
    all_ids = [i for i in all_ids if i > 0]

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for i, athlete_id in enumerate(tqdm(all_ids, desc="Scraping Athletes")):
            result = scrape_athlete_profile(athlete_id, page)
            results.append(result)

            if (i + 1) % CHECKPOINT_INTERVAL == 0:
                pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

            if i % SLEEP_EVERY == 0:
                time.sleep(SLEEP_DURATION)

        browser.close()

    # Finales Speichern
    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

    print(f"\n📉 Scraping abgeschlossen. Daten gespeichert in: {OUTPUT_FILE}")
