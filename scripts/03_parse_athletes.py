import pandas as pd
import re
import os
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
from io import StringIO
from playwright.sync_api import sync_playwright

# === Konfiguration ===
INPUT_FILE = "match_data_2023_raw.csv"
OUTPUT_FILE = "athlete_info_2023_scraped.csv"
ERROR_LOG_FILE = "athlete_scrape_errors.log"
CHUNK_SIZE = 500
SLEEP_EVERY = 50
SLEEP_DURATION = 1
SEASON = "2023/2024"

# === Hilfsfunktionen ===

def extract_hand_from_text(text):
    """
    Extrahiert 'L' oder 'R' aus dem sichtbaren Text anhand des Musters 'Hand\\nL' oder 'Hand\\nR'.
    """
    pattern = re.compile(r"Hand\s*\n\s*(L|R|Left|Right)", re.IGNORECASE)
    match = pattern.search(text)
    if match:
        val = match.group(1).strip().upper()
        if val.startswith("L"):
            return "L"
        if val.startswith("R"):
            return "R"
    return ""





def extract_year_end_rank_from_text(text, season=SEASON):
    """
    Extrahiert den Jahresend-Rank aus dem sichtbaren Text.
    Sucht z. B.:
        22nd (S)
        77.000
        2023/2024
    """
    lines = text.splitlines()
    for i in range(len(lines) - 2):
        if lines[i+2].strip() == season and "(S)" in lines[i]:
            match = re.search(r"(\d+)(?:st|nd|rd|th)", lines[i])
            if match:
                return int(match.group(1))
    return None




def scrape_athlete_profile(athlete_id, playwright):
    url = f"https://fie.org/athletes/{athlete_id}"
    try:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=10000)
        page.wait_for_timeout(2500)

        full_text = page.inner_text("body")
        browser.close()

        hand = extract_hand_from_text(full_text)
        rank = extract_year_end_rank_from_text(full_text)

        print(f"[{athlete_id}] 🫴 Hand: {hand}, 📊 Rank: {rank}")
        return {"athleteId": athlete_id, "hand": hand, "rank": rank}

    except Exception as e:
        with open(ERROR_LOG_FILE, "a") as log:
            log.write(f"{athlete_id}: {e}\n")
        return {"athleteId": athlete_id, "hand": "", "rank": None}




# === Main-Scraper ===
if __name__ == "__main__":
    df = pd.read_csv(INPUT_FILE)
    all_ids = set(df["Athlete ID"]).union(df["Opponent ID"])
    all_ids = sorted(i for i in all_ids if i > 0)

    already_scraped = set()
    if os.path.exists(OUTPUT_FILE):
        df_existing = pd.read_csv(OUTPUT_FILE)
        already_scraped = set(df_existing["athleteId"].astype(int))

    remaining_ids = [i for i in all_ids if i not in already_scraped]
    remaining_ids = remaining_ids[:20]  # 👈 Nur 20 Athleten für Testlauf

    print(f"🔄 Starte Scraping für {len(remaining_ids)} verbleibende Athleten...")

    results = []
    with sync_playwright() as playwright:
        for i, athlete_id in enumerate(tqdm(remaining_ids)):
            result = scrape_athlete_profile(athlete_id, playwright)
            results.append(result)

            if i % SLEEP_EVERY == 0:
                time.sleep(SLEEP_DURATION)

    # ✅ Nach dem Loop speichern
    if results:
        df_new = pd.DataFrame(results)
        if os.path.exists(OUTPUT_FILE):
            df_old = pd.read_csv(OUTPUT_FILE)
            df_combined = pd.concat([df_old, df_new]).drop_duplicates("athleteId")
        else:
            df_combined = df_new
        df_combined.to_csv(OUTPUT_FILE, index=False)

    print(f"\n✅ Scraping abgeschlossen. Daten gespeichert in: {OUTPUT_FILE}")
