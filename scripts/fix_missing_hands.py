# scripts/fix_missing_hands.py

import pandas as pd
import re
from tqdm import tqdm
from playwright.sync_api import sync_playwright

INPUT_FILE = "athlete_info_2023_scraped_fixed.csv"
MISSING_FILE = "athletes_missing_hand.csv"
OUTPUT_FILE = "athlete_info_2023_scraped_fixed_fixed.csv"
WAIT_MS = 300

def extract_hand_from_text(text):
    match = re.search(r"Hand\s*(?:\n|:)\s*(L|R|Left|Right)", text, re.IGNORECASE)
    if match:
        val = match.group(1).strip().upper()
        return "L" if val.startswith("L") else "R"
    return ""

def scrape_hand(athlete_id, page):
    try:
        url = f"https://fie.org/athletes/{athlete_id}"
        page.goto(url, timeout=10000)
        page.wait_for_timeout(WAIT_MS)
        text = page.inner_text("body")
        hand = extract_hand_from_text(text)
        print(f"[{athlete_id}] ➤ Hand: {hand}")
        return hand
    except Exception as e:
        print(f"[{athlete_id}] ⚠️ Fehler: {e}")
        return ""

if __name__ == "__main__":
    df_all = pd.read_csv(INPUT_FILE)
    df_missing = pd.read_csv(MISSING_FILE)

    id_set = set(df_missing["athleteId"])
    update_map = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for aid in tqdm(id_set, desc="🔁 Fixing hands"):
            hand = scrape_hand(aid, page)
            if hand:
                update_map[aid] = hand

        browser.close()

    # Update original DataFrame
    df_all["hand"] = df_all.apply(
        lambda row: update_map.get(row["athleteId"], row["hand"]),
        axis=1
    )

    df_all.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Neue Datei gespeichert unter: {OUTPUT_FILE}")
