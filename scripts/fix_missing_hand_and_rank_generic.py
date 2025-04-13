import pandas as pd
import re
from tqdm import tqdm
from playwright.sync_api import sync_playwright
import sys
import os

# === Argumente ===
if len(sys.argv) != 4:
    print("Usage: python fix_missing_hand_and_rank_generic.py <input_file> <output_file> <season_label>")
    sys.exit(1)

INPUT_FILE = sys.argv[1]
OUTPUT_FILE = sys.argv[2]
SEASON_LABEL = sys.argv[3]  # z. B. "2020/2021"
WAIT_MS = 300

# === Extraktion aus Text ===
def extract_hand_from_text(text):
    match = re.search(r"Hand\s*(?:\n|:)\s*(L|R|Left|Right)", text, re.IGNORECASE)
    if match:
        val = match.group(1).strip().upper()
        return "L" if val.startswith("L") else "R"
    return ""

def extract_rank_from_text(text, season_label):
    lines = text.splitlines()
    for i in range(len(lines) - 2):
        if lines[i+2].strip() == season_label and "(S)" in lines[i]:
            match = re.search(r"(\d+)(?:st|nd|rd|th)", lines[i])
            if match:
                return int(match.group(1))
    return None

# === Scraping einer Seite ===
def scrape_profile(athlete_id, page):
    try:
        url = f"https://fie.org/athletes/{athlete_id}"
        page.goto(url, timeout=10000)
        page.wait_for_timeout(WAIT_MS)
        text = page.inner_text("body")
        hand = extract_hand_from_text(text)
        rank = extract_rank_from_text(text, SEASON_LABEL)
        print(f"[{athlete_id}] ➤ Hand: {hand}, Rank: {rank}")
        return hand, rank
    except Exception as e:
        print(f"[{athlete_id}] ⚠️ Fehler: {e}")
        return "", None

# === Hauptlogik ===
if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Datei nicht gefunden: {INPUT_FILE}")
        sys.exit(1)

    df = pd.read_csv(INPUT_FILE)
    if "athleteId" not in df.columns:
        print("❌ Spalte 'athleteId' fehlt")
        sys.exit(1)

    to_fix = df[
        df["hand"].isna() | (df["hand"] == "") |
        df["rank"].isna()
    ].copy()

    updates = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for _, row in tqdm(to_fix.iterrows(), total=len(to_fix), desc="🔁 Fixing hands & ranks"):
            aid = row["athleteId"]
            hand, rank = scrape_profile(aid, page)
            updates[aid] = {"hand": hand or row.get("hand", ""), "rank": rank if rank is not None else row.get("rank", None)}

        browser.close()

    # Update DataFrame
    def apply_fixes(row):
        fix = updates.get(row["athleteId"])
        if fix:
            row["hand"] = fix["hand"]
            row["rank"] = fix["rank"]
        return row

    df = df.apply(apply_fixes, axis=1)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Neue Datei gespeichert unter: {OUTPUT_FILE}")
