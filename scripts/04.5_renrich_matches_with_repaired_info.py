import sys
import os
import pandas as pd
import re
from tqdm import tqdm
from playwright.sync_api import sync_playwright

# === Argumente & Pfade ===
if len(sys.argv) != 2:
    print("Verwendung: python 04.5_renrich_matches_with_repaired_info.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]
MATCH_FILE = f"outputs/{SEASON}/match_data_{SEASON}_raw.csv"
ATHLETE_FILE = f"outputs/{SEASON}/athlete_info_{SEASON}_scraped.csv"
REPAIRED_FILE = "athlete_info_repaired_global.csv"
OUTPUT_FILE = f"outputs/{SEASON}/fencing_match_analysis_{SEASON}_master.csv"

WAIT_MS = 300

# === Hilfsfunktion für Scraping ===
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
        return extract_hand_from_text(text)
    except Exception as e:
        print(f"[{athlete_id}] ⚠️ Fehler: {e}")
        return ""

# === Lade Daten ===
if not os.path.exists(MATCH_FILE) or os.path.getsize(MATCH_FILE) == 0:
    print(f"⚠️ Keine Matchdaten vorhanden für {SEASON}, breche ab.")
    sys.exit(0)

if not os.path.exists(ATHLETE_FILE) or os.path.getsize(ATHLETE_FILE) == 0:
    print(f"⚠️ Keine Athleteninfos vorhanden für {SEASON}, breche ab.")
    sys.exit(0)

if not os.path.exists(REPAIRED_FILE):
    print(f"⚠️ Repaired-Datei fehlt: {REPAIRED_FILE}")
    sys.exit(0)

matches_df = pd.read_csv(MATCH_FILE)
athletes_df = pd.read_csv(ATHLETE_FILE)
repaired_df = pd.read_csv(REPAIRED_FILE)

if matches_df.empty:
    print(f"ℹ️ Matchdatei ist leer für {SEASON}, breche ab.")
    sys.exit(0)

# === Repaired vorbereiten ===
repaired_df = repaired_df.rename(columns={
    "athleteId": "Athlete ID",
    "hand": "Fixed Hand",
    "rank": "Fixed Rank"
})
repaired_df["Athlete ID"] = repaired_df["Athlete ID"].astype(int)

# === Umbenennen für Merge ===
athletes_a = athletes_df.rename(columns={"athleteId": "Athlete ID", "hand": "hand_a", "rank": "rank_a"})
athletes_b = athletes_df.rename(columns={"athleteId": "Opponent ID", "hand": "hand_b", "rank": "rank_b"})

# === Mergen mit Athleteninfos ===
enriched = matches_df.merge(athletes_a, on="Athlete ID", how="left")
enriched = enriched.merge(athletes_b, on="Opponent ID", how="left")

# === Repaired mergen ===
enriched = enriched.merge(repaired_df, on="Athlete ID", how="left")
enriched = enriched.merge(repaired_df.rename(columns={
    "Athlete ID": "Opponent ID",
    "Fixed Hand": "Fixed Opponent Hand",
    "Fixed Rank": "Fixed Opponent Rank"
}), on="Opponent ID", how="left")

# === Fehlende ergänzen aus repaired.csv ===
enriched["hand_a"] = enriched.apply(lambda r: r["Fixed Hand"] if pd.isna(r["hand_a"]) else r["hand_a"], axis=1)
enriched["rank_a"] = enriched.apply(lambda r: r["Fixed Rank"] if pd.isna(r["rank_a"]) else r["rank_a"], axis=1)
enriched["hand_b"] = enriched.apply(lambda r: r["Fixed Opponent Hand"] if pd.isna(r["hand_b"]) else r["hand_b"], axis=1)
enriched["rank_b"] = enriched.apply(lambda r: r["Fixed Opponent Rank"] if pd.isna(r["rank_b"]) else r["rank_b"], axis=1)

# === Live-Scraping nur wenn noch fehlt ===
missing_ids = set()
if enriched["hand_a"].isna().any():
    missing_ids.update(enriched[pd.isna(enriched["hand_a"])]["Athlete ID"].dropna().astype(int))
if enriched["hand_b"].isna().any():
    missing_ids.update(enriched[pd.isna(enriched["hand_b"])]["Opponent ID"].dropna().astype(int))

if missing_ids:
    print(f"🔍 Starte Livescraping für {len(missing_ids)} fehlende Händigkeiten ...")
    scraped_hands = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for aid in tqdm(missing_ids):
            hand = scrape_hand(aid, page)
            if hand:
                scraped_hands[aid] = hand
        browser.close()

    # === Finales Ergänzen aus Livescraping ===
    enriched["hand_a"] = enriched.apply(
        lambda r: scraped_hands.get(r["Athlete ID"], r["hand_a"]) if pd.isna(r["hand_a"]) else r["hand_a"], axis=1)
    enriched["hand_b"] = enriched.apply(
        lambda r: scraped_hands.get(r["Opponent ID"], r["hand_b"]) if pd.isna(r["hand_b"]) else r["hand_b"], axis=1)

# === Aufräumen ===
enriched.drop(columns=["Fixed Hand", "Fixed Rank", "Fixed Opponent Hand", "Fixed Opponent Rank"], inplace=True)

# === Speichern ===
enriched.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Matchdaten angereichert + repariert + live ergänzt gespeichert für {SEASON}: {OUTPUT_FILE}")
