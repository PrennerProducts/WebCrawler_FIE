import sys
import os
import pandas as pd
import re
from tqdm import tqdm
from playwright.sync_api import sync_playwright

# === Argumente & Pfade ===
if len(sys.argv) != 2:
    print("Verwendung: python 04_enrich_matches.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]
SEASON_LABEL = f"{SEASON}/{int(SEASON)+1}"
MATCH_FILE = f"outputs/{SEASON}/match_data_{SEASON}_raw.csv"
ATHLETE_FILE = f"outputs/{SEASON}/athlete_info_{SEASON}_scraped.csv"
OUTPUT_FILE = f"outputs/{SEASON}/fencing_match_analysis_{SEASON}_master.csv"
SCRAPE_LOG_FILE = f"logs/scraped_missing_{SEASON}.csv"
SCRAPE_FAIL_LOG = f"logs/scrape_failures_{SEASON}.log"

WAIT_MS = 300

# === Extraktionsfunktionen ===
def extract_hand(text):
    match = re.search(r"Hand\s*(?:\n|:)\s*(L|R|Left|Right)", text, re.IGNORECASE)
    if match:
        val = match.group(1).strip().upper()
        return "L" if val.startswith("L") else "R"
    return ""

def extract_rank(text, season=SEASON_LABEL):
    lines = text.splitlines()
    for i in range(len(lines) - 2):
        if lines[i+2].strip() == season and "(S)" in lines[i]:
            match = re.search(r"(\d+)(?:st|nd|rd|th)", lines[i])
            if match:
                return int(match.group(1))
    return None

def scrape_missing(athlete_ids):
    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for aid in tqdm(athlete_ids, desc="🔁 Nachscrape fehlender Daten"):
            try:
                page.goto(f"https://fie.org/athletes/{aid}", timeout=10000)
                page.wait_for_timeout(WAIT_MS)
                text = page.inner_text("body")
                results[aid] = {
                    "hand": extract_hand(text),
                    "rank": extract_rank(text)
                }
            except Exception as e:
                with open(SCRAPE_FAIL_LOG, "a") as log:
                    log.write(f"{aid}: {e}\n")
        browser.close()
    # Speichern der Ergebnisse
    if results:
        pd.DataFrame([
            {"athleteId": aid, "hand": v["hand"], "rank": v["rank"]}
            for aid, v in results.items()
        ]).to_csv(SCRAPE_LOG_FILE, index=False)
    return results

# === Hilfsfunktion für leere oder NaN-Werte ===
def is_missing(val):
    return pd.isna(val) or str(val).strip() == ""

# === Laden ===
if not os.path.exists(MATCH_FILE) or os.path.getsize(MATCH_FILE) == 0:
    print(f"⚠️ Keine Matchdaten vorhanden für {SEASON}, breche ab.")
    sys.exit(0)

if not os.path.exists(ATHLETE_FILE) or os.path.getsize(ATHLETE_FILE) == 0:
    print(f"⚠️ Keine Athleteninfos vorhanden für {SEASON}, breche ab.")
    sys.exit(0)

matches_df = pd.read_csv(MATCH_FILE)
athletes_df = pd.read_csv(ATHLETE_FILE)

if matches_df.empty:
    print(f"ℹ️ Matchdatei ist leer für {SEASON}, breche ab.")
    sys.exit(0)

# === Umbenennen zur Unterscheidung ===
athletes_a = athletes_df.rename(columns={"athleteId": "Athlete ID", "hand": "hand_a", "rank": "rank_a"})
athletes_b = athletes_df.rename(columns={"athleteId": "Opponent ID", "hand": "hand_b", "rank": "rank_b"})

# === Mergen ===
enriched = matches_df.merge(athletes_a, on="Athlete ID", how="left")
enriched = enriched.merge(athletes_b, on="Opponent ID", how="left")

# === Fehlende herausfiltern inkl. "" ===
missing_a = enriched[
    enriched["hand_a"].apply(is_missing) | enriched["rank_a"].apply(is_missing)
]["Athlete ID"].dropna().unique()
missing_b = enriched[
    enriched["hand_b"].apply(is_missing) | enriched["rank_b"].apply(is_missing)
]["Opponent ID"].dropna().unique()
missing_ids = set(missing_a).union(set(missing_b))

# === Live nachscrapen ===
if missing_ids:
    scraped = scrape_missing(missing_ids)

    enriched["hand_a"] = enriched.apply(
        lambda r: scraped.get(r["Athlete ID"], {}).get("hand", r["hand_a"]) if is_missing(r["hand_a"]) else r["hand_a"], axis=1)
    enriched["rank_a"] = enriched.apply(
        lambda r: scraped.get(r["Athlete ID"], {}).get("rank", r["rank_a"]) if is_missing(r["rank_a"]) else r["rank_a"], axis=1)
    enriched["hand_b"] = enriched.apply(
        lambda r: scraped.get(r["Opponent ID"], {}).get("hand", r["hand_b"]) if is_missing(r["hand_b"]) else r["hand_b"], axis=1)
    enriched["rank_b"] = enriched.apply(
        lambda r: scraped.get(r["Opponent ID"], {}).get("rank", r["rank_b"]) if is_missing(r["rank_b"]) else r["rank_b"], axis=1)

# === Speichern ===
enriched.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Matchdaten (inkl. Live-Reparatur) gespeichert für {SEASON}: {OUTPUT_FILE}")