import re
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import sys
import os

# === Argument prüfen ===
if len(sys.argv) != 2:
    print("Usage: python 02_parse_matches.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]
INPUT_FILE = f"outputs/{SEASON}/competitions_{SEASON}.csv"
OUTPUT_FILE = f"outputs/{SEASON}/match_data_{SEASON}_raw.csv"

os.makedirs(f"outputs/{SEASON}", exist_ok=True)

# === Parsing-Logik ===
def get_tableau_json_from_competition_page(season, competition_id):
    url = f"https://fie.org/competitions/{season}/{competition_id}?tab=results"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find("script", id="js-competition")
        if not script:
            return None
        match = re.search(r"window\._tableau\s*=\s*(\[.*?\]);", script.string or "", re.DOTALL)
        if not match:
            return None
        return json.loads(match.group(1))
    except Exception:
        return None

def parse_matches_from_json(tableau_json, competition_id):
    parsed = []
    for suite in tableau_json:
        rounds = suite.get("rounds", {})
        if not rounds:
            print(f"[{competition_id}] ⚠️ Tableau leer (keine Runden vorhanden).")
            continue

        for round_name, matches in rounds.items():
            if not matches:
                print(f"[{competition_id}] ⚠️ Runde '{round_name}' hat keine Matches.")
                continue

            for m in matches:
                f1 = m.get("fencer1")
                f2 = m.get("fencer2")
                if not f1 or not f2:
                    print(f"[{competition_id}] ⚠️ Match mit unvollständigen Fechterdaten übersprungen.")
                    continue

                parsed.append({
                    "Competition ID": competition_id,
                    "Round": round_name,
                    "Athlete Name": f1.get("name", ""),
                    "Athlete ID": f1.get("id", -1),
                    "Athlete Nation": f1.get("nationality", ""),
                    "Opponent Name": f2.get("name", ""),
                    "Opponent ID": f2.get("id", -1),
                    "Opponent Nation": f2.get("nationality", ""),
                    "Result": "Won" if f1.get("isWinner") else "Loss",
                    "Hit": f1.get("score", 0),
                    "Counter-Hit": f2.get("score", 0)
                })
    return parsed


# === Main-Logik ===
def scrape_all_matches(competitions_csv):
    df_comp = pd.read_csv(competitions_csv)
    all_matches = []
    skipped = 0

    for _, row in tqdm(df_comp.iterrows(), total=len(df_comp), desc=f"{SEASON}"):
        comp_id, season = row["Competition ID"], row["Season"]
        tableau = get_tableau_json_from_competition_page(season, comp_id)
        if tableau:
            all_matches.extend(parse_matches_from_json(tableau, comp_id))
        else:
            skipped += 1

    print(f"\n📉 Fertig: {len(all_matches)} Duelle gefunden")
    print(f"⏭️ {skipped} Wettbewerbe ohne Tableau übersprungen")
    return pd.DataFrame(all_matches)

# === Ausführung ===
if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Eingabedatei nicht gefunden: {INPUT_FILE}")
        sys.exit(1)

    df = scrape_all_matches(INPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ {len(df)} Matches gespeichert nach: {OUTPUT_FILE}")
