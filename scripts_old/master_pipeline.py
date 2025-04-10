# master_pipeline.py

import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path
from datetime import datetime

# =============================================
# CONFIG
# =============================================
DATA_DIR = Path("data")
MATCH_FILE = DATA_DIR / "matches_2023_enriched.csv"
RANKING_FILE = DATA_DIR / "year_end_rankings_2023_sample_fixed.csv"
COMPETITION_FILE = DATA_DIR / "2023.csv"
OUTPUT_CSV = "fencing_match_analysis_2023_master.csv"

# =============================================
# REGEX für Ranking-Block
# =============================================
RANKING_RE = re.compile(r"window\._tabRanking\s*=\s*(\[.*?\]);", re.DOTALL)

# =============================================
# Hilfsfunktionen
# =============================================
def extract_ranking_from_html(text):
    match = RANKING_RE.search(text)
    if not match:
        return []
    try:
        return eval(match.group(1))
    except Exception:
        return []

def extract_name_from_html(text):
    soup = BeautifulSoup(text, "html.parser")
    title = soup.find("h1", class_="PageTitle")
    return title.text.strip() if title else None

def extract_hand_from_html(text):
    soup = BeautifulSoup(text, "html.parser")
    for row in soup.find_all("div", class_="FencerProfileRow"):
        if "Hand" in row.text:
            val = row.find("div", class_="FencerProfileRight")
            return val.text.strip() if val else None
    return None

def scrape_athlete_profile(athlete_id):
    url = f"https://fie.org/athletes/{athlete_id}"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    html = response.text
    name = extract_name_from_html(html)
    hand = extract_hand_from_html(html)
    rankings = extract_ranking_from_html(html)
    for r in rankings:
        if r.get("season") in ["2023", "2022/2023", "2023/2024"] and r.get("category") == "S":
            return {
                "athleteId": athlete_id,
                "name": name,
                "weapon": r.get("weapon"),
                "season": r.get("season"),
                "rank": r.get("rank"),
                "points": r.get("point"),
                "hand": hand
            }
    return None

# =============================================
# MAIN PIPELINE
# =============================================
def main():
    print("🚀 Starte Master-Pipeline")

    print("📆 Lade Wettbewerbsdaten für 2023...")
    df_comp = pd.read_csv(COMPETITION_FILE)
    df_comp["Date"] = pd.to_datetime(df_comp["startDate"], errors="coerce")
    df_dates = df_comp[["name", "Date"]].drop_duplicates()
    comp_to_date = dict(zip(df_dates["name"], df_dates["Date"]))

    print("📦 Lade Match- und Rankingdaten...")
    df = pd.read_csv(MATCH_FILE)
    df_ranks = pd.read_csv(RANKING_FILE)

    print("🧠 Ergänze Ranking-Händigkeit...")
    name_to_hand = dict(zip(df_ranks["name"], df_ranks["hand"]))
    name_to_rank = dict(zip(df_ranks["name"], df_ranks["rank"]))

    df["Fencer1 Rank"] = df["Fencer1 Name"].map(name_to_rank)
    df["Fencer1 Hand"] = df["Fencer1 Name"].map(name_to_hand)
    df["Fencer2 Rank"] = df["Fencer2 Name"].map(name_to_rank)
    df["Fencer2 Hand"] = df["Fencer2 Name"].map(name_to_hand)

    if "Date of Duel" not in df.columns:
        df["Date of Duel"] = df["Competition"].map(comp_to_date)

    df["Result"] = df["Fencer1 Winner"].apply(lambda x: "Won" if x else "Loss")

    df_final = pd.DataFrame({
        "Gender": df["Gender"],
        "Weapon": df["Weapon"],
        "Season": df["Season"],
        "Competition": df["Competition"],
        "Date of Duel": df["Date of Duel"],
        "Athlete Name": df["Fencer1 Name"],
        "Athlete Year End World Ranking": df["Fencer1 Rank"],
        "Athlete Hand": df["Fencer1 Hand"],
        "Opponent Name": df["Fencer2 Name"],
        "Opponent Year End World Ranking": df["Fencer2 Rank"],
        "Opponent Hand": df["Fencer2 Hand"],
        "Result": df["Result"],
        "Hit": df["Fencer1 Score"],
        "Counter-Hit": df["Fencer2 Score"]
    })

    df_final.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Export abgeschlossen: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()