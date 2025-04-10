import re
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

def get_tableau_json_from_competition_page(season, competition_id):
    url = f"https://fie.org/competitions/{season}/{competition_id}?tab=results"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find("script", id="js-competition")
        if not script:
            print(f"[{competition_id}] ❌ Kein <script id='js-competition'> gefunden.")
            return None
        match = re.search(r"window\._tableau\s*=\s*(\[.*?\]);", script.string or "", re.DOTALL)
        if not match:
            print(f"[{competition_id}] ❌ Kein window._tableau gefunden.")
            return None
        return json.loads(match.group(1))
    except Exception as e:
        print(f"[{competition_id}] ⚠️ Fehler beim Abrufen/Parsen: {e}")
        return None

def parse_matches_from_json(tableau_json, competition_id):
    parsed = []
    for suite in tableau_json:
        for round_name, matches in suite.get("rounds", {}).items():
            for m in matches:
                f1, f2 = m["fencer1"], m["fencer2"]
                parsed.append({
                    "Competition ID": competition_id,
                    "Round": round_name,
                    "Athlete Name": f1["name"],
                    "Athlete ID": f1["id"],
                    "Athlete Nation": f1["nationality"],
                    "Opponent Name": f2["name"],
                    "Opponent ID": f2["id"],
                    "Opponent Nation": f2["nationality"],
                    "Result": "Won" if f1["isWinner"] else "Loss",
                    "Hit": f1["score"],
                    "Counter-Hit": f2["score"]
                })
    return parsed

def scrape_all_matches(competitions_csv):
    df_comp = pd.read_csv(competitions_csv)
    all_matches = []
    for _, row in tqdm(df_comp.iterrows(), total=len(df_comp)):
        comp_id, season = row["Competition ID"], row["Season"]
        tableau = get_tableau_json_from_competition_page(season, comp_id)
        if tableau:
            all_matches.extend(parse_matches_from_json(tableau, comp_id))
    return pd.DataFrame(all_matches)

if __name__ == "__main__":
    competitions_csv = "competitions_2023.csv"
    output_file = "match_data_2023_raw.csv"
    df = scrape_all_matches(competitions_csv)
    df.to_csv(output_file, index=False)
    print(f"\n✅ {len(df)} Matches gespeichert nach: {output_file}")
