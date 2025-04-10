import os
import requests
import json
import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

competitions_2023 = pd.read_csv("data/2023.csv")
valid_comps = competitions_2023[competitions_2023["hasResults"] == 1]

all_matches = []

def extract_tableau_matches(html, metadata):
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", {"id": "js-competition"})
    if not script:
        return []

    match = re.search(r"window\._tableau\s*=\s*(\[\{.*?\}\]);", script.string, re.DOTALL)
    if not match:
        return []

    tableau_data = json.loads(match.group(1))
    rounds = tableau_data[0].get("rounds", {})
    parsed = []

    for round_name, bouts in rounds.items():
        for bout in bouts:
            f1 = bout.get("fencer1", {})
            f2 = bout.get("fencer2", {})
            if not f1.get("name") or not f2.get("name"):
                continue
            parsed.append({
                "Gender": metadata["gender"],
                "Weapon": metadata["weapon"],
                "Season": metadata["season"],
                "Competition": metadata["name"],
                "Round": round_name,
                "Fencer1 Name": f1.get("name"),
                "Fencer1 Nation": f1.get("nationality"),
                "Fencer1 Score": f1.get("score"),
                "Fencer1 Winner": f1.get("isWinner"),
                "Fencer2 Name": f2.get("name"),
                "Fencer2 Nation": f2.get("nationality"),
                "Fencer2 Score": f2.get("score"),
                "Fencer2 Winner": f2.get("isWinner"),
                "Referees": ", ".join(bout.get("referees", []))
            })
    return parsed

# Scrape
for _, row in tqdm(valid_comps.iterrows(), total=valid_comps.shape[0]):
    comp_id = int(row["competitionId"])
    season_year = 2024  # weil Saison 2023/2024 → URL: /competitions/2024/{id}
    url = f"https://fie.org/competitions/{season_year}/{comp_id}"

    try:
        html = requests.get(url, timeout=15).text
        matches = extract_tableau_matches(html, row)
        all_matches.extend(matches)
    except Exception as e:
        print(f"Fehler bei Competition {comp_id}: {e}")

df = pd.DataFrame(all_matches)
df.to_csv("data/matches_2023_all.csv", index=False)
print("✅ Fertig! Alle Tableau-Matches gespeichert in data/matches_2023_all.csv")
