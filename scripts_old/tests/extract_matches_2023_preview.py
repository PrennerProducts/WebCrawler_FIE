import requests
import pandas as pd
import json
import re
from bs4 import BeautifulSoup

competitions_2023 = pd.read_csv("data/2023.csv")
valid_comps = competitions_2023[competitions_2023["hasResults"] == 1].head(3)

all_matches = []

for _, row in valid_comps.iterrows():
    comp_id = row["competitionId"]
    url = f"https://fie.org/competition/{int(comp_id)}"
    print(f"🔍 Lade {url}...")

    try:
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", {"id": "js-competition"})

        if not script:
            print(f"⚠️ Kein <script> für {comp_id}")
            continue

        match = re.search(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});", script.string, re.DOTALL)
        if not match:
            print(f"⚠️ Kein JSON in {comp_id}")
            continue

        data = json.loads(match.group(1))
        matches = data.get("competition", {}).get("matches", [])

        for m in matches:
            all_matches.append({
                "Gender": row["gender"],
                "Weapon": row["weapon"],
                "Season": row["season"],
                "Competition": row["name"],
                "Date of Duel": m.get("date"),
                "Athlete Name": m.get("athlete", {}).get("name"),
                "Athlete Hand": m.get("athlete", {}).get("hand"),
                "Opponent Name": m.get("opponent", {}).get("name"),
                "Opponent Hand": m.get("opponent", {}).get("hand"),
                "Result": "Win" if m.get("winner") == "athlete" else "Loss",
                "Hit": m.get("athlete", {}).get("score"),
                "Counter-Hit": m.get("opponent", {}).get("score")
            })
    except Exception as e:
        print(f"💥 Fehler bei Competition {comp_id}: {e}")

# DataFrame + speichern
df_matches = pd.DataFrame(all_matches)
df_matches.to_csv("matches_2023_preview.csv", index=False)
print("✅ Matchdaten gespeichert in matches_2023_preview.csv")
