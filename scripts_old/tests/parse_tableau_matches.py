import requests
import json
import re
import pandas as pd
from bs4 import BeautifulSoup

comp_id = 244  # Beispiel: World Championships
season_year = 2024  # URL-Segment
url = f"https://fie.org/competitions/{season_year}/{comp_id}"

html = requests.get(url).text
soup = BeautifulSoup(html, "html.parser")
script = soup.find("script", {"id": "js-competition"})

tableau_data = []
match = re.search(r"window\._tableau\s*=\s*(\[\{.*?\}\]);", script.string, re.DOTALL)
if match:
    tableau_data = json.loads(match.group(1))

rounds = tableau_data[0].get("rounds", {})

parsed_matches = []
for round_name, bouts in rounds.items():
    for bout in bouts:
        f1 = bout.get("fencer1", {})
        f2 = bout.get("fencer2", {})
        parsed_matches.append({
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

# Speichern
df = pd.DataFrame(parsed_matches)
df.to_csv("matches_comp244_tableau.csv", index=False)
print("✅ Fertig! Gespeichert unter matches_comp244_tableau.csv")
