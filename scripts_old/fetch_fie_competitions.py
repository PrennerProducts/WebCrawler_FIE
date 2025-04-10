import os
import requests
import pandas as pd
from tqdm import tqdm

base_dir = "data"
os.makedirs(base_dir, exist_ok=True)

all_competitions = []

for season in tqdm(range(2004, 2024)):
    season_data = []
    
    for gender in ["m", "f"]:
        page = 1

        while True:
            payload = {
                "name": "",
                "status": "passed",
                "gender": [gender],
                "weapon": ["e", "f", "s"],
                "type": ["i"],
                "season": str(season),
                "level": "s",
                "competitionCategory": "",
                "fromDate": "",
                "toDate": "",
                "fetchPage": page
            }

            headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://fie.org",
                "Referer": "https://fie.org/competitions",
                "User-Agent": "Mozilla/5.0"
            }

            response = requests.post("https://fie.org/competitions/search", json=payload, headers=headers)
            data = response.json()

            competitions = data.get("items", [])
            if not competitions:
                break

            for comp in competitions:
                entry = {
                    "season": comp.get("season"),
                    "competitionId": comp.get("competitionId"),
                    "name": comp.get("name"),
                    "location": comp.get("location"),
                    "country": comp.get("country"),
                    "startDate": comp.get("startDate"),
                    "endDate": comp.get("endDate"),
                    "weapon": comp.get("weapon"),
                    "gender": comp.get("gender"),
                    "hasResults": comp.get("hasResults")
                }
                all_competitions.append(entry)
                season_data.append(entry)

            page += 1

    # Speichere eine CSV-Datei pro Saison
    df_season = pd.DataFrame(season_data)
    df_season.to_csv(os.path.join(base_dir, f"{season}.csv"), index=False)

# Optional: zentrale Übersicht
pd.DataFrame(all_competitions).to_csv(os.path.join(base_dir, "competition_master_2004_2023.csv"), index=False)
print("✅ Fertig: CSV-Dateien pro Season in 'data/'")
