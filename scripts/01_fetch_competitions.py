import requests
import pandas as pd
import os
import sys

# === CLI-Argument prüfen ===
if len(sys.argv) != 2:
    print("Usage: python 01_fetch_competitions.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

# === Ausgabeordner & Dateiname vorbereiten ===
OUTPUT_DIR = os.path.join("outputs", SEASON)
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"competitions_{SEASON}.csv")

# === Request-Konfiguration ===
url = "https://fie.org/competitions/search"
headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://fie.org",
    "Referer": "https://fie.org/competitions",
    "User-Agent": "Mozilla/5.0"
}

def scrape_competitions(season):
    all_items = []
    page = 0
    while True:
        payload = {
            "name": "",
            "status": "passed",
            "gender": ["f", "m"],
            "weapon": ["e", "f", "s"],
            "type": ["i"],
            "season": season,
            "level": "s",
            "competitionCategory": "",
            "fromDate": "",
            "toDate": "",
            "fetchPage": page
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        if "items" not in data or not data["items"]:
            break

        all_items.extend(data["items"])
        page += 1

    return all_items

def save_to_csv(competitions, filename):
    df = pd.DataFrame(competitions)
    df["Competition Name"] = df["name"] + " (" + df["location"] + ")"
    df["Start Date"] = df["startDate"]
    df["Weapon"] = df["weapon"].str.capitalize()
    df["Gender"] = df["gender"].str.capitalize()
    df["Season"] = df["season"]
    df["Competition ID"] = df["competitionId"]
    df["Location"] = df["location"]

    final_df = df[[
        "Competition ID", "Competition Name", "Location", "Start Date",
        "Weapon", "Gender", "Season"
    ]]
    final_df.to_csv(filename, index=False)
    print(f"✅ Saved {len(final_df)} competitions to {filename}")

if __name__ == "__main__":
    data = scrape_competitions(SEASON)
    save_to_csv(data, OUTPUT_FILE)
