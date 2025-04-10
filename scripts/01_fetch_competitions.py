import requests
import pandas as pd

url = "https://fie.org/competitions/search"
headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://fie.org",
    "Referer": "https://fie.org/competitions",
    "User-Agent": "Mozilla/5.0"
}

def scrape_competitions_2023():
    all_items = []
    page = 0
    while True:
        payload = {
            "name": "",
            "status": "passed",
            "gender": ["f", "m"],
            "weapon": ["e", "f", "s"],
            "type": ["i"],
            "season": "2023",
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
    print(f"Saved {len(final_df)} competitions to {filename}")

if __name__ == "__main__":
    data = scrape_competitions_2023()
    save_to_csv(data, "competitions_2023.csv")
