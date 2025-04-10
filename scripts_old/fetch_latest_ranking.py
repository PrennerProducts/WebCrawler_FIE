import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from tqdm import tqdm

RANKING_RE = re.compile(r"window\._tabRanking\s*=\s*(\[.*?\]);", re.DOTALL)


def extract_ranking_from_html(text):
    match = RANKING_RE.search(text)
    if not match:
        return []
    try:
        return eval(match.group(1))  # Achtung: eval ist hier okay, weil du eigene Daten scrapest
    except Exception:
        return []


def extract_name_from_html(text):
    soup = BeautifulSoup(text, "html.parser")
    title = soup.find("h1", class_="PageTitle")
    return title.text.strip() if title else None


def get_latest_2023_ranking(html):
    rankings = extract_ranking_from_html(html)
    for r in rankings:
        if r.get("season") in ["2023", "2022/2023", "2023/2024"] and r.get("category") == "S":
            return r
    return None


def scrape_athlete_profile(athlete_id):
    url = f"https://fie.org/athletes/{athlete_id}"
    response = requests.get(url)
    if response.status_code != 200:
        return None

    html = response.text
    name = extract_name_from_html(html)
    ranking = get_latest_2023_ranking(html)

    if not ranking:
        return None

    return {
        "athleteId": athlete_id,
        "name": name,
        "weapon": ranking.get("weapon"),
        "season": ranking.get("season"),
        "rank": ranking.get("rank"),
        "points": ranking.get("point"),
        "hand": None,  # Wird ggf. später ergänzt
    }


# ==============================
# MAIN
# ==============================

df_missing = pd.read_csv("data/missing_fencer1_names.csv")
all_names = df_missing["Fencer1 Name"].dropna().unique()

df_matches = pd.read_csv("data/matches_2023_all.csv")
df_ranks = pd.read_csv("data/year_end_rankings_2023_sample_fixed.csv")

# Mapping: name -> athleteId
name_to_id = dict(zip(df_ranks["name"], df_ranks["athleteId"]))

results = []

for name in tqdm(all_names):
    athlete_id = name_to_id.get(name)
    if not athlete_id:
        continue
    profile = scrape_athlete_profile(int(athlete_id))
    if profile:
        results.append(profile)

# Speichern, falls gefunden
if results:
    df_new = pd.DataFrame(results)
    df_new.to_csv("data/year_end_rankings_2023_recovered.csv", index=False)
    print("✅ Neue Rankings gespeichert unter data/year_end_rankings_2023_recovered.csv")
else:
    print("⚠️ Keine neuen Daten gefunden")
