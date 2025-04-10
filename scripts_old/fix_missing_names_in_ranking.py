import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# ================================
# Load incomplete ranking file
# ================================
ranking_path = "data/year_end_rankings_2023_sample.csv"
df_ranking = pd.read_csv(ranking_path)
print(f"📦 Ranking geladen: {len(df_ranking)} Einträge")

# Nur Fechter mit fehlendem Namen oder Händigkeit
df_missing = df_ranking[df_ranking["name"].isna() | df_ranking["hand"].isna()]
print(f"🔍 Fechter ohne Namen oder Hand: {len(df_missing)}")

# ================================
# Hilfsfunktion
# ================================
def fetch_name_and_hand(athlete_id):
    url = f"https://fie.org/athletes/{athlete_id}"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"⚠️ Fehler bei Anfrage: {url}")
            return None, None

        soup = BeautifulSoup(response.text, "html.parser")

        # Name
        name_tag = soup.find("h1", class_="PageTitle")
        name = name_tag.text.strip() if name_tag else None

        # Hand
        hand = None
        for div in soup.select("div.ProfileInfo-item"):
            label = div.find("span", class_="ProfileInfo-label")
            value = div.find("span", class_="ProfileInfo-value")
            if label and label.text.strip() == "Hand" and value:
                hand = value.text.strip()
                break

        return name, hand

    except Exception as e:
        print(f"⚠️ Exception bei {athlete_id}: {e}")
        return None, None


# ================================
# Aktualisieren
# ================================
updated_rows = []
for idx, row in tqdm(df_missing.iterrows(), total=len(df_missing)):
    athlete_id = row["athleteId"]
    print(f"🔗 Anfrage an https://fie.org/athletes/{athlete_id}")
    name, hand = fetch_name_and_hand(athlete_id)

    if name:
        df_ranking.at[idx, "name"] = name
    if hand:
        df_ranking.at[idx, "hand"] = hand

    updated_rows.append((athlete_id, name, hand))

# ================================
# Ausgabe
# ================================
df_ranking.to_csv("data/year_end_rankings_2023_sample_fixed.csv", index=False)
print("💾 Gespeichert unter data/year_end_rankings_2023_sample_fixed.csv")

print("✅ Aktualisierte Einträge:")
for aid, name, hand in updated_rows:
    print(f"{aid}: {name} – Hand: {hand}")
