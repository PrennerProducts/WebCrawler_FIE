import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import json
from tqdm import tqdm

# ⚙️ Beispielhafte Liste – bitte durch vollständige ID-Liste ersetzen!
athlete_ids = [30878, 34866]  # <- Hier kannst du später alle relevanten IDs einsetzen

ranking_entries = []

for athlete_id in tqdm(athlete_ids):
    url = f"https://fie.org/athletes/{athlete_id}"
    print(f"🌐 Lade Profil: {url}")
    response = requests.get(url)
    html = response.text

    soup = BeautifulSoup(html, "html.parser")

    # ✋ Händigkeit extrahieren
    hand = None
    for div in soup.select("div.ProfileInfo-item"):
        label = div.find("span", class_="ProfileInfo-label")
        if label and label.text.strip() == "Hand":
            hand = div.find_all("span")[-1].text.strip()
            print(f"✋ Händigkeit von {athlete_id}: {hand}")

    # 🔍 JavaScript-Block mit Rankings suchen
    js_block = None
    for script in soup.find_all("script"):
        if "window._tabRanking" in script.text:
            js_block = script.text
            break

    if not js_block:
        print(f"❌ Kein JS-Block mit Rankings bei {athlete_id}")
        continue

    print(f"✅ JS-Block mit Ranking gefunden für {athlete_id}")

    # 🔎 window._tabRanking parsen
    match = re.search(r"window\._tabRanking\s*=\s*(\[\{.*?\}\]);", js_block, re.DOTALL)
    if not match:
        print(f"⚠️ Kein Ranking-JSON gefunden bei {athlete_id}")
        continue

    json_raw = match.group(1)
    json_raw = re.sub(r"(\w+):", r'"\1":', json_raw)  # Keys in Anführungszeichen
    json_raw = json_raw.replace("'", '"')

    try:
        ranking_data = json.loads(json_raw)
    except json.JSONDecodeError:
        print(f"❌ JSON-Parsing-Fehler bei {athlete_id}")
        continue

    print(f"🔍 JSON-Block (gekürzt):")
    print(json.dumps(ranking_data[:5], indent=2))  # Vorschau

    found = 0
    for entry in ranking_data:
        # ✅ Jetzt auch Saisons wie "2022/2023" oder "2023/2024"
        if "2023" in entry.get("season", "") and entry.get("category") == "S":
            ranking_entries.append({
                "athleteId": athlete_id,
                "name": None,
                "weapon": entry.get("weapon"),
                "season": entry.get("season"),
                "rank": entry.get("rank"),
                "points": entry.get("point"),
                "hand": hand
            })
            found += 1


    if found:
        print(f"✅ {found} Ranking-Einträge zu {athlete_id} gefunden!")
        print(f"📝 {ranking_entries[-1]}")
    else:
        print(f"⚠️ Keine gültigen Rankings zu {athlete_id} gespeichert.")

# 💾 Speichern
df = pd.DataFrame(ranking_entries)
print("\n📦 DataFrame-Vorschau:")
print(df.head())

df.to_csv("data/year_end_rankings_2023_sample.csv", index=False)
print("✅ Fertig! Gespeichert in data/year_end_rankings_2023_sample.csv")
