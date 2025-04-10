import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path

# =============================================
# Config + Regex
# =============================================
RANKING_RE = re.compile(r"window\._tabRanking\s*=\s*(\[.*?\]);", re.DOTALL)
DATA_DIR = Path("data")

# =============================================
# Utilities
# =============================================
def extract_ranking_from_html(text):
    match = RANKING_RE.search(text)
    if not match:
        return []
    try:
        return eval(match.group(1))  # window._tabRanking ist JS-Array
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
        "hand": None
    }

# =============================================
# Step 1: Load data
# =============================================
print("📦 Lade Matches und Rankings...")
df_matches = pd.read_csv(DATA_DIR / "matches_2023_all.csv")
df_ranks = pd.read_csv(DATA_DIR / "year_end_rankings_2023_sample_fixed.csv")

# Fix dtype for merging
df_matches["Fencer1 Name"] = df_matches["Fencer1 Name"].astype(str)
df_matches["Fencer2 Name"] = df_matches["Fencer2 Name"].astype(str)
df_ranks["name"] = df_ranks["name"].astype(str)

# =============================================
# Step 2: Missing Names bestimmen
# =============================================
missing_f1 = df_matches[~df_matches["Fencer1 Name"].isin(df_ranks["name"])]
missing_f2 = df_matches[~df_matches["Fencer2 Name"].isin(df_ranks["name"])]

missing_names_f1 = missing_f1["Fencer1 Name"].dropna().unique()
missing_names_f2 = missing_f2["Fencer2 Name"].dropna().unique()

# =============================================
# Step 3: Mapping von Namen zu IDs bauen
# =============================================
print("🔗 Baue Name-zu-ID Mapping...")
name_to_id = {}
for _, row in df_matches.iterrows():
    if pd.notna(row.get("Fencer1 Name")) and pd.notna(row.get("Fencer1 Id")):
        name_to_id[str(row["Fencer1 Name"]).strip()] = row["Fencer1 Id"]
    if pd.notna(row.get("Fencer2 Name")) and pd.notna(row.get("Fencer2 Id")):
        name_to_id[str(row["Fencer2 Name"]).strip()] = row["Fencer2 Id"]

# =============================================
# Step 4: Neue Rankings scrapen
# =============================================
print("🌐 Scrape fehlende Rankings...")
results = []
for name in tqdm(set(missing_names_f1).union(set(missing_names_f2))):
    athlete_id = name_to_id.get(name)
    if not athlete_id:
        continue
    profile = scrape_athlete_profile(int(athlete_id))
    if profile:
        results.append(profile)

# =============================================
# Step 5: Neue Rankings speichern + kombinieren
# =============================================
if results:
    df_new = pd.DataFrame(results)
    df_combined = pd.concat([df_ranks, df_new], ignore_index=True).drop_duplicates(subset=["athleteId", "season"], keep="last")
    df_combined.to_csv(DATA_DIR / "year_end_rankings_2023_full.csv", index=False)
    print("✅ Kombiniertes Ranking gespeichert unter year_end_rankings_2023_full.csv")
else:
    df_combined = df_ranks.copy()
    print("⚠️ Keine neuen Rankings gefunden")

# =============================================
# Step 6: Matches anreichern
# =============================================
print("🧪 Enrich matches with Ranking...")
df_f1 = df_combined.rename(columns={
    "name": "Fencer1 Name",
    "rank": "Fencer1 Rank",
    "points": "Fencer1 Points",
    "hand": "Fencer1 Hand"
})[["Fencer1 Name", "Fencer1 Rank", "Fencer1 Points", "Fencer1 Hand"]]

df_f2 = df_combined.rename(columns={
    "name": "Fencer2 Name",
    "rank": "Fencer2 Rank",
    "points": "Fencer2 Points",
    "hand": "Fencer2 Hand"
})[["Fencer2 Name", "Fencer2 Rank", "Fencer2 Points", "Fencer2 Hand"]]

# Merge
merged = df_matches.merge(df_f1, on="Fencer1 Name", how="left")
merged = merged.merge(df_f2, on="Fencer2 Name", how="left")
merged.to_csv(DATA_DIR / "matches_2023_enriched.csv", index=False)

# =============================================
# Step 7: Neue Missing-Listen speichern
# =============================================
final_missing_f1 = merged[merged["Fencer1 Rank"].isna()]["Fencer1 Name"].value_counts()
final_missing_f2 = merged[merged["Fencer2 Rank"].isna()]["Fencer2 Name"].value_counts()

final_missing_f1.to_csv(DATA_DIR / "missing_fencer1_names.csv")
final_missing_f2.to_csv(DATA_DIR / "missing_fencer2_names.csv")

print("✅ Pipeline abgeschlossen!")