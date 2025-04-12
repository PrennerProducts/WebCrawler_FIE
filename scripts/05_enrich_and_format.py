import pandas as pd
import os
import sys

# === Argumente ===
if len(sys.argv) != 2:
    print("Verwendung: python 05_enrich_and_format.py <season>")
    sys.exit(1)

SEASON_YEAR = sys.argv[1]
INPUT_MATCHES = f"outputs/{SEASON_YEAR}/fencing_match_analysis_{SEASON_YEAR}_master.csv"
INPUT_COMPS = f"outputs/{SEASON_YEAR}/competitions_{SEASON_YEAR}.csv"
OUTPUT_FILE = f"outputs/{SEASON_YEAR}/fencing_enriched_{SEASON_YEAR}.csv"

# === Prüfungen ===
if not os.path.exists(INPUT_MATCHES) or not os.path.exists(INPUT_COMPS):
    print(f"⚠️ Eingabedateien fehlen für {SEASON_YEAR}, überspringe.")
    sys.exit(0)

# === Daten laden ===
df_matches = pd.read_csv(INPUT_MATCHES)
df_comps = pd.read_csv(INPUT_COMPS)

# === Season-Mapping (z. B. 2023 → 2023/2024) ===
season_map = {str(y): f"{y}/{y+1}" for y in range(2004, 2025)}
df_comps["Season"] = df_comps["Season"].astype(str).map(season_map).fillna(df_comps["Season"])

# === Join mit Metadaten ===
df = df_matches.merge(
    df_comps[["Competition ID", "Gender", "Weapon", "Season", "Competition Name", "Start Date"]],
    on="Competition ID",
    how="left"
)

# === Spalten anpassen ===
df.rename(columns={
    "hand_a": "Athlete Hand",
    "rank_a": "Athlete Year End World Ranking",
    "hand_b": "Opponent Hand",
    "rank_b": "Opponent Year End World Ranking",
    "Competition Name": "Competition",
    "Start Date": "Date of Duel"
}, inplace=True)

# === Spaltenreihenfolge ===
columns = [
    "Gender", "Weapon", "Season", "Competition", "Date of Duel",
    "Athlete Name", "Athlete Year End World Ranking", "Athlete Hand",
    "Opponent Name", "Opponent Year End World Ranking", "Opponent Hand",
    "Result", "Hit", "Counter-Hit"
]

df_final = df[columns].drop_duplicates()
df_final.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Fertige Datei gespeichert unter: {OUTPUT_FILE}")
