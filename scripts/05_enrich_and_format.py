import pandas as pd

# === Input-Dateien ===
MATCH_FILE = "fencing_match_analysis_2023_master.csv"
COMPETITION_FILE = "competitions_2023.csv"
OUTPUT_FILE = "fencing_enriched_2023.csv"

# === Daten laden ===
df_matches = pd.read_csv(MATCH_FILE)
df_comps = pd.read_csv(COMPETITION_FILE)

# Season-Mapping einfügen von zb 2023 auf 2023/2024
season_map = {str(y): f"{y}/{y+1}" for y in range(2004, 2025)}
df_comps["Season"] = df_comps["Season"].astype(str).map(season_map).fillna(df_comps["Season"])

# === Competition-Metadaten joinen ===
df = df_matches.merge(
    df_comps[["Competition ID", "Gender", "Weapon", "Season", "Competition Name", "Start Date"]],
    on="Competition ID",
    how="left"
)

# === Spalten umbenennen und anpassen ===
df.rename(columns={
    "Athlete Name": "Athlete Name",
    "Opponent Name": "Opponent Name",
    "hand_a": "Athlete Hand",
    "rank_a": "Athlete Year End World Ranking",
    "hand_b": "Opponent Hand",
    "rank_b": "Opponent Year End World Ranking",
    "Result": "Result",
    "Hit": "Hit",
    "Counter-Hit": "Counter-Hit",
    "Competition Name": "Competition",         
    "Start Date": "Date of Duel"              
}, inplace=True)

# === Reihenfolge der Spalten festlegen ===
column_order = [
    "Gender", "Weapon", "Season", "Competition", "Date of Duel",
    "Athlete Name", "Athlete Year End World Ranking", "Athlete Hand",
    "Opponent Name", "Opponent Year End World Ranking", "Opponent Hand",
    "Result", "Hit", "Counter-Hit"
]


df_final = df[column_order].drop_duplicates()


# === Speichern ===
df_final.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Fertige Datei gespeichert unter: {OUTPUT_FILE}")
