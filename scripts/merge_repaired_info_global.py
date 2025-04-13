import pandas as pd

MATCH_FILE = "fencing_enriched_all_years.csv"
FIXED_FILE = "athlete_info_repaired_global.csv"
OUTPUT_FILE = "fencing_enriched_all_years_fixed.csv"

# Hauptdaten laden
df_main = pd.read_csv(MATCH_FILE)

# Reparierte Daten laden
df_fix = pd.read_csv(FIXED_FILE)
df_fix = df_fix.rename(columns={
    "athleteId": "Athlete ID",
    "hand": "Fixed Athlete Hand",
    "rank": "Fixed Athlete Rank"
})

# Matchdaten mit reparierten Daten mergen – Athlet
df_main = df_main.merge(df_fix, on="Athlete ID", how="left")

# Falls Opponent ID in deinen Daten vorhanden ist, nochmal für Gegner mergen
df_fix_opp = df_fix.rename(columns={
    "Athlete ID": "Opponent ID",
    "Fixed Athlete Hand": "Fixed Opponent Hand",
    "Fixed Athlete Rank": "Fixed Opponent Rank"
})
df_main = df_main.merge(df_fix_opp, on="Opponent ID", how="left")

# Leere Hand/Rank-Felder ersetzen, wenn Fix vorhanden ist
df_main["Athlete Hand"] = df_main["Athlete Hand"].combine_first(df_main["Fixed Athlete Hand"])
df_main["Athlete Year End World Ranking"] = df_main["Athlete Year End World Ranking"].combine_first(df_main["Fixed Athlete Rank"])

df_main["Opponent Hand"] = df_main["Opponent Hand"].combine_first(df_main["Fixed Opponent Hand"])
df_main["Opponent Year End World Ranking"] = df_main["Opponent Year End World Ranking"].combine_first(df_main["Fixed Opponent Rank"])

# Unnötige Spalten löschen
df_main.drop(columns=[
    "Fixed Athlete Hand", "Fixed Athlete Rank",
    "Fixed Opponent Hand", "Fixed Opponent Rank"
], inplace=True)

# Speichern
df_main.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Reparierte Datei gespeichert unter: {OUTPUT_FILE}")
