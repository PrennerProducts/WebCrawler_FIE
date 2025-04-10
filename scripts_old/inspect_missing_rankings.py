import pandas as pd

# CSV-Dateien laden
df_matches = pd.read_csv("data/matches_2023_enriched.csv")
df_ranks = pd.read_csv("data/year_end_rankings_2023_sample_fixed.csv")

# Liste der Namen im Ranking (nur gültige Strings)
valid_rank_names = df_ranks["name"].dropna().astype(str).str.strip()

# Fehlende Fencer1 & Fencer2 analysieren
missing_f1 = df_matches[~df_matches["Fencer1 Name"].isin(valid_rank_names)]["Fencer1 Name"].value_counts()
missing_f2 = df_matches[~df_matches["Fencer2 Name"].isin(valid_rank_names)]["Fencer2 Name"].value_counts()

# Top 20 ausgeben
print("❌ Top fehlende Fencer1:")
print(missing_f1.head(20))
print("\n❌ Top fehlende Fencer2:")
print(missing_f2.head(20))

# Optional: als CSV speichern zur Analyse
missing_f1.to_csv("data/missing_fencer1_names.csv")
missing_f2.to_csv("data/missing_fencer2_names.csv")
print("\n📁 CSV-Dateien gespeichert: missing_fencer1_names.csv & missing_fencer2_names.csv")
