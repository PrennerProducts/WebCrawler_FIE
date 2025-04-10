import pandas as pd

# === Einlesen der Daten ===
df_matches = pd.read_csv("data/matches_2023_all.csv")
df_ranks = pd.read_csv("data/year_end_rankings_2023_sample.csv")

# === Datentypen sicherstellen ===
df_matches["Fencer1 Name"] = df_matches["Fencer1 Name"].fillna("").astype(str)
df_matches["Fencer2 Name"] = df_matches["Fencer2 Name"].fillna("").astype(str)
df_ranks["name"] = df_ranks["name"].fillna("").astype(str)

# === Rename für spätere Merges ===
df_ranks_f1 = df_ranks.rename(columns={
    "name": "Fencer1 Name",
    "rank": "Fencer1 Year End Rank",
    "points": "Fencer1 Points",
    "hand": "Fencer1 Hand"
})

df_ranks_f2 = df_ranks.rename(columns={
    "name": "Fencer2 Name",
    "rank": "Fencer2 Year End Rank",
    "points": "Fencer2 Points",
    "hand": "Fencer2 Hand"
})

# === Merge ===
df_enriched = df_matches.merge(df_ranks_f1, on="Fencer1 Name", how="left")
df_enriched = df_enriched.merge(df_ranks_f2, on="Fencer2 Name", how="left")

# === Speichern ===
df_enriched.to_csv("data/matches_2023_enriched.csv", index=False)
print("✅ Daten gespeichert unter data/matches_2023_enriched.csv")

# === Debugging: Welche Fechter fehlen? ===
missing_f1 = df_enriched[df_enriched["Fencer1 Year End Rank"].isnull()]["Fencer1 Name"].unique()
missing_f2 = df_enriched[df_enriched["Fencer2 Year End Rank"].isnull()]["Fencer2 Name"].unique()

print("\n❗️Fechter ohne Fencer1-Ranking:", missing_f1)
print("❗️Fechter ohne Fencer2-Ranking:", missing_f2)