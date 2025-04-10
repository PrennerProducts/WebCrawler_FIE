import pandas as pd
from pathlib import Path

# =============================================
# Konfiguration
# =============================================
DATA_DIR = Path("data")
OUTPUT_CSV = "fencing_match_analysis_2023_neu.csv"

# =============================================
# Lade die angereicherten Matchdaten
# =============================================
df = pd.read_csv(DATA_DIR / "matches_2023_enriched.csv")

# =============================================
# Wandle Datum falls vorhanden um (optional)
# =============================================
if "Date of Duel" in df.columns:
    df["Date of Duel"] = pd.to_datetime(df["Date of Duel"], errors="coerce")
else:
    df["Date of Duel"] = pd.NaT  # Placeholder falls es fehlt

# =============================================
# Wähle und benenne relevante Spalten um
# =============================================
df_final = pd.DataFrame({
    "Gender": df["Gender"],
    "Weapon": df["Weapon"],
    "Season": df["Season"],
    "Competition": df["Competition"],
    "Date of Duel": df["Date of Duel"],
    "Athlete Name": df["Fencer1 Name"],
    "Athlete Year End World Ranking": df["Fencer1 Rank"],
    "Athlete Hand": df["Fencer1 Hand"],
    "Opponent Name": df["Fencer2 Name"],
    "Opponent Year End World Ranking": df["Fencer2 Rank"],
    "Opponent Hand": df["Fencer2 Hand"],
    "Result": df.apply(lambda row: "Won" if row["Fencer1 Winner"] else "Loss", axis=1),
    "Hit": df["Fencer1 Score"],
    "Counter-Hit": df["Fencer2 Score"]
})

# =============================================
# Ergänze fehlende Händigkeit über athleteId
# =============================================
df_ranks = pd.read_csv("data/year_end_rankings_2023_sample_fixed.csv")
id_to_hand = dict(zip(df_ranks["athleteId"], df_ranks["hand"]))

# athleteId aus matches_2023_all.csv holen
df_ranks = pd.read_csv("data/year_end_rankings_2023_sample_fixed.csv")
name_to_hand = dict(zip(df_ranks["name"], df_ranks["hand"]))
name_to_rank = dict(zip(df_ranks["name"], df_ranks["rank"]))


# Mapping Hand über athleteId
df_final["Athlete Hand"] = df_final["Athlete Name"].map(name_to_hand)
df_final["Athlete Year End World Ranking"] = df_final["Athlete Name"].map(name_to_rank)
df_final["Opponent Hand"] = df_final["Opponent Name"].map(name_to_hand)
df_final["Opponent Year End World Ranking"] = df_final["Opponent Name"].map(name_to_rank)


# =============================================
# Speichere finale CSV
# =============================================
df_final.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Export abgeschlossen: {OUTPUT_CSV}")
