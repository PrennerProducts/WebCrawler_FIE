import pandas as pd

MASTER_FILE = "fencing_enriched_all_years.csv"
FIX_FILE = "athlete_info_repaired_from_logs.csv"
OUTPUT_FILE = "fencing_enriched_all_years_hand_fixed.csv"

# CSV-Dateien laden
df_match = pd.read_csv(MASTER_FILE)
df_fix = pd.read_csv(FIX_FILE)

# Spaltennamen für Merge anpassen
df_fix = df_fix.rename(columns={"name": "Athlete Name", "hand": "Fixed Athlete Hand"})

# Mergen nach Athlete Name
df_merged = df_match.merge(df_fix[["Athlete Name", "Fixed Athlete Hand"]], on="Athlete Name", how="left")

# Händigkeit überschreiben wenn neu vorhanden
df_merged["Athlete Hand"] = df_merged["Fixed Athlete Hand"].combine_first(df_merged["Athlete Hand"])

# Neue Spalte entfernen
df_merged.drop(columns=["Fixed Athlete Hand"], inplace=True)

# Speichern
df_merged.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Datei gespeichert unter: {OUTPUT_FILE}")
