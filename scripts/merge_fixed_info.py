import pandas as pd

# === Input-Dateien ===
MASTER_FILE = "athlete_info_combined.csv"
FIXED_FILE = "athlete_info_repaired_from_logs.csv"
OUTPUT_FILE = "athlete_info_combined_final.csv"

# === Laden ===
df_master = pd.read_csv(MASTER_FILE)
df_fix = pd.read_csv(FIXED_FILE)

# === Spalten ggf. umbenennen
df_fix.rename(columns={"hand": "Fixed Hand", "rank": "Fixed Rank"}, inplace=True)

# === Merge nach athleteId
df = df_master.merge(df_fix[["athleteId", "Fixed Hand", "Fixed Rank"]], on="athleteId", how="left")

# === Nur fehlende Daten überschreiben
df["hand"] = df["hand"].fillna(df["Fixed Hand"])
df["rank"] = df["rank"].fillna(df["Fixed Rank"])

# === Drop Hilfsspalten
df.drop(columns=["Fixed Hand", "Fixed Rank"], inplace=True)

# === Speichern
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Neue Datei gespeichert: {OUTPUT_FILE}")
