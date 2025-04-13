import pandas as pd
import glob
import os

OUTPUT_ROOT = "outputs"
OUTPUT_FILE = "fencing_enriched_all_years.csv"

# 🔍 Rekursiv alle passenden CSV-Dateien suchen
csv_files = sorted(glob.glob(os.path.join(OUTPUT_ROOT, "**", "fencing_enriched_*.csv"), recursive=True))

print(f"📦 {len(csv_files)} Dateien gefunden zum Zusammenführen.")

# Zusammenfügen
dfs = []
for file in csv_files:
    print(f"🔗 Lade {file}")
    df = pd.read_csv(file)
    dfs.append(df)

if not dfs:
    print("❌ Keine CSV-Dateien gefunden. Bitte prüfe die Verzeichnisstruktur und Dateinamen.")
    exit(1)

# ✅ Mergen & Speichern
merged_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
merged_df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Alle Ergebnisse zusammengeführt und gespeichert in: {OUTPUT_FILE}")


