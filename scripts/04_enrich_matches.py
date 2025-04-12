import sys
import os
import pandas as pd

# === Argumente & Pfade ===
if len(sys.argv) != 2:
    print("Verwendung: python 04_enrich_matches.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]
MATCH_FILE = f"outputs/{SEASON}/match_data_{SEASON}_raw.csv"
ATHLETE_FILE = f"outputs/{SEASON}/athlete_info_{SEASON}_scraped.csv"
OUTPUT_FILE = f"outputs/{SEASON}/fencing_match_analysis_{SEASON}_master.csv"

# === Prüfungen ===
if not os.path.exists(MATCH_FILE) or os.path.getsize(MATCH_FILE) == 0:
    print(f"⚠️ Keine Matchdaten vorhanden für {SEASON}, breche ab.")
    sys.exit(0)

if not os.path.exists(ATHLETE_FILE) or os.path.getsize(ATHLETE_FILE) == 0:
    print(f"⚠️ Keine Athleteninfos vorhanden für {SEASON}, breche ab.")
    sys.exit(0)

# === Laden der Daten ===
matches_df = pd.read_csv(MATCH_FILE)
athletes_df = pd.read_csv(ATHLETE_FILE)

if matches_df.empty:
    print(f"ℹ️ Matchdatei ist leer für {SEASON}, breche ab.")
    sys.exit(0)

# === Umbenennen zur Unterscheidung ===
athletes_a = athletes_df.rename(columns={"athleteId": "Athlete ID", "hand": "hand_a", "rank": "rank_a"})
athletes_b = athletes_df.rename(columns={"athleteId": "Opponent ID", "hand": "hand_b", "rank": "rank_b"})

# === Mergen ===
enriched = matches_df.merge(athletes_a, on="Athlete ID", how="left")
enriched = enriched.merge(athletes_b, on="Opponent ID", how="left")

# === Speichern ===
enriched.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Matchdaten erfolgreich angereichert für {SEASON}: {OUTPUT_FILE}")
