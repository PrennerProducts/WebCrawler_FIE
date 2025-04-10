import pandas as pd

# === Einlesen ===
matches_df = pd.read_csv("match_data_2023_raw.csv")
athletes_df = pd.read_csv("athlete_info_2023_scraped.csv")

# === Umbenennung zur Unterscheidung ===
athletes_a = athletes_df.rename(columns={"athleteId": "Athlete ID", "hand": "hand_a", "rank": "rank_a"})
athletes_b = athletes_df.rename(columns={"athleteId": "Opponent ID", "hand": "hand_b", "rank": "rank_b"})

# === Join (Left-Join mit beiden Seiten) ===
enriched = matches_df.merge(athletes_a, on="Athlete ID", how="left")
enriched = enriched.merge(athletes_b, on="Opponent ID", how="left")

# === Ausgabe in neue Datei ===
enriched.to_csv("fencing_match_analysis_2023_master.csv", index=False)

print("✅ Matchdaten erfolgreich angereichert und gespeichert als: fencing_match_analysis_2023_master.csv")
