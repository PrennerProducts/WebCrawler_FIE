# FIE Webcrawler Pipeline

Dieses Projekt extrahiert und verarbeitet strukturierte Matchdaten von der offiziellen Website der FIE (https://fie.org). Ziel ist es, eine strukturierte CSV-Datei für alle Einzelwettbewerbe (Senior, Damen & Herren, Foil/Épée/Sabre) von 2004/2005 bis 2023/2024 zu erzeugen.

## 📂 Projektstruktur

```
Webcrawler_TIM/
├── outputs/                  # Saisonweise Ergebnisse (raw & enriched)
├── logs/                     # Logs pro Saison
├── scripts/                  # Pipeline-Skripte 01 bis 07
├── data/                     # Externe Referenzen oder Altdaten
├── athlete_info_repaired_global.csv  # Manuelle oder gescrapete Ergänzungen
├── fencing_enriched_all_years.csv    # Finale Gesamtdatei (aus Schritt 07)
└── README.md
```

## 🔄 Pipeline-Skripte

### `01_fetch_competitions.py <season>`

- Ruft die API `https://fie.org/competitions/search` auf
- Filtert: Senioren, Einzel, Foil/Épée/Sabre, passed
- Speichert: `outputs/<season>/competitions_<season>.csv`

### `02_parse_matches.py <season>`

- Besucht jede Competition-Ergebnisseite und extrahiert das `window._tableau`-JSON
- Falls nicht vorhanden: automatischer PDF-Fallback
- Speichert: `outputs/<season>/match_data_<season>_raw.csv`

### `03_parse_athletes.py <season>`

- Scraped alle Athleten-ID-Seiten `https://fie.org/athletes/<id>`
- Extrahiert: Händigkeit (L/R), Year-End-Rank für die Season
- Mehrere Prozesse laufen parallel
- Speichert: `outputs/<season>/athlete_info_<season>_scraped.csv`

### `04_enrich_matches.py <season>`

- Merged Matchdaten mit Athleteninfos (`hand`, `rank`)
- Wenn Infos fehlen: Live-Scraping zur Vervollständigung
- Speichert: `outputs/<season>/fencing_match_analysis_<season>_master.csv`

### `05_enrich_and_format.py <season>`

- Joint mit `competitions_<season>.csv` (Gender, Weapon, Ort, Datum)
- Vereinheitlicht die CSV-Spaltenreihenfolge:
  ```csv
  Gender, Weapon, Season, Competition, Date of Duel,
  Athlete Name, Athlete Year End World Ranking, Athlete Hand,
  Opponent Name, Opponent Year End World Ranking, Opponent Hand,
  Result, Hit, Counter-Hit
  ```
- Output: `outputs/<season>/fencing_enriched_<season>.csv`

### `06_pipeline_parallel.py`

- Ruft Skript 01 bis 05 für alle Jahre (2004–2023) nacheinander auf
- Optional: einzelne Saisons möglich
- Logs unter `logs/<season>.log`

### `07_merge_enriched_results.py`

- Sucht alle `fencing_enriched_*.csv` Dateien
- Führt sie in einer Datei zusammen: `fencing_enriched_all_years.csv`
- Entfernt Duplikate

### `test_tableau_availability.py`

- Checkt, ob pro Competition ein Tableau/Matchbaum vorhanden ist
- Gibt aus: `tree_availability_<season>.csv`

## 📅 Reihenfolge zur Ausführung

```bash
# Für eine einzelne Saison
python scripts/01_fetch_competitions.py 2023
python scripts/02_parse_matches.py 2023
python scripts/03_parse_athletes.py 2023
python scripts/04_enrich_matches.py 2023
python scripts/05_enrich_and_format.py 2023

# Für alle Jahre (2004–2023)
python scripts/06_pipeline_parallel.py

# Danach zusammenführen
python scripts/07_merge_enriched_results.py
```

## 💡 Hinweise

- Die Händigkeit und das Ranking werden bereits in Schritt 03 erfasst.
- Sollten im Schritt 04 Daten fehlen, werden sie „live“ automatisch nachgescraped.
- Die FIE-Webseite ist inkonsistent bei alten Saisons (< 2014), daher wird PDF-Fallback verwendet.

## 🚀 Ziel

Ein konsistenter, vollständiger CSV-Datensatz aller FIE-Duelle für quantitative Analysen:

- Matchverlauf
- Performanceanalyse
- Linkshänderforschung
- Saisonstrends

---

Maintainer: **Mutex (2025)**
