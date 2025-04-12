# import pandas as pd
# import re
# import time
# from bs4 import BeautifulSoup
# from tqdm import tqdm
# from io import StringIO

# import pandas as pd

# df = pd.read_csv("competitions_2023.csv")
# print(df.columns.tolist())
import re
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

def has_tableau(season, comp_id):
    url = f"https://fie.org/competitions/{season}/{comp_id}?tab=results"
    try:
        response = requests.get(url, timeout=5)
        match = re.search(r'<script id="js-competition">(.+?)</script>', response.text, re.DOTALL)
        if not match:
            return False
        return "window._tableau" in match.group(1)
    except Exception as e:
        print(f"[{season} | {comp_id}] Fehler: {e}")
        return False

def check_season(season: int):
    path = f"outputs/{season}/competitions_{season}.csv"
    if not os.path.exists(path):
        print(f"⚠️  Datei nicht gefunden: {path}")
        return

    df = pd.read_csv(path)
    has_data = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"{season}"):
        comp_id = row["Competition ID"]
        ok = has_tableau(season, comp_id)
        has_data.append((comp_id, ok))

    df_result = pd.DataFrame(has_data, columns=["Competition ID", "Has_Tree"])
    df_result.to_csv(f"outputs/{season}/tree_availability_{season}.csv", index=False)
    print(f"✅ {season}: Ergebnis gespeichert in outputs/{season}/tree_availability_{season}.csv\n")

if __name__ == "__main__":
    for year in range(2004, 2024):
        check_season(year)
