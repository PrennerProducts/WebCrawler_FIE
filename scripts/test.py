import pandas as pd
import requests
import re
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
from io import StringIO

test = scrape_athlete_info_api(13715, season="2024", category="s")
print(test)