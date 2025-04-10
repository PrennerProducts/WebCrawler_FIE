import pandas as pd
import re
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
from io import StringIO

import pandas as pd

df = pd.read_csv("competitions_2023.csv")
print(df.columns.tolist())
