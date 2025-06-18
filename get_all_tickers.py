import pandas as pd
import requests
from io import BytesIO

# データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# 試しに最初の数行だけ読み込んで確認
df_preview = pd.read_excel(BytesIO(res.content), header=None, nrows=5)
print("🔍 先頭5行の中身:\n", df_preview)
