# get_all_tickers.py
import pandas as pd

# JPX公式「東証上場銘柄一覧」ExcelファイルURL :contentReference[oaicite:0]{index=0}
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

df = pd.read_excel(url, skiprows=1, usecols=['コード'])
df = df.dropna(subset=['コード'])
df['Code'] = df['コード'].astype(int).astype(str).str.zfill(4)

# .Tを付けず、コードのみCSV保存
df[['Code']].to_csv('ticker_list.csv', index=False, encoding='utf-8-sig')
print(f"✅ ticker_list.csv を生成しました（銘柄数: {len(df)}）")
