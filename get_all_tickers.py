# get_all_tickers.py
import pandas as pd

url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

# skiprows を適切に増やす（例：3〜5）
df = pd.read_excel(url, skiprows=3)  # ← この数は調整の余地あり
print(df.columns)

# 正しい列名が確認できたら、その列だけ抽出（例：'コード' ではなく '銘柄コード' かも）
# df = df[['銘柄コード']]  # 正しい名前に変更してね



df = pd.read_excel(url, skiprows=1, usecols=['コード'])
df = df.dropna(subset=['コード'])
df['Code'] = df['コード'].astype(int).astype(str).str.zfill(4)

# .Tを付けず、コードのみCSV保存
df[['Code']].to_csv('ticker_list.csv', index=False, encoding='utf-8-sig')
print(f"✅ ticker_list.csv を生成しました（銘柄数: {len(df)}）")
