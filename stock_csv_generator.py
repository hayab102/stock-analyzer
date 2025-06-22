import pandas as pd

CSV_TICKER_LIST = "ticker_list.csv"

# CSV 読み込み
ticker_df = pd.read_csv(CSV_TICKER_LIST)

# 必須列確認
if 'Code' not in ticker_df.columns:
    raise ValueError("ticker_list.csv に 'Code' 列が必要です")

# サンプル表示（先頭5件）
print("✅ 読み込んだ銘柄数:", len(ticker_df))
print(ticker_df.head())

# ここに株価取得や個別銘柄の処理など追加可能
