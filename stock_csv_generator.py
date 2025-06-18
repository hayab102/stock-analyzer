import pandas as pd
import yfinance as yf
import os

# 入力ファイル名
CSV_TICKER_LIST = "ticker_list.csv"
OUTPUT_DIR = "stock_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# CSV 読み込み
try:
    ticker_df = pd.read_csv(CSV_TICKER_LIST)
except FileNotFoundError:
    raise FileNotFoundError(f"{CSV_TICKER_LIST} が見つかりません。先に get_all_tickers.py を実行してください")

# Code列が存在するか確認
if "Code" not in ticker_df.columns:
    raise ValueError("ticker_list.csv に 'Code' 列が必要です")

# 銘柄コードのリスト
codes = ticker_df["Code"].astype(str).tolist()

# 株価を取得する関数（yfinance使用）
def fetch_price(code):
    ticker = f"{code}.T"  # 日本株コード
    try:
        data = yf.download(ticker, period="5d", interval="1d", progress=False)
        if not data.empty:
            data["Code"] = code
            return data
    except Exception as e:
        print(f"⚠️ {code} 取得失敗: {e}")
    return None

# 各銘柄について株価取得
all_data = []
for code in codes:
    print(f"🔄 {code} 取得中...")
    result = fetch_price(code)
    if result is not None:
        all_data.append(result)

# まとめて保存
if all_data:
    combined_df = pd.concat(all_data)
    combined_df.to_csv(os.path.join(OUTPUT_DIR, "all_stocks.csv"), encoding="utf-8-sig")
    print(f"✅ 完了: {len(all_data)} 銘柄の株価を保存しました → {OUTPUT_DIR}/all_stocks.csv")
else:
    print("❌ 株価データの取得に失敗しました")
