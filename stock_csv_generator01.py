# stock_csv_generator.py (完全版：東証全銘柄対応)

import pandas as pd
import yfinance as yf
import os
import time

# --- 設定 ---
CSV_TICKER_LIST = "ticker_list.csv"   # 事前に get_all_tickers.py で作成したもの
OUTPUT_DIR = "data"
DAYS = 30   # 過去30日分を取得

# --- フォルダ準備 ---
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- テクニカル指標関数 ---
def add_indicators(df):
    df = df.copy()
    df["MA5"] = df["Close"].rolling(window=5).mean()
    df["MA25"] = df["Close"].rolling(window=25).mean()
    # RSIの計算
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

# --- 銘柄リストを読み込む ---
ticker_df = pd.read_csv(CSV_TICKER_LIST)
if "Code" not in ticker_df.columns:
    raise ValueError("ticker_list.csv に 'Code' 列が必要です")

codes = ticker_df["Code"].dropna().astype(str).tolist()

# --- 各銘柄の株価データを取得＆保存 ---
for code in codes:
    ticker = f"{code}.T"  # Yahoo Finance用に.Tを付ける
    try:
        print(f"[INFO] Downloading {ticker}...")
        df = yf.download(ticker, period=f"{DAYS}d", interval="1d")
        if df.empty:
            print(f"[WARNING] No data for {ticker}")
            continue
        df = add_indicators(df)
        df.to_csv(os.path.join(OUTPUT_DIR, f"{code}.csv"))
        time.sleep(1)  # サーバ負荷対策で1秒待つ
    except Exception as e:
        print(f"[ERROR] Failed for {ticker}: {e}")
