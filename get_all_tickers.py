import pandas as pd
import requests
from io import BytesIO

# === 1. 上場銘柄一覧 Excel を JPX からダウンロード ===
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"ファイル取得失敗: {response.status_code}")

# === 2. Excel → DataFrame ===
df = pd.read_excel(BytesIO(response.content), skiprows=1)
df.columns = df.columns.str.strip()  # 全角スペースや改行を除去

# === 3. 必要な列が存在するか確認 ===
required_cols = ['コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
missing = [col for col in required_cols if col not in df.columns]
if missing:
    raise ValueError(f"以下の列が見つかりません: {missing}")

# === 4. 列整形とフィルタ ===
df = df[required_cols].dropna(subset=['コード'])  # コードが空の行を除外
df['コード'] = df['コード'].astype(str).str.zfill(4)  # 'コード'をゼロ詰め文字列に

# === 5. ソート・インデックスリセット ===
df = df.sort_values('コード').reset_index(drop=True)

# === 6. 表示確認 ===
print("取得件数:", len(df))
print(df.head())

# === 7. CSV保存 ===
output_file = "jpx_tickers_full.csv"
df.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"→ ファイル保存完了: {output_file}")
