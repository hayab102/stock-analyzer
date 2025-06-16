import pandas as pd
import requests
from io import BytesIO

# 1. Excelダウンロード
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"ファイル取得失敗: {response.status_code}")

# 2. Excel読み込み + 列名クリーニング
df = pd.read_excel(BytesIO(response.content), skiprows=1)
df.columns = df.columns.str.replace(r'\s+', '', regex=True).str.strip()

# 3. 全列名表示
print("列名一覧:\n", df.columns.tolist())

# 4. 欲しい列を「曖昧にマッチ」して拾う
def find_column(possible_names):
    for col in df.columns:
        for name in possible_names:
            if name in col:
                return col
    return None

code_col = find_column(['コード'])
name_col = find_column(['銘柄名'])
market_col = find_column(['市場'])
g33_col = find_column(['33業種区分'])
g17_col = find_column(['17業種区分'])
scale_col = find_column(['規模区分'])

# 5. マッチできなかった列があればエラー
mapping = {
    'コード': code_col,
    '銘柄名': name_col,
    '市場・商品区分': market_col,
    '33業種区分': g33_col,
    '17業種区分': g17_col,
    '規模区分': scale_col,
}
missing = [k for k, v in mapping.items() if v is None]
if missing:
    raise ValueError(f"列の自動判定に失敗しました: {missing}")

# 6. データフレーム整形
df_out = df[[code_col, name_col, market_col, g33_col, g17_col, scale_col]].copy()
df_out.columns = ['コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
df_out['コード'] = df_out['コード'].astype(str).str.zfill(4)
df_out = df_out.sort_values('コード').reset_index(drop=True)

# 7. 保存 & 表示
df_out.to_csv("jpx_tickers_full.csv", index=False, encoding="utf-8-sig")
print(f"✅ 保存完了。銘柄数: {len(df_out)} 件 → jpx_tickers_full.csv")
print(df_out.head())
