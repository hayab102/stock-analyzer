import pandas as pd
import requests
from io import BytesIO

# ① データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# ② Excelを全行読み取り
df_raw = pd.read_excel(BytesIO(res.content), header=None)

# ③ 正しいヘッダー行（"コード" を含む行）を探す
header_row_index = None
for i, row in df_raw.iterrows():
    row_clean = row.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))
    if "コード" in row_clean.values:
        header_row_index = i
        break

if header_row_index is None:
    raise ValueError("ヘッダー行が見つかりません（'コード'列を含む行）")

# ④ 正しいヘッダーで読み直し
df = pd.read_excel(BytesIO(res.content), header=header_row_index)

# ⑤ 列名正規化
df.columns = df.columns.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))

# ⑥ 必要な列
expected_columns = ['コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
missing = [col for col in expected_columns if col not in df.columns]
if missing:
    print("列名一覧:", df.columns.tolist())
    raise ValueError(f"以下の列が見つかりません: {missing}")

# ⑦ 抽出・整形・保存
df_out = df[expected_columns].copy()
df_out['コード'] = df_out['コード'].astype(str).str.zfill(4)
df_out = df_out.sort_values('コード').reset_index(drop=True)
df_out.to_csv("ticker_list.csv", index=False, encoding="utf-8-sig")

print(f"✅ 完了: {len(df_out)} 件 → ticker_list.csv を生成")
