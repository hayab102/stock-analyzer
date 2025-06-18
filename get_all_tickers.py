import pandas as pd
import requests
from io import BytesIO

# ① データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# ② 一時的に全シート読み込み
df_all = pd.read_excel(BytesIO(res.content), header=None)

# ③ ヘッダー行（'コード' を含む行）を自動検出
header_row_idx = df_all[df_all.apply(lambda row: row.astype(str).str.contains("コード").any(), axis=1)].index[0]

# ④ 正しくヘッダーを指定して再読込
df = pd.read_excel(BytesIO(res.content), header=header_row_idx)

# ⑤ 列名クリーンアップ
df.columns = df.columns.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))

# ⑥ 必要列が揃っているかチェック
expected_columns = ['日付', 'コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
missing = [col for col in expected_columns if col not in df.columns]
if missing:
    raise ValueError(f"列が見つかりません: {missing}")

# ⑦ 整形・出力
df_out = df[expected_columns].copy()
df_out['コード'] = df_out['コード'].astype(str).str.zfill(4)
df_out = df_out.sort_values('コード').reset_index(drop=True)

# ⑧ 保存
df_out.to_csv("ticker_list.csv", index=False, encoding="utf-8-sig")
print(f"✅ 完了: {len(df_out)} 件 → jpx_tickers_full.csv")
