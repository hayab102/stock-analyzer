import pandas as pd
import requests
from io import BytesIO

# ① データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# ② Excel 読み込み（header=1 → 2行目がヘッダー）
df = pd.read_excel(BytesIO(res.content), header=1)

# ③ 列名から全角/半角スペースを除去して正規化
df.columns = df.columns.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))

# ④ 必要な列（この名前でないと後工程が動かない）
expected_columns = ['コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']

# ⑤ 列が存在するかチェック
missing = [col for col in expected_columns if col not in df.columns]
if missing:
    print("列名一覧:", df.columns.tolist())
    raise ValueError(f"以下の列が見つかりません: {missing}")

# ⑥ 必要な列を抽出して整形
df_out = df[expected_columns].copy()
df_out['コード'] = df_out['コード'].astype(str).str.zfill(4)
df_out = df_out.sort_values('コード').reset_index(drop=True)

# ⑦ 保存
df_out.to_csv("ticker_list.csv", index=False, encoding="utf-8-sig")
print(f"✅ 完了: {len(df_out)} 件 → ticker_list.csv を生成")
