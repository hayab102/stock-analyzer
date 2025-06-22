import pandas as pd
import requests
from io import BytesIO

# ① データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# ② Excel読み込み → headerは1行目、データは2行目以降
df = pd.read_excel(BytesIO(res.content), header=1)

# ③ 列名整形（不要な空白など削除）
df.columns = df.columns.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))
print("列名一覧:", df.columns.tolist())

# ④ 必要列があるか確認
expected_columns = ['日付', 'コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
missing = [col for col in expected_columns if col not in df.columns]
if missing:
    raise ValueError(f"以下の列が見つかりません: {missing}")

# ⑤ 必要な列を抽出
df_out = df[expected_columns].copy()
df_out['コード'] = df_out['コード'].astype(str).str.zfill(4)
df_out = df_out.sort_values('コード').reset_index(drop=True)

# ⑥ 列名を英語に変換
df_out = df_out.rename(columns={
    'コード': 'Code',
    '銘柄名': 'Name',
    '市場・商品区分': 'Market',
    '33業種区分': 'Sector33',
    '17業種区分': 'Sector17',
    '規模区分': 'Scale'
})

# ⑦ 保存
df_out.to_csv("ticker_list.csv", index=False, encoding="utf-8-sig")
print(f"✅ 完了: {len(df_out)} 件 → ticker_list.csv")
