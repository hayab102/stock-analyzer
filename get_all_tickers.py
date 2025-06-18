import pandas as pd
import requests
from io import BytesIO

# ① データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# ② Excel 読み込み（header=1: 2行目を列名にする）
df = pd.read_excel(BytesIO(res.content), header=1)

# ③ 列名の空白を正規化
df.columns = df.columns.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))

# ④ 必要列の確認と英語名変換
required_map = {
    'コード': 'Code',
    '銘柄名': 'Name',
    '市場・商品区分': 'Market',
    '33業種区分': 'Sector33',
    '17業種区分': 'Sector17',
    '規模区分': 'Scale',
}

missing = [jp for jp in required_map if jp not in df.columns]
if missing:
    raise ValueError(f"以下の列が見つかりません: {missing}")

# ⑤ 必要な列だけ抽出・リネーム
df_out = df[list(required_map.keys())].rename(columns=required_map)
df_out['Code'] = df_out['Code'].astype(str).str.zfill(4)
df_out = df_out.sort_values('Code').reset_index(drop=True)

# ⑥ CSV に保存（次のスクリプトが使うファイル名）
df_out.to_csv("ticker_list.csv", index=False, encoding="utf-8-sig")
print(f"✅ 完了: {len(df_out)} 件 → ticker_list.csv を出力しました")
