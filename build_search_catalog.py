import pandas as pd
import os

SOURCE_CSV = "jpx_tickers_full.csv"
OUTPUT_CSV = "search_catalog.csv"

if not os.path.exists(SOURCE_CSV):
    raise FileNotFoundError(
        f"{SOURCE_CSV} が見つかりません。先に get_all_tickers.py を実行してください。"
    )

df = pd.read_csv(SOURCE_CSV)

required_cols = ["コード", "銘柄名"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"{SOURCE_CSV} に必要列がありません: {missing}")

out = df[["コード", "銘柄名"]].copy()

out["コード"] = (
    out["コード"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .str.zfill(4)
)

out["銘柄名"] = out["銘柄名"].astype(str).str.strip()

out = out.rename(columns={
    "コード": "code",
    "銘柄名": "name",
})

out = out.dropna(subset=["code", "name"])
out = out[(out["code"] != "") & (out["name"] != "")]
out = out.drop_duplicates(subset=["code"]).sort_values("code").reset_index(drop=True)

out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print(f"✅ 完了: {len(out)} 件 → {OUTPUT_CSV}")
print(out.head(10).to_string(index=False))
