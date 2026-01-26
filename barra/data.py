import polars as pl
from pathlib import Path
import os
from tqdm import tqdm

# 使用adbc引擎或者直接用pymysql
try:
    # 尝试使用connectorx
    df_db = pl.read_database_uri(
        query="select * from dy1d_exposure_cne6_zx20", uri="mysql://readonly_user:readonly_user@119.253.67.3:3306/mydb", engine="connectorx"
    )
except Exception as e:
    print(f"ConnectorX failed: {e}")
    print("Trying with pymysql...")
    # 使用pymysql作为备选
    import pymysql

    conn = pymysql.connect(host="119.253.67.3", port=3306, user="readonly_user", password="readonly_user", database="mydb")
    df_db = pl.read_database("select * from dy1d_exposure_cne6_zx20", conn)
    conn.close()

out_dir = "/home/haris/project/backtester/barra/barra_from_tonglian"
os.makedirs(out_dir, exist_ok=True)

# 先拿到所有交易日
trade_dates = df_db.select("TRADE_DATE").unique().sort("TRADE_DATE").to_series().to_list()

for date in tqdm(trade_dates):
    if os.path.exists(os.path.join(out_dir, f"{date}.feather")):
        continue
    # 1. 按日期筛选
    df_pd = df_db.filter(pl.col("TRADE_DATE") == date).to_pandas()

    # 2. 完全复刻你现在的处理
    df_pd = df_pd.iloc[:, 1:-2]
    df_pd = df_pd.rename(columns={df_pd.columns[1]: "code"})

    # 3. 保存为 feather
    save_path = os.path.join(out_dir, f"{date}.feather")
    df_pd.to_feather(save_path)

    print(f"Saved {save_path}")
