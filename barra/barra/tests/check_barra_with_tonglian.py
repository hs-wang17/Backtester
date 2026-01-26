import polars as pl
from pathlib import Path
import numpy as np
from factor_name_mapping import FACTOR_NAME_MAPPING
from plot_barra_correlation import plot_correlation_trends


def main():
    # 设置缓存文件路径
    cache_file = Path("cache_dy1d_exposure_cne6_zx20.parquet")

    # 从119.253.67.3:3306 读取数据(带缓存)
    if cache_file.exists():
        print(f"Loading cached database data from {cache_file}...")
        df_db = pl.read_parquet(cache_file)
        print(f"Cached data loaded. Shape: {df_db.shape}")
    else:
        print("Reading database dy1d_exposure_cne6_zx20...")
        # 使用adbc引擎或者直接用pymysql
        try:
            # 尝试使用connectorx
            df_db = pl.read_database_uri(
                query="select * from dy1d_exposure_cne6_zx20",
                uri="mysql://readonly_user:readonly_user@119.253.67.3:3306/mydb",
                engine="connectorx",
            )
        except Exception as e:
            print(f"ConnectorX failed: {e}")
            print("Trying with pymysql...")
            # 使用pymysql作为备选
            import pymysql

            conn = pymysql.connect(
                host="119.253.67.3",
                port=3306,
                user="readonly_user",
                password="readonly_user",
                database="mydb",
            )
            df_db = pl.read_database(
                "select * from dy1d_exposure_cne6_zx20",
                conn,
            )
            conn.close()

        print(f"Database data shape: {df_db.shape}")

        # 保存到缓存
        print(f"Saving to cache: {cache_file}...")
        df_db.write_parquet(cache_file, compression="zstd")
        print("Cache saved successfully!")

    print(f"Database columns: {df_db.columns}")

    # 读取本地计算的barra因子
    barra_dir = Path("/mnt/raid0/nfs_readonly/DailyFactors/barra/barra/")
    barra_files = sorted(barra_dir.glob("*.feather"))
    print(f"\nFound {len(barra_files)} local Barra factor files")

    # 获取数据库中的日期列表（大写列名）
    if "TRADE_DATE" in df_db.columns:
        date_col = "TRADE_DATE"
    elif "trade_date" in df_db.columns:
        date_col = "trade_date"
    elif "date" in df_db.columns:
        date_col = "date"
    elif "DATE" in df_db.columns:
        date_col = "DATE"
    else:
        print("Available columns in database:", df_db.columns)
        raise ValueError("Cannot find date column in database")

    # 获取数据库中的股票代码列
    if "TICKER_SYMBOL" in df_db.columns:
        code_col = "TICKER_SYMBOL"
    elif "sec_id" in df_db.columns:
        code_col = "sec_id"
    elif "stock_code" in df_db.columns:
        code_col = "stock_code"
    elif "STOCK_CODE" in df_db.columns:
        code_col = "STOCK_CODE"
    else:
        print("Available columns in database:", df_db.columns)
        raise ValueError("Cannot find stock code column in database")

    # 读取一个本地文件查看列名
    sample_local = pl.read_ipc(str(barra_files[0]))
    print(f"\nLocal Barra columns: {sample_local.columns}")

    # 找出共同的style因子列（排除sec_id等标识列）
    df_db = df_db.rename(FACTOR_NAME_MAPPING)
    db_style_factors = [
        col for col in df_db.columns if col not in [date_col, code_col, "id"]
    ]
    local_style_factors = [col for col in sample_local.columns if col != "sec_id"]

    # 找出共同的因子
    common_factors = list(set(db_style_factors) & set(local_style_factors))
    print(f"\nCommon style factors ({len(common_factors)}): {common_factors}")

    if not common_factors:
        print("\nWarning: No common factors found!")
        print(f"DB factors: {db_style_factors[:10]}...")
        print(f"Local factors: {local_style_factors}")
        return

    # 使用LazyFrame和streaming模式处理大数据
    print("\nReading all local Barra factor files with streaming...")

    # 将数据库数据转换为LazyFrame
    lf_db = df_db.lazy()

    # 读取所有本地barra因子并拼接成LazyFrame
    lazy_dfs = []
    for barra_file in barra_files:
        date_str = barra_file.stem  # yyyymmdd
        lf_local = pl.scan_ipc(str(barra_file))
        lf_local = lf_local.with_columns(pl.lit(date_str).alias("date"))
        lazy_dfs.append(lf_local)

    lf_local_all = pl.concat(lazy_dfs)

    # 使用streaming模式合并数据
    print("\nMerging database and local data with streaming...")
    lf_merged = lf_db.join(
        lf_local_all,
        left_on=[date_col, code_col],
        right_on=["date", "sec_id"],
        how="inner",
        suffix="_local",
    )

    # 按日期分组计算相关系数 - 分批处理避免内存溢出
    print("\nCalculating correlations by date (streaming mode)...")

    # 获取所有唯一日期
    unique_dates = df_db.select(pl.col(date_col).unique()).to_series().sort()
    print(f"Processing {len(unique_dates)} unique dates...")

    results = []
    batch_size = 50  # 每批处理50个日期

    for i in range(0, len(unique_dates), batch_size):
        batch_dates = unique_dates[i : i + batch_size].to_list()

        # 使用streaming模式筛选当前批次的日期
        lf_batch = lf_merged.filter(pl.col(date_col).is_in(batch_dates))

        # collect当前批次到内存
        df_batch = lf_batch.collect(engine="streaming")

        # 对当前批次按日期分组计算相关系数
        for date in batch_dates:
            df_date = df_batch.filter(pl.col(date_col) == date)

            if df_date.height == 0:
                continue

            result = {"date": date, "n_stocks": df_date.height}

            for factor in common_factors:
                factor_local = (
                    f"{factor}_local"
                    if f"{factor}_local" in df_date.columns
                    else factor
                )

                # 获取两列数据并去除NaN
                db_values = df_date[factor].to_numpy()
                local_values = df_date[factor_local].to_numpy()

                mask = ~(np.isnan(db_values) | np.isnan(local_values))
                if mask.sum() > 1:  # 至少需要2个点才能计算相关系数
                    corr = np.corrcoef(db_values[mask], local_values[mask])[0, 1]
                    result[factor] = corr
                else:
                    result[factor] = np.nan

            results.append(result)

        print(
            f"Processed {min(i + batch_size, len(unique_dates))}/{len(unique_dates)} dates..."
        )

    # 转换为DataFrame并按日期排序
    df_results = pl.DataFrame(results).sort("date")

    # 保存结果
    output_file = "barra_correlation_check.csv"
    df_results.write_csv(output_file)
    print(f"\nResults saved to {output_file}")

    # 打印统计信息
    print("\n=== Correlation Statistics ===")
    for factor in sorted(common_factors):
        if factor in df_results.columns:
            corrs = df_results[factor].drop_nulls()
            if len(corrs) > 0:
                print(f"\n{factor}:")
                print(f"  Mean: {corrs.mean():.4f}")
                print(f"  Median: {corrs.median():.4f}")
                print(f"  Min: {corrs.min():.4f}")
                print(f"  Max: {corrs.max():.4f}")
                print(f"  Std: {corrs.std():.4f}")

    # 生成可视化图表
    print("\n=== Generating Visualization ===")
    plot_correlation_trends(df_results, common_factors, output_dir=".")


if __name__ == "__main__":
    main()
