import os
import polars as pl
import pytest

PATH_DAILY_DATA = r"/mnt/raid0/nfs_from6_readonly/data_frames"
PATH_FAC_ROOT = r"/mnt/raid0/user_data/daily_factor/barra/"


def test_example():
    if os.getenv("BARRA_RUN_INTEGRATION_TESTS", "0") != "1":
        pytest.skip("integration test (requires local data mounts)")
    idx_close = pl.scan_ipc(
        os.path.join(PATH_DAILY_DATA, "idx_close.feather"), memory_map=True
    )
    # columns = df.collect_schema().names()
    idx_close.head().collect().glimpse()

    stk_close = pl.scan_ipc(
        os.path.join(PATH_DAILY_DATA, "stk_close.feather"), memory_map=True
    ).select(
        pl.col("__index_level_0__").str.to_date("%Y%m%d").alias("date"),
        pl.all().exclude("__index_level_0__"),
    )

    # 计算每列的 null 数量和总行数
    # 注意：这会触发一次数据扫描，对于大文件可能有一定开销
    stats = stk_close.select(
        pl.all().null_count(), pl.len().alias("total_rows")
    ).collect()

    total_rows = stats["total_rows"][0]

    # 筛选出不全为 null 的列
    valid_cols = [
        col
        for col in stats.columns
        if col != "total_rows" and stats[col][0] < total_rows
    ]

    # 只选择有效列
    stk_close = stk_close.select(valid_cols)

    stk_close.head().collect().glimpse()


if __name__ == "__main__":
    test_example()
