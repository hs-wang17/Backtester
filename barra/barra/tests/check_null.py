import polars as pl
import os

pl.Config.set_engine_affinity(engine="streaming")

DAILY_FAC_PATH = "/mnt/raid0/nfs_readonly/DailyFactors/barra"
df = (
    pl.concat(
        [
            pl.scan_ipc(os.path.join(DAILY_FAC_PATH, file), memory_map=True)
            # .rename({"__index_level_0__": "code"})
            .rename({"sec_id": "code"})
            .with_columns(pl.lit(file[:8]).alias("date"))
            for file in os.listdir(DAILY_FAC_PATH)
        ],
        how="vertical",
    )
    .group_by("date")
    .agg(
        [
            # pl.all().exclude("code", "date").mean().name.suffix("_mean"),
            # pl.all().exclude("code", "date").std().name.suffix("_std"),
            pl.all().exclude("code", "date").count().name.suffix("_count"),
            pl.all().exclude("code", "date").min().name.suffix("_min"),
            pl.all().exclude("code", "date").max().name.suffix("_max"),
        ]
    )
    .collect()
)

# 检查 _min 或 _max 列中是否存在 inf 或 -inf
# Polars 支持 is_infinite() 方法来检测
min_max_cols = [c for c in df.columns if c.endswith("_min") or c.endswith("_max")]

# 统计每列的无限值数量
inf_stats = df.select([pl.col(c).is_infinite().sum() for c in min_max_cols])
print(inf_stats)

# 筛选出计数大于0的列
cols_with_inf = [c for c in inf_stats.columns if inf_stats[c][0] > 0]
cols_with_inf.sort()

if cols_with_inf:
    print(f"⚠️ 发现无限值 (inf/-inf) 的列: {cols_with_inf}")

    # 展示包含无限值的行
    inf_rows = df.filter(
        pl.any_horizontal([pl.col(c).is_infinite() for c in cols_with_inf])
    ).select(["date"] + cols_with_inf)
    print("包含无限值的行示例:")
    print(inf_rows)
else:
    print("✅ 在 _min 和 _max 列中未发现无限值。")
