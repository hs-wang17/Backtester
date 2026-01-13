import pandas as pd
import numpy as np
import os

# ================= 配置路径 =================
score_1_path = "/home/haris/results/predictions/240_120_ret3_5_10_20_mse2.csv"
score_2_path = "/home/haris/results/predictions/StockPredictor_20251231.csv"
output_dir = "/home/haris/results/predictions/fusion_StockPredictor_20251231_and_240_120_ret3_5_10_20_mse2"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# ================= 数据加载与对齐 =================
def load_wide_data(path):
    print(f"Loading {path}...")
    df = pd.read_csv(path, index_col=0)
    df = df.reindex(sorted(df.columns), axis=1)
    return df


df1 = load_wide_data(score_1_path)
df2 = load_wide_data(score_2_path)

# 严格对齐：取公共股票代码(Index)和公共日期(Columns)
common_codes = df1.index.intersection(df2.index)
common_dates = df1.columns.intersection(df2.columns)

# 重新切片并排序，确保矩阵位置完全对应
s1 = df1.loc[common_codes, common_dates]
s2 = df2.loc[common_codes, common_dates]

print(f"Aligned Shape: {s1.shape} (Stocks x Dates)")

# ================= 矩阵化处理工具 (针对宽表列操作) =================


def get_rank(df):
    # axis=0 对每一列（日期）内的股票进行排名
    return df.rank(axis=0, pct=True)


def get_zscore(df):
    # 截面标准化，加入极小值防止除以0
    return df.apply(lambda x: (x - x.mean()) / (x.std() + 1e-12), axis=0)


def get_winsorize(df, limits=0.025):
    # 截面去极值
    return df.apply(lambda x: x.clip(x.quantile(limits), x.quantile(1 - limits)), axis=0)


# ================= 10+ 种融合算法 =================

results = {}

# 预计算基础指标
r1, r2 = get_rank(s1), get_rank(s2)
z1, z2 = get_zscore(s1), get_zscore(s2)

# 1. Rank 平均 (核心需求)
results["01_rank_avg"] = (r1 + r2) / 2

# 2. Z-Score 平均
results["02_zscore_avg"] = (z1 + z2) / 2

# 3. 几何平均 (Rank 映射到 [1,2] 避免 0)
results["03_rank_geo_avg"] = np.sqrt((r1 + 1) * (r2 + 1)) - 1

# 4. 最大值融合 (取两模型中最看好的)
results["04_rank_max"] = np.maximum(r1, r2)

# 5. 最小值融合 (取共识最强的部分)
results["05_rank_min"] = np.minimum(r1, r2)

# 6. 非线性 Rank 增强 (平方和平均，突出头部)
results["06_rank_pow2"] = (r1**2 + r2**2) / 2

# 7. 缩尾 Z-Score 平均 (更稳健)
results["07_winsor_zscore_avg"] = (get_zscore(get_winsorize(s1)) + get_zscore(get_winsorize(s2))) / 2


# 8. Rank Sigmoid 变换 (拉开两端差距)
def sigmoid(x):
    return 1 / (1 + np.exp(-(x - 0.5) * 8))


results["08_rank_sigmoid"] = (sigmoid(r1) + sigmoid(r2)) / 2

# 9. 差异惩罚权重 (一致性越高得分越高)
diff = np.abs(r1 - r2)
results["09_consistency_weighted"] = ((r1 + r2) / 2) * (1 - 0.5 * diff)

# 10. 离散分箱投票 (将 Rank 分为 10 个箱，取平均)
results["10_binned_voting"] = (np.floor(r1 * 10) + np.floor(r2 * 10)) / 2


# ================= 保存结果 =================
print("Saving results to CSV...")
for name, df_res in results.items():
    output_path = f"{output_dir}/{name}.csv"
    df_res.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_res.dropna(how="all", inplace=True)
    df_res.to_csv(output_path)
    print(f"Saved: {output_path}")

print("\nDone! All fusion files are in wide format (Index: Code, Columns: Date).")
