import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# ==================== 路径配置 ====================
min_path = "/home/haris/data/min_data"
output_path = "/home/haris/raid0/shared/haris/vwap_twap"
files = sorted(os.listdir(min_path))

# ==================== 区间定义 ====================
intervals = [
    (0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30),  # 早盘前30分钟每5分钟一个区间
    (0, 10), (10, 20), (20, 30),  # 早盘前30分钟每10分钟一个区间
    (0, 15), (15, 30),  # 早盘前30分钟每15分钟一个区间
    (0, 30),  # 早盘前30分钟总区间
    (121, 126), (126, 131), (131, 136), (136, 141), (141, 146), (146, 151),  # 午盘前30分钟每5分钟一个区间
    (121, 131), (131, 141), (141, 151),  # 午盘前30分钟每10分钟一个区间
    (121, 136), (136, 151),  # 午盘前30分钟每15分钟一个区间
    (121, 151)  # 午盘前30分钟总区间
]

# ==================== 单文件处理 ====================
def process_file(f):
    data = pd.read_feather(os.path.join(min_path, f))

    vol = data.pivot(index="time", columns="code", values="volume")
    amt = data.pivot(index="time", columns="code", values="amount")
    price = data.pivot(index="time", columns="code", values="close")

    date = f[:8]

    result = {
        "date": date,
        "interval_vwap": {},
        "interval_twap": {}
    }

    for start, end in intervals:
        key = f"{start}_{end}"
        result["interval_vwap"][key] = amt.iloc[start:end].sum() / vol.iloc[start:end].sum().replace(0, np.nan)
        result["interval_twap"][key] = price.iloc[start:end].mean()

    return result


# ==================== 并行执行 ====================
vwap_5min_dict = defaultdict(dict)
twap_5min_dict = defaultdict(dict)

max_workers = os.cpu_count() - 2

with ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_file, f) for f in files]

    for future in tqdm(as_completed(futures), total=len(futures), desc="并行计算VWAP/TWAP"):
        res = future.result()
        date = res["date"]

        for start, end in intervals:
            key = f"{start}_{end}"
            vwap_5min_dict[key][date] = res["interval_vwap"][key]
            twap_5min_dict[key][date] = res["interval_twap"][key]


# ==================== 保存结果 ====================
pd.DataFrame(vwap_5min_dict["0_30"]).T.sort_index().to_feather(
    os.path.join(output_path, "vwap.fea")
)

pd.DataFrame(twap_5min_dict["0_30"]).T.sort_index().to_feather(
    os.path.join(output_path, "twap.fea")
)

pd.DataFrame(vwap_5min_dict["121_151"]).T.sort_index().to_feather(
    os.path.join(output_path, "vwap_noon.fea")
)

pd.DataFrame(twap_5min_dict["121_151"]).T.sort_index().to_feather(
    os.path.join(output_path, "twap_noon.fea")
)

# 保存所有区间
for key, interval_vwap in vwap_5min_dict.items():
    pd.DataFrame(interval_vwap).T.sort_index().to_feather(
        os.path.join(output_path, f"vwap_{key}.fea")
    )

for key, interval_twap in twap_5min_dict.items():
    pd.DataFrame(interval_twap).T.sort_index().to_feather(
        os.path.join(output_path, f"twap_{key}.fea")
    )