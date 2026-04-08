import os
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 绘图风格
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams.update({
    "font.sans-serif": ["WenQuanYi Micro Hei"],
    "axes.unicode_minus": False,
    "font.size": 12
})


def plot_strategy_comparison(file_paths, title, save_dir, date_str, support_type):
    """
    绘制策略超额净值比较图（交易日等距）
    """

    os.makedirs(save_dir, exist_ok=True)

    # ========= 1️⃣ 读取数据 =========
    data_frames = {}
    start_dates = []

    for path in file_paths:
        df = pd.read_csv(path, index_col=0)
        df.index = pd.to_datetime(df.index.astype(str), format='%Y%m%d')
        df = df.sort_index()

        label_name = os.path.basename(path).replace("_rel_nv.csv", "")
        data_frames[label_name] = df
        start_dates.append(df.index[0])

    # ========= 2️⃣ 找共同起点 =========
    latest_start_date = max(start_dates)
    print(f"所有曲线统一从 {latest_start_date.date()} 开始")

    # ========= 3️⃣ 截断 + 对齐长度 =========
    sliced_dfs = {}
    min_len = float('inf')

    for name, df in data_frames.items():
        df_sliced = df[df.index >= latest_start_date].copy()
        if not df_sliced.empty:
            min_len = min(min_len, len(df_sliced))
            sliced_dfs[name] = df_sliced

    # 统一长度（防止x轴错位）
    for name in sliced_dfs:
        sliced_dfs[name] = sliced_dfs[name].iloc[:min_len]

    # ========= 4️⃣ 绘图 =========
    plt.figure(figsize=(12, 6))

    legend_labels = [
        "早盘策略(20日)",
        "早盘策略(10日)",
        "午盘策略(20日)",
        "午盘策略(10日)"
    ]

    for i, (name, df) in enumerate(sliced_dfs.items()):
        # 确保是Series
        if isinstance(df, pd.DataFrame):
            series = df.iloc[:, 0]
        else:
            series = df

        # 归一化
        norm_nv = series / series.iloc[0]

        # ✅ 关键：使用等距横轴
        x = np.arange(len(norm_nv))

        plt.plot(
            x,
            norm_nv,
            label=f"{legend_labels[i]} ({norm_nv.iloc[-1]:.4f})",
            linewidth=1.5
        )

    # ========= 5️⃣ 设置交易日标签 =========
    sample_df = list(sliced_dfs.values())[0]
    dates = sample_df.index.strftime('%Y-%m-%d')
    x = np.arange(len(dates))

    step = max(len(dates) // 10, 1)  # 控制显示密度

    plt.xticks(
        ticks=x[::step],
        labels=dates[::step],
        rotation=45
    )

    # ========= 6️⃣ 图表美化 =========
    plt.axhline(y=1.0, color='black', linestyle='-', alpha=0.3)
    plt.title(title, fontsize=14)
    plt.xlabel('交易日', fontsize=12)
    plt.ylabel('超额净值', fontsize=12)

    plt.legend(bbox_to_anchor=(1, 1), loc='upper left', fontsize=12)
    plt.grid(True)
    plt.tight_layout()

    # ========= 7️⃣ 保存 =========
    save_path = os.path.join(save_dir, f"{date_str}_trade_support_{support_type}.pdf")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"图表已保存到: {save_path}")

    plt.show()


def main():
    target_dir_list = [
        "/home/haris/mymodel/backtests",
        "/home/haris/mymodel_noon/backtests",
        "/home/haris/mymodel_10/backtests",
        "/home/haris/mymodel_noon_10/backtests"
    ]

    suffix = '_rel_nv.csv'
    result_files = []

    # ========= 1️⃣ 搜索文件 =========
    for target_dir in target_dir_list:
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                if file.endswith(suffix):
                    result_files.append(os.path.join(root, file))

    result_files.sort()

    # ========= 2️⃣ 按日期分组 =========
    date_groups = defaultdict(list)

    for path in result_files:
        filename = os.path.basename(path)
        if "period_" not in filename:
            continue
        date_str = filename.split('period_')[1][:8]
        date_groups[date_str].append(path)

    latest_date = max(date_groups.keys())
    latest_files = date_groups[latest_date]

    # ========= 3️⃣ 按 support 分类 =========
    support_groups = {
        "support5": [],
        "support7": [],
        "support8": [],
    }

    for path in latest_files:
        if "trade_support5" in path:
            support_groups["support5"].append(path)
        elif "trade_support7" in path:
            support_groups["support7"].append(path)
        elif "trade_support8" in path:
            support_groups["support8"].append(path)

    # ========= 4️⃣ 绘图 =========
    if support_groups["support5"]:
        plot_strategy_comparison(
            support_groups["support5"],
            "策略超额净值比较 (Trade Support 5)",
            "/home/haris/mymodel_compare",
            latest_date,
            5
        )

    if support_groups["support7"]:
        plot_strategy_comparison(
            support_groups["support7"],
            "策略超额净值比较 (Trade Support 7)",
            "/home/haris/mymodel_compare",
            latest_date,
            7
        )

    if support_groups["support8"]:
        plot_strategy_comparison(
            support_groups["support8"],
            "策略超额净值比较 (Trade Support BARRA)",
            "/home/haris/mymodel_compare",
            latest_date,
            8
        )


if __name__ == "__main__":
    main()