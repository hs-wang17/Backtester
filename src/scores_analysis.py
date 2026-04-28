import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.offsetbox import AnchoredText
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import os
import math
from multiprocessing import Pool
import argparse
import matplotlib

matplotlib.use("Agg")
plt.style.use("bmh")
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "SimHei", "Arial"]
plt.rcParams["axes.unicode_minus"] = False


# ==============================================================================
#  配置参数 (Configuration)
# ==============================================================================
parser = argparse.ArgumentParser(description="Factor score analysis report")
parser.add_argument("--scores_path", type=str, required=True, help="Path to score csv file (stocks x dates or dates x stocks)")
args = parser.parse_args()

SCORE_FILE_PATH = args.scores_path
SAVE_DIR = "/home/haris/results/scores_analysis"
PERIODS = [1, 3, 5, 10, 20]
EXRET = True
NUM_PROCESSES = min(len(PERIODS), os.cpu_count())
NEED_SHIFT = True  # [重要设置] 因子是否需要滞后一天？
FILE_PREFIX = os.path.splitext(os.path.basename(SCORE_FILE_PATH))[0]
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)


# ==============================================================================
# 1. 数据读取
# ==============================================================================
print("=" * 60)
print(f"正在读取数据: {FILE_PREFIX}")
if NEED_SHIFT:
    print(">>> 模式: [ Shift(1) ] (T日因子 -> T+1日收益)")
else:
    print(">>> 模式: [ No Shift ] (T日因子 -> T日收益)")
print("=" * 60)

scores_raw = pd.read_csv(SCORE_FILE_PATH, index_col=0).T
if NEED_SHIFT:
    scores_raw = scores_raw.shift(1).dropna(how="all")
else:
    scores_raw = scores_raw.dropna(how="all")

scores_raw.index = scores_raw.index.map(lambda x: str(x).replace("-", ""))
scores_raw.columns = ["%06d" % (int(x)) for x in scores_raw.columns]

df_open = pd.read_feather("/home/haris/data/data_frames/stk_adjopen.feather")
df_amount = pd.read_feather("/home/haris/data/data_frames/stk_amount.feather")
df_idx_open = pd.read_feather("/home/haris/data/data_frames/idx_open.feather")
sig_ipo = pd.read_feather("/home/haris/data/data_frames/stk_ipodays.feather")
sig_stop = pd.read_feather("/home/haris/data/data_frames/stk_is_stop_stock.feather")
sig_st = pd.read_feather("/home/haris/data/data_frames/stk_is_st_stock.feather")


# ==============================================================================
# 2. 计算任务函数
# ==============================================================================
def run_calculation_task(p):
    try:
        df_amount_sig = (df_amount > 0) * 1
        valid_sig = ((sig_ipo > 120) * (sig_stop == 0) * (sig_st == 0) * df_amount_sig).replace(0, np.nan)

        df_idx_ret = (df_idx_open.shift(-p) / df_idx_open - 1).loc[:, "中证1000"]

        if EXRET:
            df_ret = (df_open.shift(-p) / df_open - 1).subtract(df_idx_ret, axis=0)
        else:
            df_ret = df_open.shift(-p) / df_open - 1

        df_ret = df_ret / p

        common_dates = scores_raw.index.intersection(df_ret.index).intersection(valid_sig.index)
        common_tickers = scores_raw.columns.intersection(df_ret.columns).intersection(valid_sig.columns)

        s_aligned = scores_raw.loc[common_dates, common_tickers] * valid_sig.loc[common_dates, common_tickers]
        r_aligned = df_ret.loc[common_dates, common_tickers] * valid_sig.loc[common_dates, common_tickers]

        ic_series = s_aligned.corrwith(r_aligned, axis=1)
        rank_ic_series = s_aligned.rank(axis=1).corrwith(r_aligned.rank(axis=1), axis=1)

        def get_stats_block(series, name):
            n = series.count()
            mean = series.mean()
            std = series.std()
            t_val = (mean / std * np.sqrt(n)) if std != 0 else np.nan
            ir = mean / std if std != 0 else np.nan
            return f"{name:<6} | Mean:{mean:>7.4f} | Std:{std:>6.4f}\n" f"{' '*7}| IR  :{ir:>7.4f} | T  :{t_val:>6.2f}"

        stats_text = f"Period: {p}\n" f"{'-'*32}\n" f"{get_stats_block(ic_series, 'IC')}\n" f"{get_stats_block(rank_ic_series, 'RankIC')}"

        df_stack = pd.DataFrame({"score": s_aligned.stack(), "ret": r_aligned.stack()})

        def fast_qcut_internal(x):
            if x.count() < 10:
                return pd.Series(index=x.index, data=np.nan)
            return pd.cut(x.rank(pct=True), bins=np.linspace(0, 1, 11), labels=range(1, 11))

        df_stack["group"] = df_stack.groupby(level=0, group_keys=False)["score"].apply(fast_qcut_internal)

        final_df = df_stack.groupby([df_stack.index.get_level_values(0), "group"], observed=True)["ret"].mean().unstack()
        final_df.columns = [f"G{int(i)}" for i in final_df.columns]

        return {
            "status": "success",
            "period": p,
            "ic_cumsum": ic_series.cumsum(),
            "rank_ic_cumsum": rank_ic_series.cumsum(),
            "cum_ret": final_df.cumsum(),
            "group_means": final_df.mean(),
            "stats_text": stats_text,
            "dates": common_dates,
        }
    except Exception as e:
        return {"status": "fail", "period": p, "error": str(e)}


# ==============================================================================
# 3. 辅助绘图函数：顶部因子分布图
# ==============================================================================
def draw_distribution_plot(ax, scores):
    """
    在指定的 ax 上绘制因子分布折线图 (多色区分)
    """
    # 1. 计算统计量
    stats = pd.DataFrame(index=scores.index)
    stats["Max"] = scores.max(axis=1)
    stats["Min"] = scores.min(axis=1)
    stats["Mean"] = scores.mean(axis=1)
    stats["Median"] = scores.median(axis=1)
    stats["Q01"] = scores.quantile(0.01, axis=1)
    stats["Q25"] = scores.quantile(0.25, axis=1)
    stats["Q75"] = scores.quantile(0.75, axis=1)
    stats["Q99"] = scores.quantile(0.99, axis=1)

    dates = stats.index

    # 2. 绘制区间填充
    ax.fill_between(dates, stats["Q01"], stats["Q99"], color="#9467bd", alpha=0.08)  # 紫色
    ax.fill_between(dates, stats["Q25"], stats["Q75"], color="#1f77b4", alpha=0.15)  # 蓝色

    # 3. 绘制彩色线条

    # [修改点] 极值 (Max/Min): 改为黑色 (black)，提高 alpha，确保在灰色背景下可见
    ax.plot(dates, stats["Max"], color="black", ls=":", lw=1.2, alpha=0.7, label="Max/Min")
    ax.plot(dates, stats["Min"], color="black", ls=":", lw=1.2, alpha=0.7)

    # 99% & 1%: 紫色
    ax.plot(dates, stats["Q99"], color="#9467bd", ls="--", lw=1.2, alpha=0.8, label="1% / 99%")
    ax.plot(dates, stats["Q01"], color="#9467bd", ls="--", lw=1.2, alpha=0.8)

    # 75% & 25%: 蓝色
    ax.plot(dates, stats["Q75"], color="#1f77b4", ls="-.", lw=1.5, alpha=0.9, label="25% / 75%")
    ax.plot(dates, stats["Q25"], color="#1f77b4", ls="-.", lw=1.5, alpha=0.9)

    # Median: 绿色
    ax.plot(dates, stats["Median"], color="#2ca02c", ls="-", lw=1.8, alpha=1.0, label="Median")

    # Mean: 红色 (最醒目)
    ax.plot(dates, stats["Mean"], color="#d62728", ls="-", lw=2.2, alpha=1.0, label="Mean")

    ax.set_title(f"Factor Score Distribution Over Time", fontsize=14, fontweight="bold", pad=10)
    ax.set_ylabel("Score Value")
    ax.legend(loc="upper left", frameon=True, fontsize=9, ncol=5)
    ax.grid(True, linestyle=":", alpha=0.6)

    tick_spacing = max(len(dates) // 12, 1)
    ax.set_xticks(range(0, len(dates), tick_spacing))
    ax.set_xticklabels([dates[i] for i in range(0, len(dates), tick_spacing)], rotation=15, ha="right", fontsize=9)

    y_lower = stats["Min"].quantile(0.01)
    y_upper = stats["Max"].quantile(0.99)
    y_range = y_upper - y_lower
    if y_range > 0:
        ax.set_ylim(y_lower - y_range * 0.1, y_upper + y_range * 0.1)


# ==============================================================================
# 主程序
# ==============================================================================
if __name__ == "__main__":
    print(f"\n开始并行回测任务 | 周期: {PERIODS}")

    with Pool(processes=NUM_PROCESSES) as pool:
        results = pool.map(run_calculation_task, PERIODS)

    success_results = [r for r in results if r["status"] == "success"]
    success_results.sort(key=lambda x: x["period"])

    if not success_results:
        print("所有任务失败。")
        exit()

    # --- 布局计算 ---
    N = len(success_results)
    n_cols = math.ceil(math.sqrt(N))
    if n_cols < 2:
        n_cols = 2
    n_rows_backtest = math.ceil(N / n_cols)
    total_rows = n_rows_backtest + 1

    fig = plt.figure(figsize=(6 + (14 * n_rows_backtest), 11 * n_cols))

    cmap = plt.get_cmap("RdYlGn")
    colors_list = [cmap(i) for i in np.linspace(0, 1, 10)]
    ratios = [0.6] + [1.0] * n_rows_backtest
    outer_grid = gridspec.GridSpec(total_rows, n_cols, figure=fig, height_ratios=ratios, wspace=0.25, hspace=0.30, left=0.05, right=0.95, top=0.96, bottom=0.03)

    # 1. 顶部分布图
    print("绘制顶部因子分布图 ...")
    ax_dist = fig.add_subplot(outer_grid[0, :])
    draw_distribution_plot(ax_dist, scores_raw)

    # 2. 回测图
    for i, res in enumerate(success_results):
        p = res["period"]
        print(f"绘制 Period {p} ...")

        row_idx = (i // n_cols) + 1
        col_idx = i % n_cols
        cell = outer_grid[row_idx, col_idx]

        inner_grid = gridspec.GridSpecFromSubplotSpec(3, 1, subplot_spec=cell, height_ratios=[1, 1.2, 0.8], hspace=0.45)

        ax1 = fig.add_subplot(inner_grid[0])
        ax2 = fig.add_subplot(inner_grid[1], sharex=ax1)
        ax3 = fig.add_subplot(inner_grid[2])

        # A. IC
        ax1.axhline(0, color="black", lw=0.8, alpha=0.5)
        (l1,) = ax1.plot(res["ic_cumsum"], color="#2b6ea6", lw=1.8, label="IC", alpha=0.9)
        (l2,) = ax1.plot(res["rank_ic_cumsum"], color="#e88523", lw=1.8, ls="--", label="RankIC", alpha=0.9)

        all_vals = np.concatenate([res["ic_cumsum"].values, res["rank_ic_cumsum"].values])
        finite_vals = all_vals[np.isfinite(all_vals)]
        if finite_vals.size == 0:
            ax1.set_ylim(-1, 1)
        else:
            y_min, y_max = finite_vals.min(), finite_vals.max()
            y_range = y_max - y_min
            ax1.set_ylim(y_min - y_range * 0.05, y_max + y_range * 0.40)

        title_suffix = "(Shifted)" if NEED_SHIFT else "(No Shift)"
        ax1.set_title(f"IC Analysis {title_suffix} (Period={p})", fontsize=13, fontweight="bold", pad=5)
        ax1.legend(loc="lower left", frameon=True, fontsize=9, ncol=2)

        at = AnchoredText(res["stats_text"], prop=dict(size=9, family="monospace"), frameon=True, loc="upper left")
        at.patch.set_boxstyle("round,pad=0.3,rounding_size=0.2")
        at.patch.set_facecolor("white")
        at.patch.set_alpha(0.95)
        ax1.add_artist(at)

        # B. Quantile
        cum_ret = res["cum_ret"]
        for idx, col in enumerate(cum_ret.columns):
            ax2.plot(cum_ret.index, cum_ret[col], color=colors_list[idx], lw=1.5, label=col)

        ax2.set_title(f"Quantile Returns", fontsize=13, fontweight="bold", pad=5)
        ax2.grid(True, linestyle=":", alpha=0.5)
        ax2.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8, borderaxespad=0.0)

        plt.setp(ax1.get_xticklabels(), visible=False)
        dates = res["dates"]
        tick_spacing = max(len(dates) // 8, 1)
        ax2.set_xticks(range(0, len(dates), tick_spacing))
        ax2.set_xticklabels(dates[::tick_spacing], rotation=30, ha="right", fontsize=9)

        # C. Bar
        means = res["group_means"]
        x_pos = np.arange(len(means))
        bars = ax3.bar(x_pos, means, color=colors_list, edgecolor="grey", linewidth=0.5, alpha=0.9, width=0.6)

        ax3.set_title("Group Mean Returns (Daily)", fontsize=13, fontweight="bold", pad=10)
        ax3.axhline(0, color="black", linewidth=0.8)
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels([f"G{i+1}" for i in range(10)], fontsize=10)

        bar_max = means.max()
        bar_min = means.min()
        bar_range = bar_max - bar_min
        if bar_range > 0:
            ax3.set_ylim(bottom=bar_min - bar_range * 0.15, top=bar_max + bar_range * 0.30)

        for bar, val in zip(bars, means):
            height = bar.get_height()
            offset = bar_range * 0.03
            ax3.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + (offset if height > 0 else -offset * 2),
                f"{val:.5f}",
                ha="center",
                va="bottom" if height > 0 else "top",
                fontsize=9,
                color="#333333",
                fontweight="bold",
            )

    save_path = os.path.join(SAVE_DIR, f"{FILE_PREFIX}_Report.pdf")
    print(f"正在保存 PDF: {save_path} ...")
    plt.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
