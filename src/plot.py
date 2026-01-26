import matplotlib.pyplot as plt
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import pandas as pd
import src.config as config

plt.rcParams.update({"font.sans-serif": ["WenQuanYi Micro Hei"], "axes.unicode_minus": False, "font.size": 12})


def plot(net_value_df, relative_net_value, info, strategy=None, scores_path=None, hold_style: pd.DataFrame = None):
    """
    修正版 plot：
    - 统一使用 datetime 类型的 x 轴（pd.to_datetime）
    - 避免将 x 转为字符串再混合使用 range，防止 Plotly 不显示线
    """

    if hold_style is None or not isinstance(hold_style, pd.DataFrame):
        raise ValueError("需要提供 hold_style DataFrame")

    cmvg_cols = [c for c in hold_style.columns if "cmvg" in c]
    style_cols = [c for c in hold_style.columns if "style" in c]

    hold_num_col = "hold_num"
    mem_hold_col = "mem_hold"
    turnover_col = "turnover"
    amt_weighted_rank_col = "amt_weighted_rank"

    # ========== 准备净值数据 ==========
    plot_df = net_value_df.copy()
    try:
        plot_df["超额"] = relative_net_value
    except Exception:
        plot_df["超额"] = pd.Series(relative_net_value, index=plot_df.index)
    plot_df = plot_df.ffill().fillna(1)

    # 统一将 indices 转为 datetime（若已是 datetime 则无变化）
    try:
        plot_df_index_dt = pd.to_datetime(plot_df.index)
    except Exception:
        plot_df_index_dt = plot_df.index

    try:
        hold_index_dt = pd.to_datetime(hold_style.index)
    except Exception:
        hold_index_dt = hold_style.index

    # 计算联合 x_range（使用 datetime 类型）
    try:
        combined = pd.Index(list(plot_df_index_dt) + list(hold_index_dt))
        x_min = combined.min()
        x_max = combined.max()
        x_range = [x_min, x_max]
    except Exception:
        x_range = None

    # ========== Matplotlib ==========
    fig = plt.figure(figsize=(18, 16))
    gs = fig.add_gridspec(3, 2, height_ratios=[3, 1.5, 1.5], wspace=0.25, hspace=0.35)

    # 主图
    ax_main = fig.add_subplot(gs[0, :])
    plot_df.plot(
        ax=ax_main,
        grid=True,
        title=f"基于{strategy}的策略回测结果 (trade_support{config.TRADE_SUPPORT})" if config.STRATEGY == "solve" else f"基于{strategy}的策略回测结果 (top n)",
    )
    ax_main.set_xlabel("")
    ax_main.legend(["策略净值", "指数净值", "超额净值"], loc="upper left", fontsize=14)

    # 次图
    ax_cmvg = fig.add_subplot(gs[1, 0])
    hold_style[cmvg_cols].plot(ax=ax_cmvg, grid=True, legend=True)
    ax_cmvg.set_title("市值偏离")
    ax_cmvg.tick_params(axis="x", labelrotation=30)

    ax_holdnum = fig.add_subplot(gs[1, 1])
    mix_cols = [c for c in [hold_num_col, amt_weighted_rank_col] if c in hold_style.columns]
    mean_map = {c: hold_style[c].mean() for c in mix_cols}
    hold_style[mix_cols].plot(ax=ax_holdnum, grid=True, legend=False)
    labels = [f"{c} ({mean_map[c]:.2f})" for c in mix_cols]
    ax_holdnum.legend(labels)
    ax_holdnum.set_title("持股数量 / 市值加权排名")
    ax_holdnum.tick_params(axis="x", labelrotation=30)

    ax_style = fig.add_subplot(gs[2, 0])
    hold_style[style_cols].plot(ax=ax_style, grid=True, legend=True)
    ax_style.set_title("风格偏离")
    ax_style.tick_params(axis="x", labelrotation=30)

    ax_turnover = fig.add_subplot(gs[2, 1])
    mix_cols = [c for c in [mem_hold_col, turnover_col] if c in hold_style.columns]
    mean_map = {c: hold_style[c].mean() for c in mix_cols}
    hold_style[mix_cols].plot(ax=ax_turnover, grid=True, legend=False)
    labels = [f"{c} ({mean_map[c]:.2f})" for c in mix_cols]
    ax_turnover.legend(labels)
    ax_turnover.set_title("成分股占比 / 换手率")
    ax_turnover.tick_params(axis="x", labelrotation=30)

    # 文本
    abs_keys = ["年化收益", "年化波动", "夏普比率", "累计收益", "最大回撤", "平均回撤", "胜率(天)"]
    rel_keys = ["超额年化收益", "超额年化波动", "信息比率", "超额累计收益", "超额最大回撤", "超额平均回撤", "超额胜率(天)"]

    abs_vals = [info[k] for k in abs_keys]
    rel_vals = [info[k] for k in rel_keys]

    n = len(abs_keys)
    y_pos = np.linspace(0.8, 0.6, n)
    fig.text(1.15, 0.85, "绝对指标", fontsize=18, ha="right", family="WenQuanYi Micro Hei")
    fig.text(1.35, 0.85, "相对指标", fontsize=18, ha="right", family="WenQuanYi Micro Hei")

    for y, ak, rk, av, rv in zip(y_pos, abs_keys, rel_keys, abs_vals, rel_vals):
        fig.text(1, y, ak, fontsize=16, ha="left", family="WenQuanYi Micro Hei")
        fig.text(1.15, y, f"{av*100:.2f}%" if "比率" not in ak else f"{av:.4f}", fontsize=16, ha="right", family="WenQuanYi Micro Hei")
        fig.text(1.2, y, rk, fontsize=16, ha="left", family="WenQuanYi Micro Hei")
        fig.text(1.35, y, f"{rv*100:.2f}%" if "比率" not in ak else f"{rv:.4f}", fontsize=16, ha="right", family="WenQuanYi Micro Hei")

    annual_metrics = ["超额年化收益", "超额年化波动", "信息比率"]
    years = sorted(info["逐年超额年化收益"].keys())
    annual_dicts = [info["逐年超额年化收益"], info["逐年超额年化波动"], info["逐年信息比率"]]
    row_height = 0.035  # 固定行距（可微调）
    y_start = 0.45  # 第一行 y 位置
    y_pos = [y_start - i * row_height for i in range(len(years))]
    fig.text(1.02, 0.5, "年份", fontsize=18, ha="left", family="WenQuanYi Micro Hei")
    for j, m in enumerate(annual_metrics):
        fig.text(1.15 + 0.1 * j, 0.5, m, fontsize=18, ha="right", family="WenQuanYi Micro Hei")
    for y, year in zip(y_pos, years):
        fig.text(1.02, y, str(int(year)), fontsize=16, ha="left", family="WenQuanYi Micro Hei")
        for j, (k, d) in enumerate(zip(annual_metrics, annual_dicts)):
            v = d[year]
            txt = f"{v:.4f}" if k == "信息比率" else f"{v*100:.2f}%"
            fig.text(1.15 + 0.1 * j, y, txt, fontsize=16, ha="right", family="WenQuanYi Micro Hei")

    if config.STRATEGY == "solve":
        png_path = (
            f"/home/haris/results/backtests/{strategy}_trade_support{config.TRADE_SUPPORT}.png"
            if strategy
            else "/home/haris/results/backtests/strategy_trade_support{config.TRADE_SUPPORT}.png"
        )
    else:
        png_path = f"/home/haris/results/backtests/{strategy}_topn.png" if strategy else "/home/haris/results/backtests/strategy_topn.png"
    fig.savefig(png_path, format="png", bbox_inches="tight")
    plt.close(fig)
    print(f"PNG 已保存: {png_path}")

    # ========== Plotly ==========
    fig_plotly = make_subplots(
        rows=4,
        cols=3,
        specs=[
            [{"type": "xy", "rowspan": 2}, None, {"type": "table"}],  # 净值 + 表1
            [None, None, {"type": "table"}],  # 表2
            [{"type": "xy"}, None, {"type": "xy"}],  # 市值 / 持股
            [{"type": "xy"}, None, {"type": "xy"}],  # 风格 / 换手
        ],
        column_widths=[0.65, 0.03, 0.35],
        row_heights=[0.2, 0.2, 0.2, 0.2],
        subplot_titles=["净值曲线", "绝对 / 相对指标", "分年度超额指标", "市值偏离", "持股数量 / 市值加权排名", "风格偏离", "成分股占比 / 换手率"],
    )

    # 主图：净值（legend）
    legend_names = ["策略净值", "指数净值", "超额净值"]
    for col, legend_name in zip(plot_df.columns, legend_names):
        fig_plotly.add_trace(
            go.Scatter(x=plot_df_index_dt, y=plot_df[col].values, mode="lines", name=legend_name, showlegend=True, legend="legend"), row=1, col=1
        )
    # row=3,col=1：风格因子（legend2）
    for col in cmvg_cols:
        fig_plotly.add_trace(go.Scatter(x=hold_index_dt, y=hold_style[col].values, mode="lines", name=col, showlegend=True, legend="legend2"), row=3, col=1)
    # row=3,col=2：持股数量 / 市值加权排名（legend3）
    for col in [c for c in [hold_num_col, amt_weighted_rank_col] if c in hold_style.columns]:
        mean_val = hold_style[col].mean()
        fig_plotly.add_trace(
            go.Scatter(x=hold_index_dt, y=hold_style[col].values, mode="lines", name=f"{col} ({mean_val:.2f})", showlegend=True, legend="legend3"), row=3, col=3
        )
    # row=4,col=1：风格暴露（legend4）
    for col in style_cols:
        fig_plotly.add_trace(go.Scatter(x=hold_index_dt, y=hold_style[col].values, mode="lines", name=col, showlegend=True, legend="legend4"), row=4, col=1)
    # row=4,col=2：成分股占比 / 换手率（legend5）
    for col in [c for c in [mem_hold_col, turnover_col] if c in hold_style.columns]:
        mean_val = hold_style[col].mean()
        fig_plotly.add_trace(
            go.Scatter(x=hold_index_dt, y=hold_style[col].values, mode="lines", name=f"{col} ({mean_val:.2f})", showlegend=True, legend="legend5"), row=4, col=3
        )

    # 统一 x 轴范围（datetime）
    fig_plotly.update_xaxes(range=x_range, row=1, col=1)
    fig_plotly.update_xaxes(range=x_range, row=2, col=1)
    fig_plotly.update_xaxes(range=x_range, row=2, col=2)
    fig_plotly.update_xaxes(range=x_range, row=3, col=1)
    fig_plotly.update_xaxes(range=x_range, row=3, col=2)
    fig_plotly.update_layout(
        height=1300,
        width=1500,
        title=f"基于{strategy}的策略回测结果 (trade_support{config.TRADE_SUPPORT})" if config.STRATEGY == "solve" else f"基于{strategy}的策略回测结果 (top n)",
        legend=dict(x=0.55, y=1.0, xanchor="left", yanchor="top"),
        legend2=dict(x=0.55, y=0.44, xanchor="left", yanchor="top"),
        legend3=dict(x=1.002, y=0.44, xanchor="left", yanchor="top"),
        legend4=dict(x=0.55, y=0.16, xanchor="left", yanchor="top"),
        legend5=dict(x=1.002, y=0.16, xanchor="left", yanchor="top"),
    )

    # 指标表1
    headers = ["", "绝对指标", "", "相对指标"]
    cells = [
        abs_keys,
        [f"{v*100:.2f}%" if "比率" not in k else f"{v:.4f}" for k, v in zip(abs_keys, abs_vals)],
        rel_keys,
        [f"{v*100:.2f}%" if "比率" not in k else f"{v:.4f}" for k, v in zip(rel_keys, rel_vals)],
    ]
    fig_plotly.add_trace(
        go.Table(
            header=dict(values=headers, font=dict(family="WenQuanYi Micro Hei", size=12)),
            cells=dict(values=cells, font=dict(family="WenQuanYi Micro Hei", size=10)),
        ),
        row=1,
        col=3,
    )

    # 指标表2
    annual_headers = ["年份"] + ["超额年化收益", "超额年化波动", "信息比率"]
    annual_cells = [
        [[y] for y in years],
        [f"{annual_dicts[0][y]*100:.2f}%" for y in years],  # 超额年化收益
        [f"{annual_dicts[1][y]*100:.2f}%" for y in years],  # 超额年化波动
        [f"{annual_dicts[2][y]:.4f}" for y in years],  # 信息比率
    ]
    fig_plotly.add_trace(
        go.Table(
            header=dict(values=annual_headers, font=dict(family="WenQuanYi Micro Hei", size=12)),
            cells=dict(values=annual_cells, font=dict(family="WenQuanYi Micro Hei", size=10)),
        ),
        row=2,
        col=3,
    )

    # 保存可交互 HTML
    if config.STRATEGY == "solve":
        html_path = (
            f"/home/haris/results/backtests/{strategy}_trade_support{config.TRADE_SUPPORT}.html"
            if strategy
            else "/home/haris/results/backtests/strategy_trade_support{config.TRADE_SUPPORT}.html"
        )
    else:
        html_path = f"/home/haris/results/backtests/{strategy}_topn.html" if strategy else "/home/haris/results/backtests/strategy_topn.html"
    pio.write_html(fig_plotly, file=html_path, auto_open=False)
    print(f"可交互 HTML 已保存: {html_path}")
