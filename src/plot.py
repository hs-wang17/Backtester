import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.lines as mlines
import numpy as np
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

    # ==========================================
    # 1. 样式配置 (Style Configuration)
    # ==========================================
    STYLE = {
        "font_family": ["WenQuanYi Micro Hei", "SimHei", "DejaVu Sans"],
        "colors": {
            "strategy": "#1f77b4",  # 策略：深蓝
            "benchmark": "#7f7f7f",  # 指数：中性灰
            "excess": "#d62728",  # 超额：红色
            "grid": "#e0e0e0",  # 网格：极浅灰
            "text_main": "#333333",  # 主文本
            "text_sub": "#666666",  # 次级文本
            "table_row_even": "#ffffff",  # 表格偶数行背景
            "table_row_odd": "#f8f9fa",  # 表格奇数行背景
            "border": "#818181",  # 边框颜色
        },
        "font_size": {"title": 20, "subtitle": 18, "body": 16, "small": 12},
    }

    plt.rcParams["font.sans-serif"] = STYLE["font_family"]
    plt.rcParams["axes.unicode_minus"] = False

    # ==========================================
    # 2. 辅助绘图函数 (Helper Functions)
    # ==========================================

    def beautify_axis(ax, title=None, fontsize=STYLE["font_size"]["title"]):
        """标准化图表样式"""
        if title:
            ax.set_title(title, fontsize=fontsize, fontweight="bold", color=STYLE["colors"]["text_main"], pad=10)

        # 设置网格
        ax.grid(True, which="major", linestyle="--", linewidth=0.5, color=STYLE["colors"]["grid"], zorder=0)

        # 隐藏上右边框
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(STYLE["colors"]["border"])
        ax.spines["bottom"].set_color(STYLE["colors"]["border"])

        ax.tick_params(axis="both", which="major", labelsize=STYLE["font_size"]["small"], colors=STYLE["colors"]["text_sub"])

    def draw_table_block(fig, x, y, width, title, data_list, col_ratios, row_height=0.035, fontsize_title=STYLE["font_size"]["subtitle"]):
        """
        绘制表格块通用函数
        :param col_ratios: 各列宽度比例，总和应为 1
        """
        # 绘制标题
        fig.text(x, y, title, fontsize=fontsize_title, fontweight="bold", color=STYLE["colors"]["strategy"], ha="left", va="bottom")

        current_y = y - 0.01
        col_widths = [r * width for r in col_ratios]
        total_height = len(data_list) * row_height

        # 顶部横线
        line = mlines.Line2D(
            [x, x + width], [current_y, current_y], color=STYLE["colors"]["strategy"], linewidth=2, transform=fig.transFigure
        )
        fig.add_artist(line)

        for i, row_data in enumerate(data_list):
            row_y = current_y - (i + 1) * row_height

            # 斑马纹背景
            bg_color = STYLE["colors"]["table_row_odd"] if i % 2 == 0 else STYLE["colors"]["table_row_even"]
            rect = patches.Rectangle((x, row_y), width, row_height, linewidth=0, facecolor=bg_color, transform=fig.transFigure, zorder=-1)
            fig.add_artist(rect)

            current_x = x
            for j, text_val in enumerate(row_data):
                # 对齐方式逻辑：
                # 如果是4列模式：第1、3列（名称）左对齐，第2、4列（数值）右对齐
                # 如果是其他模式：第1列左对齐，其余右对齐
                if len(col_ratios) == 4:
                    ha = "left" if j % 2 == 0 else "right"
                else:
                    ha = "left" if j == 0 else "right"

                text_x = current_x + 0.005 if ha == "left" else current_x + col_widths[j] - 0.005

                # 简单着色逻辑
                txt_color = STYLE["colors"]["text_main"]
                val_str = str(text_val)

                fig.text(
                    text_x,
                    row_y + row_height / 2,
                    val_str,
                    ha=ha,
                    va="center",
                    fontsize=STYLE["font_size"]["body"],
                    color=txt_color,
                    fontname="WenQuanYi Micro Hei",
                )

                current_x += col_widths[j]

            # 行底部分隔线
            if i < len(data_list) - 1:
                line = mlines.Line2D(
                    [x, x + width], [row_y, row_y], color=STYLE["colors"]["grid"], linewidth=0.5, transform=fig.transFigure
                )
                fig.add_artist(line)

        # 底部横线
        bottom_y = current_y - total_height
        line = mlines.Line2D([x, x + width], [bottom_y, bottom_y], color=STYLE["colors"]["border"], linewidth=1, transform=fig.transFigure)
        fig.add_artist(line)

        return bottom_y

    # ==========================================
    # 3. 主绘图逻辑
    # ==========================================

    # 设置画布，右侧留出空间
    fig = plt.figure(figsize=(24, 16), facecolor="white")
    gs = fig.add_gridspec(3, 2, height_ratios=[3, 1.5, 1.5], wspace=0.2, hspace=0.45, left=0.05, right=0.65, top=0.95, bottom=0.05)

    # --- 左侧：图表区域 ---

    # 1. 主图：净值曲线
    ax_main = fig.add_subplot(gs[0, :])
    # 显式指定颜色和标签，防止混淆
    plot_df.plot(
        ax=ax_main, color=[STYLE["colors"]["strategy"], STYLE["colors"]["benchmark"], STYLE["colors"]["excess"]], linewidth=2, alpha=0.9
    )
    if config.APM_MODE:
        main_title = (
            f"基于{strategy}的策略回测结果 (早午盘)" if config.STRATEGY == "solve" else f"基于{strategy}的策略回测结果 (前 N / 早午盘)"
        )
    else:
        if config.AFTERNOON_START:
            main_title = (
                f"基于{strategy}的策略回测结果 (午盘)" if config.STRATEGY == "solve" else f"基于{strategy}的策略回测结果 (前 N / 午盘)"
            )
        elif config.CALL_START:
            main_title = (
                f"基于{strategy}的策略回测结果 (集合竞价)"
                if config.STRATEGY == "solve"
                else f"基于{strategy}的策略回测结果 (前 N / 集合竞价)"
            )
        else:
            main_title = (
                f"基于{strategy}的策略回测结果 (早盘)" if config.STRATEGY == "solve" else f"基于{strategy}的策略回测结果 (前 N / 早盘)"
            )
    beautify_axis(ax_main, main_title)
    ax_main.set_xlabel("")
    # 优化图例：放置在左上角，去除边框，使其融合
    ax_main.legend(
        ["策略净值", "指数净值", "超额净值"],
        loc="upper left",
        frameon=True,
        framealpha=0.9,
        edgecolor="none",
        fontsize=STYLE["font_size"]["small"],
    )

    # 2. 次图：市值偏离
    ax_cmvg = fig.add_subplot(gs[1, 0])
    if not hold_style[cmvg_cols].empty:
        hold_style[cmvg_cols].plot(ax=ax_cmvg, linewidth=1.5, alpha=0.8)
    beautify_axis(ax_cmvg, "市值偏离", fontsize=STYLE["font_size"]["subtitle"])
    ax_cmvg.tick_params(axis="x", labelrotation=30)
    ax_cmvg.legend(
        [f"分组{i}" for i in range(1, len(cmvg_cols) + 1)], fontsize=STYLE["font_size"]["small"], loc="upper right", framealpha=0.8
    )

    # 3. 次图：持股数量
    ax_holdnum = fig.add_subplot(gs[1, 1])
    mix_cols_1 = [c for c in [hold_num_col, amt_weighted_rank_col] if c in hold_style.columns]
    if mix_cols_1:
        mean_map_1 = {c: hold_style[c].mean() for c in mix_cols_1}
        # 使用双色避免混淆
        colors_hold = [STYLE["colors"]["strategy"], STYLE["colors"]["excess"]]
        hold_style[mix_cols_1].plot(ax=ax_holdnum, color=colors_hold[: len(mix_cols_1)], linewidth=1.5)
        labels_1 = [f"{name} ({mean_map_1[c]:.2f})" for name, c in zip(["持股数量", "市值加权排名"], mix_cols_1)]
        ax_holdnum.legend(labels_1, fontsize=STYLE["font_size"]["small"], framealpha=0.8)
    beautify_axis(ax_holdnum, "持股数量 / 市值加权排名", fontsize=STYLE["font_size"]["subtitle"])
    ax_holdnum.tick_params(axis="x", labelrotation=30)

    # 4. 次图：风格偏离
    ax_style = fig.add_subplot(gs[2, 0])
    if not hold_style[style_cols].empty:
        colors = plt.cm.tab10(np.linspace(0, 1, len(style_cols)))
        hold_style[style_cols].plot(ax=ax_style, color=colors, linewidth=1.5, alpha=0.8)
    beautify_axis(ax_style, "风格偏离", fontsize=STYLE["font_size"]["subtitle"])
    ax_style.tick_params(axis="x", labelrotation=30)
    ax_style.legend([f"{i[8:]}" for i in style_cols], ncol=2, fontsize=STYLE["font_size"]["small"], loc="upper left", framealpha=0.8)

    # 5. 次图：换手率
    ax_turnover = fig.add_subplot(gs[2, 1])
    mix_cols_2 = [c for c in [mem_hold_col, turnover_col] if c in hold_style.columns]
    if mix_cols_2:
        mean_map_2 = {c: hold_style[c].mean() for c in mix_cols_2}
        colors_turn = [STYLE["colors"]["strategy"], STYLE["colors"]["benchmark"]]
        hold_style[mix_cols_2].plot(ax=ax_turnover, color=colors_turn[: len(mix_cols_2)], linewidth=1.5)
        labels_2 = [f"{name} ({mean_map_2[c]:.2f})" for name, c in zip(["成分股占比", "换手率"], mix_cols_2)]
        ax_turnover.legend(labels_2, fontsize=STYLE["font_size"]["small"], framealpha=0.8)
    beautify_axis(ax_turnover, "成分股占比 / 换手率", fontsize=STYLE["font_size"]["subtitle"])
    ax_turnover.tick_params(axis="x", labelrotation=30)

    # --- 右侧：数据表格区域 ---
    table_x = 0.68
    table_width = 0.28
    current_y = 0.92

    # 表格 1：综合绩效指标
    abs_keys = ["年化收益", "年化波动", "夏普比率", "累计收益", "最大回撤", "平均回撤", "胜率(天)"]
    rel_keys = ["超额年化收益", "超额年化波动", "信息比率", "超额累计收益", "超额最大回撤", "超额平均回撤", "超额胜率(天)"]

    metrics_data = []
    metrics_data.append(["绝对指标", "数值", "相对指标", "数值"])  # 表头

    for ak, rk in zip(abs_keys, rel_keys):
        av = info[ak]
        rv = info[rk]
        av_str = f"{av*100:.2f}%" if "比率" not in ak else f"{av:.4f}"
        rv_str = f"{rv*100:.2f}%" if "比率" not in rk else f"{rv:.4f}"
        metrics_data.append([ak, av_str, rk, rv_str])

    current_y = draw_table_block(fig, table_x, current_y, table_width, "综合绩效指标", metrics_data, col_ratios=[0.25, 0.25, 0.25, 0.25])

    current_y -= 0.05

    # 表格 2：逐年表现
    annual_metrics = ["超额收益", "超额波动", "超额最大回撤", "信息比率"]
    years = sorted(info["逐年超额年化收益"].keys())

    annual_data = []
    annual_data.append(["年份"] + annual_metrics)  # 表头

    for year in years:
        row = [str(int(year))]
        v1 = info["逐年超额年化收益"][year]
        v2 = info["逐年超额年化波动"][year]
        v3 = info["逐年超额最大回撤"][year]
        v4 = info["逐年信息比率"][year]
        row.extend([f"{v1*100:.1f}%", f"{v2*100:.1f}%", f"{v3*100:.1f}%", f"{v4:.2f}"])
        annual_data.append(row)

    current_y = draw_table_block(
        fig,
        table_x,
        current_y,
        table_width,
        "分年度超额表现",
        annual_data,
        col_ratios=[0.15, 0.22, 0.22, 0.22, 0.19],
    )

    current_y -= 0.05

    # 表格 3：回测参数配置
    if config.APM_MODE:
        # 准备左侧数据
        parameters_left = ["行业限制(早盘)", "行业限制(午盘)", "市值限制", "风格限制", "成分股持仓限制"]
        param_vals_left = [config.CITIC_LIMIT, config.CITIC_LIMIT_NOON, config.CMVG_LIMIT, config.OTHER_LIMIT, config.MEM_HOLD]

        # 准备右侧数据
        parameters_right = ["换手率限制(早盘)", "换手率限制(午盘)", "个股持仓限制", "个股买入比例限制", ""]
        param_vals_right = [config.TURN_MAX, config.TURN_MAX_NOON, config.STK_HOLD_LIMIT, config.STK_BUY_R, None]
    else:
        # 准备左侧数据
        parameters_left = ["行业限制", "市值限制", "风格限制", "成分股持仓限制"]
        param_vals_left = [config.CITIC_LIMIT, config.CMVG_LIMIT, config.OTHER_LIMIT, config.MEM_HOLD]

        # 准备右侧数据
        parameters_right = ["换手率限制", "个股持仓限制", "个股买入比例限制", ""]
        param_vals_right = [config.TURN_MAX, config.STK_HOLD_LIMIT, config.STK_BUY_R, None]

    param_data = []
    param_data.append(["限制参数", "参数值", "限制参数", "参数值"])  # 表头

    # 组合数据，还原之前的并行结构
    max_len = max(len(parameters_left), len(parameters_right))
    for i in range(max_len):
        # 左侧
        if i < len(parameters_left):
            l_k = parameters_left[i]
            l_v = f"{param_vals_left[i]:.4f}"
        else:
            l_k, l_v = "", ""

        # 右侧
        if i < len(parameters_right) and parameters_right[i]:
            r_k = parameters_right[i]
            val = param_vals_right[i]
            r_v = f"{val:.4f}" if val is not None else ""
        else:
            r_k, r_v = "", ""

        param_data.append([l_k, l_v, r_k, r_v])

    current_y = draw_table_block(
        fig,
        table_x,
        current_y,
        table_width,
        f"策略回测参数 (trade_support{config.TRADE_SUPPORT})",
        param_data,
        col_ratios=[0.25, 0.25, 0.25, 0.25],
    )

    # 页脚
    fig.text(0.95, 0.01, f"生成时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", ha="right", fontsize=10, color="#999999")

    # ==========================================
    # 4. 保存
    # ==========================================
    if config.AFTERNOON_START:
        file_name_suffix = "_afternoon"
    elif config.CALL_START:
        file_name_suffix = "_call"
    else:
        file_name_suffix = ""

    if config.STRATEGY == "solve":
        png_path = (
            f"{config.RESULT_PATH}/{strategy}" + file_name_suffix + f"_trade_support{config.TRADE_SUPPORT}.png"
            if strategy
            else f"{config.RESULT_PATH}/strategy" + file_name_suffix + f"_trade_support{config.TRADE_SUPPORT}.png"
        )
    else:
        png_path = (
            f"{config.RESULT_PATH}/{config.STRATEGY_NAME}/{strategy}" + file_name_suffix + f"_topn.png"
            if strategy
            else f"{config.RESULT_PATH}/{config.STRATEGY_NAME}/strategy" + file_name_suffix + f"_topn.png"
        )

    fig.savefig(png_path, format="png", bbox_inches="tight", pad_inches=0.2, dpi=150)
    plt.close(fig)
    print(f"回测结果 PNG 已保存至: {png_path}")
