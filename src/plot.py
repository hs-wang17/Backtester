import matplotlib.pyplot as plt
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio

plt.rcParams.update({"font.sans-serif": ["WenQuanYi Micro Hei"], "axes.unicode_minus": False, "font.size": 12})


def plot(nv_df, rel_nv, info, strategy=None, scores_path=None):
    """
    Plot strategy performance.
    Supports both Matplotlib png and Plotly interactive HTML.
    """
    # ---------------- Matplotlib png ----------------
    plot_df = nv_df.copy()
    plot_df["超额净值"] = rel_nv
    plot_df.fillna(1, inplace=True)

    fig, ax = plt.subplots(figsize=(16, 6))
    plt.subplots_adjust(right=0.65)
    plot_df.plot(ax=ax, grid=True, title=f"基于{strategy}的策略回测结果")

    # 添加右侧文本
    text_keys = []
    text_vals = []
    text_keys.append("策略回测指标")
    text_vals.append("")
    for k, v in info.items():
        text_keys.append(k)
        if isinstance(v, (float, np.floating)):
            if "收益" in k or "回撤" in k or "波动" in k:
                text_vals.append(f"{v*100:7.2f}%")
            else:
                text_vals.append(f"{v:7.4f}")
        else:
            text_vals.append(str(v))

    y_pos = np.linspace(0.75, 0.25, len(text_keys))
    for y, k, v in zip(y_pos, text_keys, text_vals):
        fig.text(0.70, y, k, fontsize=12, va="center", ha="left", family="WenQuanYi Micro Hei")
        fig.text(0.90, y, v, fontsize=12, va="center", ha="right", family="WenQuanYi Micro Hei")

    png_path = f"/home/user0/results/backtests/{strategy}.png" if strategy else "/home/user0/results/backtests/strategy.png"
    fig.savefig(png_path, format="png", bbox_inches="tight")
    plt.close(fig)
    print(f"PNG 已保存: {png_path}")

    # ---------------- Plotly 可交互 HTML ----------------
    html_path = f"/home/user0/results/backtests/{strategy}.html" if strategy else "/home/user0/results/backtests/strategy.html"
    fig_plotly = go.Figure()

    for col in plot_df.columns:
        fig_plotly.add_trace(go.Scatter(x=plot_df.index, y=plot_df[col], mode="lines", name=col))

    # 右侧指标注释
    annotation_text = "<br>".join([f"{k}: {v}" for k, v in zip(text_keys, text_vals)])
    fig_plotly.add_annotation(
        xref="paper", yref="paper", x=1.05, y=0.5, text=annotation_text, showarrow=False, align="left", font=dict(family="WenQuanYi Micro Hei", size=12)
    )

    fig_plotly.update_layout(
        title=f"基于{strategy}的策略回测结果", xaxis_title="日期", yaxis_title="净值", autosize=False, width=1000, height=600, margin=dict(r=1000)
    )

    pio.write_html(fig_plotly, file=html_path, auto_open=False)
    print(f"可交互 HTML 已保存: {html_path}")
