import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plt.rcParams.update({"font.sans-serif": ["WenQuanYi Micro Hei"], "axes.unicode_minus": False, "font.size": 12})


def analyse(nv, plotting=False, strategy=None, scores_path=None):
    # 策略收益
    ret = nv["strategy"].pct_change().dropna()
    mean_ret = ret.mean() * 250
    std = ret.std() * np.sqrt(250)
    sharpe = mean_ret / std
    dd = nv["strategy"].cummax() - nv["strategy"]
    max_dd = dd.max()
    cum_return = nv["strategy"].iloc[-1] / nv["strategy"].iloc[0] - 1
    win_rate = (ret > 0).mean()

    # 最大连续亏损天数
    def max_drawdown_streak(series):
        streak = max_streak = 0
        for val in series:
            if val < 0:
                streak += 1
                if streak > max_streak:
                    max_streak = streak
            else:
                streak = 0
        return max_streak

    max_loss_streak = max_drawdown_streak(ret)

    # Calmar比率
    calmar = mean_ret / max_dd if max_dd != 0 else np.nan

    # 基准收益
    zs_ret = nv["zs"].pct_change().dropna()
    excess = ret - zs_ret
    rel_nv = 1 + excess.cumsum()
    ex_ret = excess.mean() * 250
    ex_std = excess.std() * np.sqrt(250)
    ir = ex_ret / ex_std
    rel_dd = rel_nv.cummax() - rel_nv
    ex_max_dd = rel_dd.max()
    ir2 = ex_ret / ex_max_dd if ex_max_dd != 0 else np.nan

    # info汇总
    info = pd.Series(
        {
            "策略年化收益": mean_ret,
            "策略年化波动": std,
            "策略夏普": sharpe,
            "策略累计收益": cum_return,
            "策略最大回撤": max_dd,
            "策略最大连续亏损天数": max_loss_streak,
            "胜率(天)": win_rate,
            "Calmar比率": calmar,
            "超额年化收益": ex_ret,
            "超额年化波动": ex_std,
            "信息比率": ir,
            "超额最大回撤": ex_max_dd,
            "收益回撤比": ir2,
        }
    )

    if plotting:
        # 主图数据
        nv_df = pd.concat([nv["strategy"], nv["zs"], rel_nv], axis=1, keys=["策略净值", "基准指数", "超额净值"]).fillna(1)

        fig, ax = plt.subplots(figsize=(16, 6))
        plt.subplots_adjust(right=0.65)
        nv_df.plot(ax=ax, grid=True, title=f"基于{strategy}的策略回测结果")

        # 构造右侧文本
        text_keys = []
        text_vals = []
        if scores_path is not None:
            text_keys.append("SCORES_PATH")
            text_vals.append(str(scores_path))
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

        # 左右两列分别绘制
        y_pos = np.linspace(0.95, 0.05, len(text_keys))
        for y, k, v in zip(y_pos, text_keys, text_vals):
            fig.text(0.60, y, k, fontsize=12, va="center", ha="left", family="WenQuanYi Micro Hei")
            fig.text(0.88, y, v, fontsize=12, va="center", ha="right", family="WenQuanYi Micro Hei")

        # 保存 PDF
        pdf_path = f"/home/user0/results/backtests/{strategy}.pdf" if strategy else "/home/user0/results/backtests/strategy.pdf"
        fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
        plt.close(fig)
        print(f"图表已保存: {pdf_path}")

    return info, pd.concat([nv["strategy"], nv["zs"]], axis=1), rel_nv
