import pandas as pd
import numpy as np


def max_drawdown_streak(series):
    """
    Calculate the maximum number of continuous loss days in a series.
    """
    streak = max_streak = 0
    for val in series:
        if val < 0:
            streak += 1
            if streak > max_streak:
                max_streak = streak
        else:
            streak = 0
    return max_streak


def analyse(nv):
    """
    Analyse the backtest results and calculate performance metrics.

    Parameters
    ----------
    nv : pd.DataFrame
        Must contain 'strategy' and 'zs' columns.

    Returns
    -------
    info : pd.Series
        Series containing backtest metrics.
    nv_df : pd.DataFrame
        DataFrame containing strategy and benchmark.
    rel_nv : pd.Series
        Series containing cumulative excess returns.
    """
    # 策略收益
    ret = nv["strategy"].pct_change().dropna()
    mean_ret = ret.mean() * 250
    std = ret.std() * np.sqrt(250)
    sharpe = mean_ret / std
    dd = nv["strategy"].cummax() - nv["strategy"]
    max_dd = dd.max()
    cum_return = nv["strategy"].iloc[-1] / nv["strategy"].iloc[0] - 1
    win_rate = (ret > 0).mean()
    max_loss_streak = max_drawdown_streak(ret)
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

    # 汇总 info
    info = pd.Series(
        {
            "年化收益": mean_ret,
            "年化波动": std,
            "夏普比率": sharpe,
            "累计收益": cum_return,
            "最大回撤": max_dd,
            "最大连续亏损天数": max_loss_streak,
            "胜率(天)": win_rate,
            "Calmar比率": calmar,
            "超额年化收益": ex_ret,
            "超额年化波动": ex_std,
            "信息比率": ir,
            "超额最大回撤": ex_max_dd,
        }
    )

    nv_df = pd.concat([nv["strategy"], nv["zs"]], axis=1)
    return info, nv_df, rel_nv
