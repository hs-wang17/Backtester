import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis


def max_drawdown_streak(series):
    """最大连续亏损天数"""
    streak = max_streak = 0
    for val in series:
        if val < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def drawdown_duration(nv):
    """最长回撤持续时间"""
    dd = nv.cummax() - nv
    duration = 0
    max_duration = 0
    for d in dd:
        if d > 0:
            duration += 1
            max_duration = max(max_duration, duration)
        else:
            duration = 0
    return max_duration


def hurst_exponent(series):
    """计算赫斯特指数"""
    ts = np.log(series.dropna())
    lags = range(2, 100)
    tau = [np.sqrt(np.std(ts.diff(lag))) for lag in lags]
    tau = np.array(tau)
    if np.any(tau <= 0):
        return np.nan
    poly = np.polyfit(np.log(lags), np.log(tau), 1)
    return poly[0]


def analyse(nv):
    """
    Backtest Performance Analyzer (Enhanced Version)
    ------------------------------------------------
    输入：
        nv: DataFrame 必须包含 'strategy' 和 'zs'
    输出：
        info: pd.Series     - 策略指标
        nv_df: pd.DataFrame - 策略与基准净值
        rel_nv: pd.Series   - 超额净值曲线
    """

    ret = nv["strategy"].pct_change().dropna()
    zs_ret = nv["zs"].pct_change().dropna()
    excess = ret - zs_ret
    mean_ret = ret.mean() * 250
    std = ret.std() * np.sqrt(250)
    sharpe = mean_ret / std if std > 0 else np.nan
    cum_return = nv["strategy"].iloc[-1] / nv["strategy"].iloc[0] - 1
    max_loss_streak = max_drawdown_streak(ret)
    win_rate = (ret > 0).mean()
    dd = nv["strategy"].cummax() - nv["strategy"]
    max_dd = dd.max()
    calmar = mean_ret / max_dd if max_dd > 0 else np.nan
    skewness = skew(ret)
    kurt = kurtosis(ret)
    downside = ret[ret < 0]
    downside_vol = downside.std() * np.sqrt(250)
    sortino = mean_ret / downside_vol if downside_vol > 0 else np.nan
    VaR_95 = np.percentile(ret, 5)
    CVaR_95 = ret[ret <= VaR_95].mean()
    rel_nv = 1 + excess.cumsum()
    ex_ret = excess.mean() * 250
    ex_std = excess.std() * np.sqrt(250)
    ir = ex_ret / ex_std if ex_std > 0 else np.nan
    rel_dd = rel_nv.cummax() - rel_nv
    ex_max_dd = rel_dd.max()
    tracking_error = excess.std() * np.sqrt(250)
    cov = np.cov(ret, zs_ret)[0][1]
    market_var = zs_ret.var()
    beta = cov / market_var if market_var > 0 else np.nan
    alpha = mean_ret - beta * (zs_ret.mean() * 250) if beta == beta else np.nan
    treynor = mean_ret / beta if beta and beta != 0 else np.nan
    threshold = 0.0
    omega = (ret[ret > threshold] - threshold).sum() / (-ret[ret < threshold] + threshold).sum()
    drawdown_dur = drawdown_duration(nv["strategy"])
    avg_dd = dd[dd > 0].mean()
    hurst = hurst_exponent(nv["strategy"])
    log_nv = np.log(nv["strategy"])
    slope, intercept = np.polyfit(range(len(log_nv)), log_nv, 1)
    fitted = slope * np.arange(len(log_nv)) + intercept
    r2 = 1 - np.sum((log_nv - fitted) ** 2) / np.sum((log_nv - log_nv.mean()) ** 2)

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
            "偏度": skewness,
            "峰度": kurt,
            "下行波动率": downside_vol,
            "Sortino比率": sortino,
            "VaR(95%)": VaR_95,
            "CVaR(95%)": CVaR_95,
            "平均回撤": avg_dd,
            "最长回撤持续天数": drawdown_dur,
            "超额年化收益": ex_ret,
            "超额年化波动": ex_std,
            "信息比率": ir,
            "跟踪误差": tracking_error,
            "超额最大回撤": ex_max_dd,
            "Beta": beta,
            "Alpha": alpha,
            "Treynor比率": treynor,
            "Omega比率": omega,
            "Hurst指数": hurst,
            "收益稳定性(R²)": r2,
        }
    )

    nv_df = pd.concat([nv["strategy"], nv["zs"]], axis=1)
    return info, nv_df, rel_nv
