import pandas as pd
import numpy as np
import src.config as config


def analyse(net_value):
    """
    Backtest Performance Analyzer
    ------------------------------------------------
    输入：
        net_value: DataFrame 必须包含 'strategy' 和 'zs'
    输出：
        info: pd.Series                     - 策略指标
        net_value_df: pd.DataFrame          - 策略与基准净值
        relative_net_value: pd.Series       - 超额净值曲线
    """
    # 绝对指标
    abs_ret = net_value["strategy"].pct_change().dropna()
    abs_mean_ret = abs_ret.mean() * 250
    abs_std_ret = abs_ret.std() * np.sqrt(250)
    abs_sharpe = abs_mean_ret / abs_std_ret if abs_std_ret > 0 else np.nan  # 夏普指数
    abs_cum_ret = abs_ret.cumsum()
    abs_dd = abs_cum_ret.cummax() - abs_cum_ret
    abs_max_dd = abs_dd.max()
    abs_mean_dd = abs_dd[abs_dd > 0].mean()
    abs_win_rate = (abs_ret > 0).mean()

    # 相对指标
    zs_ret = net_value["zs"].pct_change().dropna()
    rel_ret = abs_ret - zs_ret
    if config.REMOVE_ABNORMAL:
        rel_ret.loc["20240130":"20240219"] = 0.0
    rel_mean_ret = rel_ret.mean() * 250
    rel_std_ret = rel_ret.std() * np.sqrt(250)
    rel_sharpe = rel_mean_ret / rel_std_ret if rel_std_ret > 0 else np.nan  # 信息比率
    rel_cum_ret = rel_ret.cumsum()
    rel_dd = rel_cum_ret.cummax() - rel_cum_ret
    if config.REMOVE_ABNORMAL:
        rel_dd.loc["20240130":"20240219"] = 0.0
    rel_max_dd = rel_dd.max()
    rel_mean_dd = rel_dd[rel_dd > 0].mean()
    rel_win_rate = (rel_ret > 0).mean()

    # 部分相对指标（分年度）
    df = pd.concat([abs_ret, zs_ret], axis=1)
    df.columns = ["strategy", "zs"]
    df["excess"] = df["strategy"] - df["zs"]
    df.index = pd.to_datetime(df.index.astype(str), format="%Y%m%d")
    ex_ret, ex_std, ex_max_dd, ir = {}, {}, {}, {}
    for year, y_ret in df["excess"].groupby(df.index.year):
        mean_y = y_ret.mean() * 250
        std_y = y_ret.std() * np.sqrt(250)
        ex_ret[year] = mean_y
        ex_std[year] = std_y
        ex_max_dd[year] = (y_ret.cumsum().cummax() - y_ret.cumsum()).max()
        ir[year] = mean_y / std_y if std_y > 0 else np.nan

    info = pd.Series(
        {
            "年化收益": abs_mean_ret,
            "年化波动": abs_std_ret,
            "夏普比率": abs_sharpe,
            "累计收益": abs_cum_ret.iloc[-1],
            "最大回撤": abs_max_dd,
            "平均回撤": abs_mean_dd,
            "胜率(天)": abs_win_rate,
            "超额年化收益": rel_mean_ret,
            "超额年化波动": rel_std_ret,
            "信息比率": rel_sharpe,
            "超额累计收益": rel_cum_ret.iloc[-1],
            "超额最大回撤": rel_max_dd,
            "超额平均回撤": rel_mean_dd,
            "超额胜率(天)": rel_win_rate,
            "逐年超额年化收益": ex_ret,
            "逐年超额年化波动": ex_std,
            "逐年超额最大回撤": ex_max_dd,
            "逐年信息比率": ir,
        }
    )

    net_value = pd.concat([net_value["strategy"], net_value["zs"]], axis=1)
    relative_net_value = 1 + rel_cum_ret

    return info, net_value, relative_net_value
