# -*- coding: utf-8 -*-
# 代码路径与run.py同级
# load_target_weights函数的weights_dir参数需要根据实际情况修改(/home/user0/share/van/stf_stk_hold)
# trade_dates是已经统计好的交易日(因为上一个目录中的交易日不全，需要fill)，放在(/home/user0/share/haris/trade_date.fea)
# 其他路径与config.py相同

from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import src.config as config
from src.utils import get_daily_price, get_daily_support
from src.account import account
from src.analysis import analyse
from src.plot import plot


def load_target_weights(date, weights_dir="/home/user0/temp/Vanmine_20251120"):
    """
    加载指定日期的目标权重文件

    Parameters
    ----------
    date : str
        日期字符串，格式如 '20241011'
    weights_dir : str
        权重文件目录路径

    Returns
    -------
    pd.Series or None
        返回股票代码到权重的映射，如果没有该日期的文件则返回None
    """
    file_path = os.path.join(weights_dir, f"{date}.csv")
    if not os.path.exists(file_path):
        return None

    try:
        df = pd.read_csv(file_path)
        if "stk_code" in df.columns and "WEIGHT" in df.columns:
            # 将股票代码转换为6位数字字符串格式
            df["stk_code"] = df["stk_code"].astype(str).str.zfill(6)
            # 设置stk_code为索引，WEIGHT为值
            weights = df.set_index("stk_code")["WEIGHT"]
            return weights
        else:
            print(f"Warning: {file_path} 文件格式不正确，需要包含stk_code和WEIGHT列")
            return None
    except Exception as e:
        print(f"Warning: 读取权重文件 {file_path} 失败: {e}")
        return None


def load_daily_data(name):
    return pd.read_feather(os.path.join(config.DAILY_DATA_PATH, f"{name}.feather"))


def run_backtest_with_weights():
    high_limit = load_daily_data("stk_ztprice").replace(0, np.nan).ffill()
    low_limit = load_daily_data("stk_dtprice").replace(0, np.nan).ffill()
    pre_close = load_daily_data("stk_preclose").replace(0, np.nan).ffill()
    adj_factor = load_daily_data("stk_adjfactor").replace(0, np.nan).ffill()
    close = load_daily_data("stk_close").replace(0, np.nan).ffill()
    last_zt_df = (close == high_limit).shift(1).fillna(False).astype(int)
    upper_price = pre_close + 0.9 * (high_limit - pre_close)
    lower_price = pre_close + 0.9 * (low_limit - pre_close)
    adj = adj_factor / adj_factor.shift(1)
    zs_day = load_daily_data("idx_close")[config.IDX_NAME2].dropna()
    vwap_df = pd.read_feather(os.path.join(config.DATA_PATH, "vwap.fea"))
    """
    使用预计算权重的回测函数

    Returns
    -------
    dict
        包含回测结果的字典
    """
    s = account(config.INITIAL_MONEY)
    account_s = {}
    cash_s = {}
    buy_s = {}
    sell_s = {}
    hold_df_dict = {}
    trade_df_dict = {}
    hold_style_dict = {}

    # 记录上一个交易日的权重，用于判断是否需要调仓
    last_target_weights = None

    trade_dates = pd.read_feather("/home/user0/mydata/trade_date.fea")
    date_list = [x[0] for x in trade_dates.values.tolist() if x[0] >= "20241011" and x[0] <= "20251013"]

    for date in tqdm(date_list, desc="Backtesting with weights"):
        # 加载当日目标权重
        target_weights = load_target_weights(str(date))

        # 获取当日数据
        td_open, td_close, td_preclose, td_adj, td_score, td_upper, td_lower, last_zt = get_daily_price(
            str(date), vwap_df, close, pre_close, adj, target_weights, upper_price, lower_price, last_zt_df
        )
        td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support(str(date))

        # 获取可交易股票列表
        code_list = pd.concat([td_upper, td_lower, td_close, td_open], axis=1).dropna(how="any").index.tolist()
        code_list = [x for x in code_list if (x in sub_code_list) & (x[0] != "4") & (x[0] != "8")]  # 剔除新股,ST股票,北交所股票

        # 获取涨停股票
        zt_codes = last_zt[last_zt == 1].index.tolist()

        # 开盘前刷新参数
        account0 = s.refresh_open(td_upper, td_lower, td_preclose.to_dict(), td_adj)
        stk_buy_amt = pd.Series([config.STK_BUY_R * account0] * len(code_list), index=code_list)
        for code in zt_codes:
            if code in code_list:
                stk_buy_amt[code] = 0

        # 判断是否需要调仓
        need_rebalance = False
        if target_weights is not None:
            # 有新的权重文件，需要调仓
            need_rebalance = True
            last_target_weights = target_weights.copy()
            print(date, "需要调仓")
        else:
            # 没有新权重文件，但有历史权重，继续使用历史权重
            target_weights = last_target_weights
            need_rebalance = False  # 不调仓，保持现有持仓
            print(date, "不需要调仓")

        # 获取昨日持仓
        if len(s.hold_dict) == 0:
            last_hold = td_mem.reindex(code_list).fillna(0) * account0  # pd.Series([0]*len(code_list),index=code_list)
        else:
            last_hold = hold_df["amt"].reindex(code_list).fillna(0) + s.cash * td_mem.reindex(code_list).fillna(0)
            st_hold = hold_df["amt"].reindex([x for x in hold_df.index if (x not in code_list)])  #

        if need_rebalance:
            # 需要调仓：根据目标权重计算目标持仓金额
            # 过滤权重，只保留可交易的股票
            valid_weights = target_weights.reindex(code_list).fillna(0)

            # 归一化权重（确保权重和为1）
            if valid_weights.sum() > 0:
                valid_weights = valid_weights / valid_weights.sum()
            else:
                valid_weights = pd.Series(0, index=code_list)

            # 计算目标持仓金额
            tgt_hold = valid_weights * account0

            # 根据目标持仓，得到交易任务
            sort_index = td_score.reindex(valid_weights.index).fillna(0).sort_values(ascending=False).index

            if len(s.hold_dict) == 0:  # 第一天建仓
                to_buy_s = round(tgt_hold).replace(0, np.nan).reindex(sort_index).dropna()
                to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # 金额转换成量
                to_sell_s = pd.Series(dtype=float)
            else:
                to_trade_s = round(tgt_hold - last_hold).replace(0, np.nan)
                to_buy_s = to_trade_s[to_trade_s > 0].reindex(sort_index).dropna()
                to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # 金额转换成量
                to_sell_s = pd.concat([st_hold, -to_trade_s[to_trade_s < 0].reindex(sort_index).dropna().iloc[::-1]])
                to_sell_s = round(to_sell_s / td_preclose.reindex(to_sell_s.index))  # 金额转换成量
        else:
            # 不调仓：没有交易任务
            to_buy_s = pd.Series(dtype=float)
            to_sell_s = pd.Series(dtype=float)

        max_buy = s.cash

        s.fresh_price(td_open.to_dict())
        buy_amt, sell_amt = s.daily_trade(max_buy, to_buy_s, to_sell_s)
        s.fresh_price(td_close.to_dict())

        account_s[date] = s.cal_tot()
        cash_s[date] = s.cash
        buy_s[date] = buy_amt
        sell_s[date] = sell_amt
        hold_df, trade_df = s.close_today()
        hold_df_dict[date] = hold_df
        trade_df_dict[date] = trade_df

        # 计算持仓风格偏离
        hold_weight = hold_df["amt"] / hold_df["amt"].sum()
        td_citic_diff = td_citic.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_citic  # 行业偏离
        td_cmvg_diff = td_cmvg.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_cmvg  # 市值偏离
        td_style_diff = style_fac.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_style  # 风格偏离
        td_MEM_HOLD = hold_weight.reindex(td_mem[td_mem > 0].index).fillna(0).sum()
        td_hold_num = (hold_weight > 0).sum()
        td_turnover = (buy_s[date] + sell_s[date]) / account_s[date] * 0.5
        td_diff = pd.concat([td_citic_diff, td_cmvg_diff, td_style_diff])
        td_diff["idx_hold"] = td_MEM_HOLD
        td_diff["hold_num"] = td_hold_num
        td_diff["turnover"] = td_turnover
        hold_style_dict[date] = td_diff

    # 结果汇总
    tot_s = pd.concat(
        [pd.Series(account_s), pd.Series(cash_s), pd.Series(buy_s), pd.Series(sell_s)], axis=1, keys=["tot_account", "cash", "buy_amt", "sell_amt"]
    )
    nv = pd.concat([zs_day.reindex(tot_s.index), tot_s["tot_account"]], axis=1, keys=["zs", "strategy"])
    nv = nv / nv.iloc[0]
    # info, _, _ = analyse(nv, plotting=True, strategy=STRATEGY_NAME)
    hold_style = pd.DataFrame(hold_style_dict).T

    info, nv_df, rel_nv = analyse(nv)
    plot(nv_df, rel_nv, info, strategy="Vanmine_20251120", scores_path=config.SCORES_PATH, hold_style=hold_style)

    return {"tot_account_s": tot_s, "nv": nv, "info": info, "hold_style": hold_style}


if __name__ == "__main__":
    # 运行基于权重的回测
    results = run_backtest_with_weights()
