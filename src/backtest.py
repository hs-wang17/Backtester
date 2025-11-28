import os
from tqdm import tqdm
import pandas as pd
import numpy as np
import src.config as config
from src.utils import get_daily_price, get_daily_support
from src.portfolio_optimizer import solve_problem
from src.account import account
from src.analysis import analyse
from src.plot import plot


def load_daily_data(name):
    return pd.read_feather(os.path.join(config.DAILY_DATA_PATH, f"{name}.feather"))


def run_backtest():
    high_limit = load_daily_data("stk_ztprice").replace(0, np.nan).ffill()
    low_limit = load_daily_data("stk_dtprice").replace(0, np.nan).ffill()
    pre_close = load_daily_data("stk_preclose").replace(0, np.nan).ffill()
    adj_factor = load_daily_data("stk_adjfactor").replace(0, np.nan).ffill()
    close = load_daily_data("stk_close").replace(0, np.nan).ffill()
    last_zt_df = (close == high_limit).shift(1).fillna(False).astype(int)
    # stop buying stocks at 90% of limit up and limit down prices
    upper_price = pre_close + 0.9 * (high_limit - pre_close)
    lower_price = pre_close + 0.9 * (low_limit - pre_close)
    adj = adj_factor / adj_factor.shift(1)
    zs_day = load_daily_data("idx_close")[config.IDX_NAME_CN].dropna()
    vwap_df = pd.read_feather(os.path.join(config.DATA_PATH, "vwap.fea"))
    scores = pd.read_csv(config.SCORES_PATH, index_col=0).T.sort_index().shift(1).dropna(how="all")
    scores.columns = scores.columns.astype(str).str.zfill(6)
    scores = scores[scores.columns[scores.columns.str[0].isin(["0", "3", "6"])]]
    scores.index = scores.index.astype(str)
    date_list = sorted(scores.index.tolist())

    """
    Run backtest on the given dataset.

    Parameters
    ----------
    INITIAL_MONEY : float
        The initial amount of money used for backtesting.
    STK_HOLD_LIMIT : float
        The limit of holding stock.
    STK_BUY_R : float
        The rate of buying stock.
    TURN_MAX : float
        The maximum rate of buying stock.
    CITIC_LIMIT : float
        The limit of CITIC.
    CMVG_LIMIT : float
        The limit of CMVG.
    OTHER_LIMIT : float
        The limit of other style metrics.

    Returns
    -------
    A dictionary containing the backtesting result.
    """
    s = account(config.INITIAL_MONEY)
    s.cal_total()

    act_s = {}
    cash_s = {}
    buy_s = {}
    sell_s = {}
    hold_df_dict = {}  # save each day's holding
    trade_df_dict = {}  # save each day's trade
    hold_style_dict = {}  # save each day's holding style diff

    for date in tqdm(date_list, desc="Backtesting"):
        # get daily data
        td_open, td_close, td_preclose, td_adj, td_score, td_upper, td_lower, last_zt = get_daily_price(
            str(date), vwap_df, close, pre_close, adj, scores, upper_price, lower_price, last_zt_df
        )
        # get daily support data
        td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support(str(date))
        # get today's tradable stocks
        code_list = pd.concat([td_upper, td_lower, td_close, td_open], axis=1).dropna(how="any").index.tolist()  # 今日可交易
        code_list = [x for x in code_list if (x in sub_code_list) & (x[0] != "4") & (x[0] != "8")]  # 剔除新股,ST股票,北交所股票
        # calculate stock permission
        stk_perm = (td_mem + td_mem.max()) * (config.STK_HOLD_LIMIT / (2 * td_mem.max()))
        zt_codes = last_zt[last_zt == 1].index.tolist()

        # refresh before market open
        act = s.refresh_open(td_upper, td_lower, td_preclose.to_dict(), td_adj)
        stk_buy_amt = pd.Series([config.STK_BUY_R * act] * len(code_list), index=code_list)
        for code in zt_codes:
            if code in code_list:
                stk_buy_amt[code] = 0

        # get last holding
        if len(s.hold_dict) == 0:
            last_hold = td_mem.reindex(code_list).fillna(0) * act  # pd.Series([0]*len(code_list),index=code_list)
        else:
            last_hold = hold_df["amt"].reindex(code_list).fillna(0) + s.cash * td_mem.reindex(code_list).fillna(0)
            st_hold = hold_df["amt"].reindex([x for x in hold_df.index if (x not in code_list)])  #

        try:
            # solve problem using cvxpy
            tgt_hold = solve_problem(
                code_list=code_list,
                x_last=last_hold,
                score=(td_score - td_score.min()) / (td_score.max() - td_score.min()),
                stk_low=((td_mem - stk_perm).clip(0) * act).clip(upper=last_hold + stk_buy_amt, lower=last_hold - 2 * config.STK_BUY_R * act),
                stk_high=((td_mem + stk_perm) * act).clip(upper=last_hold + stk_buy_amt, lower=last_hold - 2 * config.STK_BUY_R * act),
                tot_amt=1.01 * act,
                sell_max=act * config.TURN_MAX,
                td_mem=(td_mem > 0).astype(int),
                td_mem_amt=config.MEM_HOLD * act,
                td_ind=td_citic,
                td_ind_up=((zz_citic + config.CITIC_LIMIT) * act),
                td_ind_down=((zz_citic - config.CITIC_LIMIT) * act),
                td_cmvg=td_cmvg,
                td_cmvg_up=((zz_cmvg + config.CMVG_LIMIT) * act),
                td_cmvg_down=((zz_cmvg - config.CMVG_LIMIT) * act),
                td_style=style_fac,
                style_up=((zz_style + config.OTHER_LIMIT) * act),
                style_down=((zz_style - config.OTHER_LIMIT) * act),
                solver="SCIPY",  # ['ECOS', 'SCS', 'OSQP', 'SCIPY']
            )
            if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
                raise ValueError("no target hold")

        except Exception as e:
            print("Error:", e)
            # if failed, move toward index
            print(date, "no target hold, move toward index")
            tgt_hold = last_hold * (1 - config.TURN_MAX) + td_mem.reindex(code_list).fillna(0) * config.TURN_MAX * s.total_account

        # get to buy and to sell series
        sort_index = td_score.sort_values(ascending=False).index
        if len(s.hold_dict) == 0:  # first day build position, follow index
            to_buy_s = round(tgt_hold).replace(0, np.nan).reindex(sort_index).dropna()  # sort by score
            to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
            to_sell_s = pd.Series(dtype=float)
        else:
            last_hold = hold_df["amt"].reindex(code_list).fillna(0)
            to_trade_s = round(tgt_hold - last_hold).replace(0, np.nan)
            to_buy_s = to_trade_s[to_trade_s > 0].reindex(sort_index).dropna()  # sort by score
            to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
            to_sell_s = pd.concat([st_hold, -to_trade_s[to_trade_s < 0].reindex(sort_index).dropna().iloc[::-1]])  # sort by score
            to_sell_s = round(to_sell_s / td_preclose.reindex(to_sell_s.index))  # transform amount to quantity

        max_buy = s.cash

        s.fresh_price(td_open.to_dict())
        buy_amt, sell_amt = s.daily_trade(max_buy, to_buy_s, to_sell_s)
        s.fresh_price(td_close.to_dict())

        act_s[date] = s.cal_total()
        cash_s[date] = s.cash
        buy_s[date] = buy_amt
        sell_s[date] = sell_amt
        hold_df, trade_df = s.close_today()
        hold_df_dict[date] = hold_df
        trade_df_dict[date] = trade_df

        hold_weight = hold_df["amt"] / hold_df["amt"].sum()
        td_citic_diff = td_citic.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_citic  # 行业偏离
        td_cmvg_diff = td_cmvg.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_cmvg  # 市值偏离
        td_style_diff = style_fac.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_style  # 风格偏离
        td_MEM_HOLD = hold_weight.reindex(td_mem[td_mem > 0].index).fillna(0).sum()
        td_hold_num = len(hold_weight)
        td_turnover = (buy_s[date] + sell_s[date]) / act_s[date] * 0.5
        td_diff = pd.concat([td_citic_diff, td_cmvg_diff, td_style_diff])
        td_diff["idx_hold"] = td_MEM_HOLD
        td_diff["hold_num"] = td_hold_num
        td_diff["turnover"] = td_turnover
        hold_style_dict[date] = td_diff

    # aggregate results
    total_s = pd.concat([pd.Series(act_s), pd.Series(cash_s), pd.Series(buy_s), pd.Series(sell_s)], axis=1, keys=["total_act", "cash", "buy_amt", "sell_amt"])
    nv = pd.concat([zs_day.reindex(total_s.index), total_s["total_act"]], axis=1, keys=["zs", "strategy"])
    nv = nv / nv.iloc[0]
    hold_style = pd.DataFrame(hold_style_dict).T

    # combine all daily hold_df into a single DataFrame with date information
    all_hold_df = pd.DataFrame()
    for date, daily_hold_df in hold_df_dict.items():
        daily_hold_df_copy = daily_hold_df.copy()
        daily_hold_df_copy["date"] = date
        all_hold_df = pd.concat([all_hold_df, daily_hold_df_copy], ignore_index=False)

    all_hold_df.to_csv(config.HOLD_DF_PATH + config.STRATEGY_NAME + "_hold_df.csv", index_label="code")

    info, nv_df, rel_nv = analyse(nv)
    plot(nv_df, rel_nv, info, strategy=config.STRATEGY_NAME, scores_path=config.SCORES_PATH, hold_style=hold_style)

    return {"total_act_s": total_s, "nv": nv, "info": info, "hold_style": hold_style}
