import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

import src.config as config
from src.account import account
from src.analysis import analyse
from src.plot import plot
from src.strategy import solve_strategy, solve_strategy_noon, topn_strategy, record_trade
from src.utils import get_daily_price_apm, get_daily_support5, get_daily_support7, get_daily_support_barra


def load_daily_data(name):
    return pd.read_feather(os.path.join(config.DAILY_DATA_PATH, f"{name}.feather"))


def run_backtest_apm():
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
    TURN_MAX_NOON : float
        The maximum rate of buying stock for noon trading.
    CITIC_LIMIT : float
        The limit of CITIC.
    CITIC_LIMIT_NOON : float
        The limit of CITIC for noon trading.
    CMVG_LIMIT : float
        The limit of CMVG.
    OTHER_LIMIT : float
        The limit of other style metrics.

    Returns
    -------
    A dictionary containing the backtesting result.
    """

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
    vwap_am_df = pd.read_feather(os.path.join(config.DATA_PATH, "vwap.fea"))
    vwap_pm_df = pd.read_feather(os.path.join(config.DATA_PATH, "vwap_noon.fea"))

    index_sets, col_sets = [], []

    scores_am = pd.read_csv(config.SCORES_PATH[0], index_col=0).T.sort_index().shift(1).dropna(how="all")
    scores_pm = pd.read_csv(config.NOON_SCORES_PATH[0], index_col=0).T.sort_index().dropna(how="all")
    scores_am.columns = scores_am.columns.astype(str).str.zfill(6)
    scores_pm.columns = scores_pm.columns.astype(str).str.zfill(6)
    scores_am = scores_am[scores_am.columns[scores_am.columns.str[0].isin(["0", "3", "6"])]]
    scores_pm = scores_pm[scores_pm.columns[scores_pm.columns.str[0].isin(["0", "3", "6"])]]
    scores_am.index = scores_am.index.astype(str)
    scores_pm.index = scores_pm.index.astype(str)
    index_sets = [set(scores_am.index), set(scores_pm.index)]
    col_sets = [set(scores_am.columns), set(scores_pm.columns)]

    common_dates = sorted(
        set.intersection(*index_sets) & set(vwap_am_df.index.astype(str)) & set(vwap_pm_df.index.astype(str))
    )  # apply date filter with vwap_df
    common_cols = sorted(set.intersection(*col_sets))
    scores_am = scores_am.loc[common_dates, common_cols]
    scores_pm = scores_pm.loc[common_dates, common_cols]
    date_list = common_dates[config.START_DATE_SHIFT :]  # apply start date shift

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
        td_open, td_noon_open, td_close, td_preclose, td_adj, td_score, td_score_noon, td_upper, td_lower, last_zt = get_daily_price_apm(
            str(date), vwap_am_df, vwap_pm_df, close, pre_close, adj, scores_am, scores_pm, upper_price, lower_price, last_zt_df
        )

        # get daily support data
        if config.TRADE_SUPPORT == 5:
            td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support5(str(date))
        elif config.TRADE_SUPPORT == 7:
            td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support7(str(date))
        else:
            td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support_barra(str(date))

        # get today's tradable and zt stocks
        code_list_all = pd.concat([td_upper, td_lower, td_close, td_open], axis=1).dropna(how="any").index.tolist()  # tradable stocks
        code_list = [
            x for x in code_list_all if (x in sub_code_list) and (x[0] not in ["4", "8"])
        ]  # tradable stocks except new/ST/BSE stocks
        zt_codes = last_zt[last_zt == 1].index.tolist()
        code_list_zt = [x for x in code_list if x not in zt_codes]  # remove zt stocks

        # calculate stk_perm
        stk_perm = (td_mem + td_mem.max()) * (config.STK_HOLD_LIMIT / (2 * td_mem.max()))

        """morning trading"""

        # refresh before market open
        act = s.refresh_open(td_upper, td_lower, td_preclose.to_dict(), td_adj)

        # strategy
        if config.STRATEGY == "solve":
            to_buy_s, to_sell_s = solve_strategy(
                s,
                act,
                code_list,
                code_list_all,
                zt_codes,
                td_score,
                td_mem,
                stk_perm,
                td_citic,
                zz_citic,
                td_cmvg,
                zz_cmvg,
                style_fac,
                zz_style,
                td_preclose,
            )
        elif config.STRATEGY == "topn":
            to_buy_s, to_sell_s = topn_strategy(s, act, code_list, code_list_all, code_list_zt, td_score, td_preclose)

        # execute trades
        hold_df, sellable_amt = record_trade(
            s, td_open, to_buy_s, to_sell_s, date, act_s, cash_s, buy_s, sell_s, hold_df_dict, trade_df_dict, "am", None
        )

        """noon trading"""

        # refresh before market open
        act = s.cal_total()

        # strategy
        if config.STRATEGY == "solve":
            to_buy_s, to_sell_s = solve_strategy_noon(
                s,
                act,
                sellable_amt,
                code_list,
                code_list_all,
                zt_codes,
                td_score_noon,
                td_mem,
                stk_perm,
                td_citic,
                zz_citic,
                td_cmvg,
                zz_cmvg,
                style_fac,
                zz_style,
                td_preclose,
            )
        elif config.STRATEGY == "topn":
            to_buy_s, to_sell_s = topn_strategy(s, act, code_list, code_list_all, code_list_zt, td_score_noon, td_preclose)

        # execute trades
        hold_df, sellable_amt = record_trade(
            s,
            td_noon_open,
            to_buy_s,
            to_sell_s,
            date,
            act_s,
            cash_s,
            buy_s,
            sell_s,
            hold_df_dict,
            trade_df_dict,
            "pm",
            td_close,
        )

        # calculate holding style difference
        hold_weight = hold_df["amt"] / hold_df["amt"].sum()
        td_citic_diff = td_citic.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_citic  # 行业偏离
        td_cmvg_diff = td_cmvg.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_cmvg  # 市值偏离
        td_style_diff = style_fac.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_style  # 风格偏离
        td_mem_hold = hold_weight.reindex(td_mem[td_mem > 0].index).fillna(0).sum()
        td_hold_num = len(hold_weight)
        td_turnover = (buy_s[date + "am"] + sell_s[date + "am"] + buy_s[date + "pm"] + sell_s[date + "pm"]) / act_s[date] * 0.5
        if isinstance(td_score, list):
            hold_weight_aligned = hold_weight.reindex(td_score[0].index).fillna(0)
            amt_weighted_rank = hold_weight_aligned @ td_score[0].rank(ascending=False)
        else:
            hold_weight_aligned = hold_weight.reindex(td_score.index).fillna(0)
            amt_weighted_rank = hold_weight_aligned @ td_score.rank(ascending=False)

        td_diff = pd.concat([td_citic_diff, td_cmvg_diff, td_style_diff])
        td_diff["mem_hold"] = td_mem_hold
        td_diff["hold_num"] = td_hold_num
        td_diff["turnover"] = td_turnover
        td_diff["amt_weighted_rank"] = amt_weighted_rank
        hold_style_dict[date] = td_diff

    # aggregate results
    total_s = pd.concat(
        [pd.Series(act_s), pd.Series(cash_s)],
        axis=1,
        keys=["total_act", "cash"],
    )
    nv = pd.concat([zs_day.reindex(total_s.index), total_s["total_act"]], axis=1, keys=["zs", "strategy"])
    nv = nv / nv.iloc[0]
    hold_style = pd.DataFrame(hold_style_dict).T
    info, nv, rel_nv = analyse(nv)

    if config.PLOT:
        # PLOT=True: use for backtest analysis and visualization, save daily holding data to a CSV file
        # combine all daily hold_df into a single DataFrame with date information
        all_hold_df = pd.DataFrame()
        for date, daily_hold_df in hold_df_dict.items():
            daily_hold_df_copy = daily_hold_df.copy()
            daily_hold_df_copy["date"] = date
            all_hold_df = pd.concat([all_hold_df, daily_hold_df_copy], ignore_index=False)

        if config.AFTERNOON_START:
            if config.STRATEGY == "solve":
                all_hold_df.to_csv(
                    config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_afternoon_trade_support{config.TRADE_SUPPORT}_hold_df.csv",
                    index_label="code",
                )
            elif config.STRATEGY == "topn":
                all_hold_df.to_csv(config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_afternoon_topn_hold_df.csv", index_label="code")
        else:
            if config.STRATEGY == "solve":
                all_hold_df.to_csv(
                    config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_trade_support{config.TRADE_SUPPORT}_hold_df.csv", index_label="code"
                )
            elif config.STRATEGY == "topn":
                all_hold_df.to_csv(config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_topn_hold_df.csv", index_label="code")
        plot(nv, rel_nv, info, strategy=config.STRATEGY_NAME, scores_path=config.SCORES_PATH, hold_style=hold_style)

    else:
        # PLOT=False: use for parameter optimization (efficient frontier), save results to a unified JSON file
        json_path = f"/home/haris/project/backtester/para_optimizer_ef/scores/{config.PARA_NAME}.json"

        new_entry = {
            "parameters": {
                "CITIC_LIMIT": config.CITIC_LIMIT,
                "CMVG_LIMIT": config.CMVG_LIMIT,
                "STK_HOLD_LIMIT": config.STK_HOLD_LIMIT,
                "OTHER_LIMIT": config.OTHER_LIMIT,
                "STK_BUY_R": config.STK_BUY_R,
                "TURN_MAX": config.TURN_MAX,
                "MEM_HOLD": config.MEM_HOLD,
            },
            "backtest_info": [info.to_dict()],
        }

        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                try:
                    all_data = json.load(f)
                    if not isinstance(all_data, list):
                        all_data = [all_data]
                except json.JSONDecodeError:
                    all_data = []
        else:
            all_data = []

        all_data.append(new_entry)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=4, ensure_ascii=False)

        print(f"本次回测已追加至统一文件，当前总记录数: {len(all_data)}")

        return info
