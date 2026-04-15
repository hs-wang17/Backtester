import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

import src.config as config
from src.account import account
from src.analysis import analyse
from src.plot import plot
from src.strategy import solve_strategy, solve_strategy_second, topn_strategy, record_trade
from src.utils import get_daily_price_continuous_general, get_daily_support5, get_daily_support7, get_daily_support_barra


def load_daily_data(name):
    return pd.read_feather(os.path.join(config.DAILY_DATA_PATH, f"{name}.feather"))


def run_backtest_continuous_general():
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
    
    intervals_morning = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30)]
    intervals_noon = [(121, 126), (126, 131), (131, 136), (136, 141), (141, 146), (146, 151)]
    
    if config.AFTERNOON_START:
        if not config.TWAP_MODE:
            vwap_df_list = [pd.read_feather(os.path.join(config.VWAP_TWAP_PATH, f"vwap_{start}_{end}.fea"))
                            for start, end in intervals_noon]
        else:
            vwap_df_list = [pd.read_feather(os.path.join(config.VWAP_TWAP_PATH, f"twap_{start}_{end}.fea"))
                            for start, end in intervals_noon]
    else:
        if not config.TWAP_MODE:
            vwap_df_list = [pd.read_feather(os.path.join(config.VWAP_TWAP_PATH, f"vwap_{start}_{end}.fea"))
                            for start, end in intervals_morning]
        else:
            vwap_df_list = [pd.read_feather(os.path.join(config.VWAP_TWAP_PATH, f"twap_{start}_{end}.fea"))
                            for start, end in intervals_morning]
    
    scores, index_sets, col_sets = [], [], []
    for path in config.SCORES_PATH:
        if config.AFTERNOON_START or config.CALL_START:
            scores_single = pd.read_csv(path, index_col=0).T.sort_index().dropna(how="all")
        else:
            scores_single = pd.read_csv(path, index_col=0).T.sort_index().shift(1).dropna(how="all")
        scores_single.columns = scores_single.columns.astype(str).str.zfill(6)
        scores_single = scores_single[scores_single.columns[scores_single.columns.str[0].isin(["0", "3", "6"])]]
        scores_single.index = scores_single.index.astype(str)
        scores.append(scores_single)
        index_sets.append(set(scores_single.index))
        col_sets.append(set(scores_single.columns))
    common_dates = sorted(set.intersection(*index_sets) & set(vwap_df_list[0].index.astype(str)))
    common_cols = sorted(set.intersection(*col_sets))
    scores = [df.loc[common_dates, common_cols] for df in scores]
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
        td_open_list, td_close, td_preclose, td_adj, td_score, td_upper, td_lower, last_zt = get_daily_price_continuous_general(
            str(date), vwap_df_list, close, pre_close, adj, scores, upper_price, lower_price, last_zt_df
        )

        # get daily support data
        if config.TRADE_SUPPORT == 5:
            td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support5(str(date))
        elif config.TRADE_SUPPORT == 7:
            td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support7(str(date))
        else:
            td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support_barra(str(date))

        mem_hs300, mem_zz500, mem_zz1000, mem_zz2000 = td_mem
        if config.IDX_NAME == "hs300":
            td_mem = mem_hs300
        elif config.IDX_NAME == "zz500":
            td_mem = mem_zz500
        elif config.IDX_NAME == "zz1000":
            td_mem = mem_zz1000
        elif config.IDX_NAME == "zz2000":
            td_mem = mem_zz2000
            
        # get today's tradable and zt stocks
        code_list_all = pd.concat([td_upper, td_lower, td_close, td_open_list[0]], axis=1).dropna(how="any").index.tolist()  # tradable stocks
        code_list = [
            x for x in code_list_all if (x in sub_code_list) and (x[0] not in ["4", "8"])
        ]  # tradable stocks except new/ST/BSE stocks
        zt_codes = last_zt[last_zt == 1].index.tolist()
        code_list_zt = [x for x in code_list if x not in zt_codes]  # remove zt stocks

        # calculate stk_perm
        stk_perm = (td_mem + td_mem.max()) * (config.STK_HOLD_LIMIT / (2 * td_mem.max()))

        """first trading"""

        # refresh before market open
        act = s.refresh_open(td_upper, td_lower, td_preclose.to_dict(), td_adj)

        # prepare params dictionary
        params = {
            'code_list': code_list,
            'code_list_all': code_list_all,
            'zt_codes': zt_codes,
            'code_list_zt': code_list_zt,
            'td_score': td_score,
            'td_mem': td_mem,
            'stk_perm': stk_perm,
            'td_citic': td_citic,
            'zz_citic': zz_citic,
            'td_cmvg': td_cmvg,
            'zz_cmvg': zz_cmvg,
            'style_fac': style_fac,
            'zz_style': zz_style,
            'td_preclose': td_preclose,
        }
        
        # strategy
        if config.STRATEGY == "solve":
            to_buy_s, to_sell_s = solve_strategy(s, act, **params)
        elif config.STRATEGY == "topn":
            to_buy_s, to_sell_s = topn_strategy(s, act, **params)

        # execute trades
        hold_df, sellable_amt = record_trade(
            s, td_open_list[0], to_buy_s, to_sell_s, date, act_s, cash_s, buy_s, sell_s, hold_df_dict, trade_df_dict, "0", None
        )

        """second to sixth trading"""
        
        for i in range(1, len(td_open_list)):
            # refresh before market open
            act = s.cal_total()
            
            # prepare params dictionary
            ret = td_open_list[i] / td_open_list[i - 1] - 1
            params['td_score'] = [score.add(ret, fill_value=0) for score in params['td_score']]
            
            # strategy
            if config.STRATEGY == "solve":
                to_buy_s, to_sell_s = solve_strategy_second(s, act, sellable_amt, **params)
            elif config.STRATEGY == "topn":
                to_buy_s, to_sell_s = topn_strategy(s, act, **params)
            
            # execute trades
            hold_df, sellable_amt = record_trade(
                s, td_open_list[i], to_buy_s, to_sell_s, date, 
                act_s, cash_s, buy_s, sell_s, hold_df_dict, trade_df_dict, str(i), td_close,
            )
        
        # calculate holding style difference
        hold_weight = hold_df["amt"] / hold_df["amt"].sum()
        td_citic_diff = td_citic.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_citic  # 行业偏离
        td_cmvg_diff = td_cmvg.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_cmvg  # 市值偏离
        td_style_diff = style_fac.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_style  # 风格偏离
        # td_mem_hold = hold_weight.reindex(td_mem[td_mem > 0].index).fillna(0).sum()
        td_mem_hs300_hold = hold_weight.reindex(mem_hs300[mem_hs300 > 0].index).fillna(0).sum()
        td_mem_zz500_hold = hold_weight.reindex(mem_zz500[mem_zz500 > 0].index).fillna(0).sum()
        td_mem_zz1000_hold = hold_weight.reindex(mem_zz1000[mem_zz1000 > 0].index).fillna(0).sum()
        td_mem_zz2000_hold = hold_weight.reindex(mem_zz2000[mem_zz2000 > 0].index).fillna(0).sum()
        td_hold_num = len(hold_weight)
        td_turnover = (buy_s[date + "0"] + sell_s[date + "0"] 
                       + buy_s[date + "1"] + sell_s[date + "1"] 
                       + buy_s[date + "2"] + sell_s[date + "2"] 
                       + buy_s[date + "3"] + sell_s[date + "3"] 
                       + buy_s[date + "4"] + sell_s[date + "4"] 
                       + buy_s[date + "5"] + sell_s[date + "5"]) / act_s[date] / 6.0
        if isinstance(td_score, list):
            hold_weight_aligned = hold_weight.reindex(td_score[0].index).fillna(0)
            amt_weighted_rank = hold_weight_aligned @ td_score[0].rank(ascending=False)
        else:
            hold_weight_aligned = hold_weight.reindex(td_score.index).fillna(0)
            amt_weighted_rank = hold_weight_aligned @ td_score.rank(ascending=False)

        td_diff = pd.concat([td_citic_diff, td_cmvg_diff, td_style_diff])
        # td_diff["mem_hold"] = td_mem_hold
        td_diff["mem_hs300_hold"] = td_mem_hs300_hold
        td_diff["mem_zz500_hold"] = td_mem_zz500_hold
        td_diff["mem_zz1000_hold"] = td_mem_zz1000_hold
        td_diff["mem_zz2000_hold"] = td_mem_zz2000_hold
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
                    config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_afternoon_trade_support{config.TRADE_SUPPORT}_continuous_hold_df.csv",
                    index_label="code",
                )
            elif config.STRATEGY == "topn":
                all_hold_df.to_csv(config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_afternoon_topn_continuous_hold_df.csv", index_label="code")
        else:
            if config.STRATEGY == "solve":
                all_hold_df.to_csv(
                    config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_trade_support{config.TRADE_SUPPORT}_continuous_hold_df.csv", index_label="code"
                )
            elif config.STRATEGY == "topn":
                all_hold_df.to_csv(config.RESULT_PATH + "/" + config.STRATEGY_NAME + f"_topn_continuous_hold_df.csv", index_label="code")
        plot(nv, rel_nv, info, strategy=config.STRATEGY_NAME, scores_path=config.SCORES_PATH, hold_style=hold_style)

    else:
        # PLOT=False: use for parameter optimization (efficient frontier), save results to a unified JSON file
        json_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../para_optimizer_ef/scores/",
            f"{config.PARA_NAME}.json"
        )
        json_path = os.path.abspath(json_path)

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
