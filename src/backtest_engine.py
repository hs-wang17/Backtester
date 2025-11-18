from tqdm import tqdm
import pandas as pd
import numpy as np
from src.config import *
from src.data_loader import *
from src.utils import get_daily_price, get_daily_support
from src.portfolio_optimizer import solve_problem
from src.account import account
from src.analysis import analyse


def run_backtest():
    s = account(INITIAL_MONEY)
    account_s = {}
    cash_s = {}
    buy_s = {}
    sell_s = {}
    hold_df_dict = {}
    trade_df_dict = {}
    hold_style_dict = {}

    for date in tqdm(date_list, desc="Backtesting"):
        td_open, td_close, td_preclose, td_adj, td_score, td_upper, td_lower, last_zt = get_daily_price(
            str(date), vwap_df, close, pre_close, adj, scores, upper_price, lower_price, last_zt_df
        )
        td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support(str(date))
        code_list = pd.concat([td_upper, td_lower, td_close, td_open], axis=1).dropna(how="any").index.tolist()  # 今日可交易
        code_list = [x for x in code_list if (x in sub_code_list) & (x[0] != "4") & (x[0] != "8")]  # 剔除新股,ST股票,北交所股票
        stk_perm = (td_mem + td_mem.max()) * (STK_HOLD_LIMIT / (2 * td_mem.max()))
        zt_codes = last_zt[last_zt == 1].index.tolist()

        # 开盘前刷新参数
        account0 = s.refresh_open(td_upper, td_lower, td_preclose.to_dict(), td_adj)
        stk_buy_amt = pd.Series([STK_BUY_R * account0] * len(code_list), index=code_list)
        for code in zt_codes:
            if code in code_list:
                stk_buy_amt[code] = 0

        # 获取昨日持仓
        if len(s.hold_dict) == 0:
            last_hold = td_mem.reindex(code_list).fillna(0) * account0  # pd.Series([0]*len(code_list),index=code_list)
        else:
            last_hold = hold_df["amt"].reindex(code_list).fillna(0) + s.cash * td_mem.reindex(code_list).fillna(0)
            st_hold = hold_df["amt"].reindex([x for x in hold_df.index if (x not in code_list)])  #

        try:
            # 通过组合优化，获取当日目标持仓市值
            tgt_hold = solve_problem(
                code_list,
                x_last=last_hold,
                score0=(td_score - td_score.min()) / (td_score.max() - td_score.min()),
                stk_low0=((td_mem - stk_perm).clip(0) * account0).clip(
                    upper=last_hold + stk_buy_amt, lower=last_hold - 2 * STK_BUY_R * account0
                ),  # 每日卖出额为2份，则个股持仓额下限进考虑指数成分股偏离
                stk_high0=((td_mem + stk_perm) * account0).clip(
                    upper=last_hold + stk_buy_amt, lower=last_hold - 2 * STK_BUY_R * account0
                ),  # 考虑指数成分股偏离上限，以及个股当日最多可买一份
                tot_amt0=1.01 * account0,  # 适当增加买入任务金额, 避免买入失败而空仓
                sell_max0=account0 * TURN_MAX,
                td_mem0=(td_mem > 0).astype(int),
                td_mem_amt0=MEM_HOLD * account0,
                td_ind0=td_citic,
                td_ind_up0=((zz_citic + CITIC_LIMIT) * account0),
                td_ind_down0=((zz_citic - CITIC_LIMIT) * account0),
                td_cmvg0=td_cmvg,
                td_cmvg_up0=((zz_cmvg + CMVG_LIMIT) * account0),
                td_cmvg_down0=((zz_cmvg - CMVG_LIMIT) * account0),
                td_style=style_fac,
                style_up0=((zz_style + OTHER_LIMIT) * account0),
                style_down0=((zz_style - OTHER_LIMIT) * account0),
                solver="SCIPY",
            )

            if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:  # 如果求解失败，换手率放大3倍
                raise ValueError("no target hold")
        except:
            # 当组合求解失败, 默认向指数成分股靠拢换手率比例
            print(date, "no target hold, move toward index")
            tgt_hold = last_hold * (1 - TURN_MAX) + td_mem.reindex(code_list).fillna(0) * TURN_MAX * s.tot_account

        # 根据目标持仓，得到交易任务
        sort_index = td_score.sort_values(ascending=False).index
        if len(s.hold_dict) == 0:  # 第一天建仓，参考中证1000指数
            to_buy_s = round(tgt_hold).replace(0, np.nan).reindex(sort_index).dropna()  # 按打分排序，剔除任务金额为0的股票
            to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # 金额转换成量
            to_sell_s = pd.Series(dtype=float)
        else:
            last_hold = hold_df["amt"].reindex(code_list).fillna(0)
            to_trade_s = round(tgt_hold - last_hold).replace(0, np.nan)
            to_buy_s = to_trade_s[to_trade_s > 0].reindex(sort_index).dropna()  # 按打分排序，剔除任务金额为0的股票
            to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # 金额转换成量
            to_sell_s = pd.concat([st_hold, -to_trade_s[to_trade_s < 0].reindex(sort_index).dropna().iloc[::-1]])  # 卖出股票从低分开始排序
            to_sell_s = round(to_sell_s / td_preclose.reindex(to_sell_s.index))  # 金额转换成量

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

        hold_weight = hold_df["amt"] / hold_df["amt"].sum()
        td_citic_diff = td_citic.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_citic
        td_cmvg_diff = td_cmvg.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_cmvg
        td_style_diff = style_fac.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_style
        td_MEM_HOLD = hold_weight.reindex(td_mem[td_mem > 0].index).fillna(0).sum()
        td_diff = pd.concat([td_citic_diff, td_cmvg_diff, td_style_diff])
        td_diff["idx_hold"] = td_MEM_HOLD
        hold_style_dict[date] = td_diff

    # 结果汇总
    tot_s = pd.concat(
        [pd.Series(account_s), pd.Series(cash_s), pd.Series(buy_s), pd.Series(sell_s)], axis=1, keys=["tot_account", "cash", "buy_amt", "sell_amt"]
    )
    nv = pd.concat([zs_day.reindex(tot_s.index), tot_s["tot_account"]], axis=1, keys=["zs", "strategy"])
    nv = nv / nv.iloc[0]
    info, _, _ = analyse(nv, plotting=True, strategy=STRATEGY_NAME)
    return {"tot_account_s": tot_s, "nv": nv, "info": info, "hold_style": pd.DataFrame(hold_style_dict).T}
