import numpy as np
import pandas as pd
import src.config as config
from src.portfolio_optimizer import solve_problem


def solve_strategy(s, act, **kwargs):
    """Solve strategy: Optimize portfolio based on various constraints and scores."""

    code_list = kwargs.get('code_list')
    code_list_all = kwargs.get('code_list_all')
    zt_codes = kwargs.get('zt_codes')
    td_score = kwargs.get('td_score')
    td_mem = kwargs.get('td_mem')
    stk_perm = kwargs.get('stk_perm')
    td_citic = kwargs.get('td_citic')
    zz_citic = kwargs.get('zz_citic')
    td_cmvg = kwargs.get('td_cmvg')
    zz_cmvg = kwargs.get('zz_cmvg')
    style_fac = kwargs.get('style_fac')
    zz_style = kwargs.get('zz_style')
    td_preclose = kwargs.get('td_preclose')
    
    # ensure td_score is a list
    td_score = td_score if isinstance(td_score, list) else [td_score]

    # get initial holding
    if len(s.hold_dict) == 0:
        if config.HOLD_INIT == "member":
            last_hold_init = td_mem.reindex(code_list).fillna(0)

        elif config.HOLD_INIT == "solve":
            # calculate initial last holding based on td_mem
            last_hold_init = td_mem.reindex(code_list).fillna(0)
            stk_buy_weight_init = pd.Series([0.1] * len(code_list), index=code_list)  # initial buy weight (0.1)
            for code in zt_codes:
                if code in code_list:
                    stk_buy_weight_init[code] = 0

            last_hold_inits = []
            for td_score_single in td_score:
                last_hold_init = solve_problem(
                    code_list=code_list,
                    x_last=last_hold_init,
                    score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                    stk_low=(td_mem - stk_perm)
                    .clip(0)
                    .clip(upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R),
                    stk_high=(td_mem + stk_perm).clip(
                        upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R
                    ),
                    tot_weight=1.01,
                    sell_max=1,  # allow full adjustment of holding
                    td_mem=(td_mem > 0).astype(int),
                    td_mem_weight=config.MEM_HOLD,
                    td_ind=td_citic,
                    td_ind_up=(zz_citic + config.CITIC_LIMIT),
                    td_ind_down=(zz_citic - config.CITIC_LIMIT),
                    td_cmvg=td_cmvg,
                    td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                    td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                    td_style=style_fac,
                    style_up=(zz_style + config.OTHER_LIMIT),
                    style_down=(zz_style - config.OTHER_LIMIT),
                    solver="SCIPY",
                    method=config.SOLVER_METHOD,
                )
                last_hold_inits.append(last_hold_init)
            # last_hold_init = pd.concat(last_hold_inits, axis=1).mean(axis=1)
            if len(last_hold_inits) == 1:
                last_hold_init = last_hold_inits[0]
            else:
                last_hold_init = last_hold_inits[0] * config.MIX_COEFFICIENT + last_hold_inits[1] * (1 - config.MIX_COEFFICIENT)

        last_hold = last_hold_init
    else:
        hold_df, _ = s.close_today()
        last_hold = hold_df["amount"].reindex(code_list).fillna(0) / act
        st_hold = hold_df["amount"].reindex(hold_df.index.difference(code_list).intersection(code_list_all))
        
    # solve optimization problem
    stk_buy_weight = pd.Series([config.STK_BUY_R] * len(code_list), index=code_list)
    for code in zt_codes:
        if code in code_list:
            stk_buy_weight[code] = 0

    # first trial
    try:
        tgt_holds = []
        for td_score_single in td_score:
            tgt_hold = act * solve_problem(
                code_list=code_list,
                x_last=last_hold,
                score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                stk_low=(td_mem - stk_perm).clip(0).clip(upper=last_hold + stk_buy_weight, lower=last_hold - 2 * config.STK_BUY_R),
                stk_high=(td_mem + stk_perm).clip(upper=last_hold + stk_buy_weight, lower=last_hold - 2 * config.STK_BUY_R),
                tot_weight=1.01,
                sell_max=config.TURN_MAX,
                td_mem=(td_mem > 0).astype(int),
                td_mem_weight=config.MEM_HOLD,
                td_ind=td_citic,
                td_ind_up=(zz_citic + config.CITIC_LIMIT),
                td_ind_down=(zz_citic - config.CITIC_LIMIT),
                td_cmvg=td_cmvg,
                td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                td_style=style_fac,
                style_up=(zz_style + config.OTHER_LIMIT),
                style_down=(zz_style - config.OTHER_LIMIT),
                solver="SCIPY",
                method=config.SOLVER_METHOD,
            )
            tgt_holds.append(tgt_hold)
        tgt_hold = pd.concat(tgt_holds, axis=1).mean(axis=1)

        if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
            raise ValueError("first trial empty")

    except Exception as first_exception:
        print("First trial failed:", first_exception)

        # second trial
        try:
            tgt_holds = []
            for td_score_single in td_score:
                tgt_hold = act * solve_problem(
                    code_list=code_list,
                    x_last=last_hold,
                    score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                    stk_low=(td_mem - stk_perm).clip(0).clip(upper=last_hold + 2 * stk_buy_weight, lower=last_hold - 4 * config.STK_BUY_R),
                    stk_high=(td_mem + stk_perm).clip(upper=last_hold + 2 * stk_buy_weight, lower=last_hold - 4 * config.STK_BUY_R),
                    tot_weight=1.01,
                    sell_max=2 * config.TURN_MAX,
                    td_mem=(td_mem > 0).astype(int),
                    td_mem_weight=config.MEM_HOLD,
                    td_ind=td_citic,
                    td_ind_up=(zz_citic + config.CITIC_LIMIT),
                    td_ind_down=(zz_citic - config.CITIC_LIMIT),
                    td_cmvg=td_cmvg,
                    td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                    td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                    td_style=style_fac,
                    style_up=(zz_style + config.OTHER_LIMIT),
                    style_down=(zz_style - config.OTHER_LIMIT),
                    solver="SCIPY",
                    method=config.SOLVER_METHOD,
                )
                tgt_holds.append(tgt_hold)
            tgt_hold = pd.concat(tgt_holds, axis=1).mean(axis=1)

            if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
                raise ValueError("second trial empty")

        except Exception as second_exception:
            print("Second trial failed:", second_exception)

            # fallback
            if config.HOLD_INIT == "member":
                last_hold_init = td_mem.reindex(code_list).fillna(0)

            elif config.HOLD_INIT == "solve":
                # calculate initial last holding based on td_mem
                last_hold_init = td_mem.reindex(code_list).fillna(0)
                stk_buy_weight_init = pd.Series([0.1] * len(code_list), index=code_list)  # initial buy weight
                for code in zt_codes:
                    if code in code_list:
                        stk_buy_weight_init[code] = 0

                last_hold_inits = []
                for td_score_single in td_score:
                    last_hold_init = solve_problem(
                        code_list=code_list,
                        x_last=last_hold_init,
                        score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                        stk_low=(td_mem - stk_perm)
                        .clip(0)
                        .clip(upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R),
                        stk_high=(td_mem + stk_perm).clip(
                            upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R
                        ),
                        tot_weight=1.01,
                        sell_max=1,  # allow full adjustment
                        td_mem=(td_mem > 0).astype(int),
                        td_mem_weight=config.MEM_HOLD,
                        td_ind=td_citic,
                        td_ind_up=(zz_citic + config.CITIC_LIMIT),
                        td_ind_down=(zz_citic - config.CITIC_LIMIT),
                        td_cmvg=td_cmvg,
                        td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                        td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                        td_style=style_fac,
                        style_up=(zz_style + config.OTHER_LIMIT),
                        style_down=(zz_style - config.OTHER_LIMIT),
                        solver="SCIPY",
                        method=config.SOLVER_METHOD,
                    )
                    last_hold_inits.append(last_hold_init)
                # last_hold_init = pd.concat(last_hold_inits, axis=1).mean(axis=1)
                if len(last_hold_inits) == 1:
                    last_hold_init = last_hold_inits[0]
                else:
                    last_hold_init = last_hold_inits[0] * config.MIX_COEFFICIENT + last_hold_inits[1] * (1 - config.MIX_COEFFICIENT)
            tgt_hold = act * (last_hold * (1 - config.TURN_MAX) + last_hold_init * config.TURN_MAX)

    # get to buy and to sell series
    sort_index = td_score[0].sort_values(ascending=False).index  # sort by one of the td_score
    if len(s.hold_dict) == 0:  # first day build position, follow index
        to_buy_s = round(tgt_hold).replace(0, np.nan).reindex(sort_index).dropna()
        to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
        to_sell_s = pd.Series(dtype=float)
    else:
        last_hold = hold_df["amount"].reindex(code_list).fillna(0)
        to_trade_s = round(tgt_hold - last_hold).replace(0, np.nan)
        to_buy_s = to_trade_s[to_trade_s > 0].reindex(sort_index).dropna()
        to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity

        to_sell_s = pd.concat([st_hold, -to_trade_s[to_trade_s < 0].reindex(sort_index).dropna().iloc[::-1]])  # sort by score
        to_sell_s = round(to_sell_s / td_preclose.reindex(to_sell_s.index))  # transform amount to quantity

    return to_buy_s, to_sell_s

def solve_strategy_noon(s, act, sellable_amount, **kwargs):
    """Solve strategy: Optimize portfolio based on various constraints and scores."""

    code_list = kwargs.get('code_list')
    code_list_all = kwargs.get('code_list_all')
    zt_codes = kwargs.get('zt_codes')
    td_score = kwargs.get('td_score')
    td_mem = kwargs.get('td_mem')
    stk_perm = kwargs.get('stk_perm')
    td_citic = kwargs.get('td_citic')
    zz_citic = kwargs.get('zz_citic')
    td_cmvg = kwargs.get('td_cmvg')
    zz_cmvg = kwargs.get('zz_cmvg')
    style_fac = kwargs.get('style_fac')
    zz_style = kwargs.get('zz_style')
    td_preclose = kwargs.get('td_preclose')

    # ensure td_score is a list
    td_score = td_score if isinstance(td_score, list) else [td_score]

    hold_df, _ = s.close_today()
    last_hold = hold_df["amount"].reindex(code_list).fillna(0) / act
    sellable = sellable_amount.set_index("code")["sellable_amount"].reindex(code_list).fillna(0) / act

    # solve optimization problem
    stk_buy_weight = pd.Series([config.STK_BUY_R] * len(code_list), index=code_list)
    for code in zt_codes:
        if code in code_list:
            stk_buy_weight[code] = 0

    # first trial
    try:
        tgt_holds = []
        for td_score_single in td_score:
            tgt_hold = act * solve_problem(
                code_list=code_list,
                x_last=last_hold,
                score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                stk_low=(td_mem - stk_perm)
                .clip(0)
                .clip(upper=last_hold + stk_buy_weight, lower=last_hold - 2 * config.STK_BUY_R)
                .clip(lower=last_hold - sellable),  # constraint the lower bound that cannot be sold today
                stk_high=(td_mem + stk_perm)
                .clip(upper=last_hold + stk_buy_weight, lower=last_hold - 2 * config.STK_BUY_R)
                .clip(lower=last_hold - sellable),
                tot_weight=1.01,
                sell_max=config.TURN_MAX_NOON,
                td_mem=(td_mem > 0).astype(int),
                td_mem_weight=config.MEM_HOLD,
                td_ind=td_citic,
                td_ind_up=(zz_citic + (config.CITIC_LIMIT)),
                td_ind_down=(zz_citic - (config.CITIC_LIMIT)),
                td_cmvg=td_cmvg,
                td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                td_style=style_fac,
                style_up=(zz_style + config.OTHER_LIMIT),
                style_down=(zz_style - config.OTHER_LIMIT),
                solver="SCIPY",
                method=config.SOLVER_METHOD,
            )
            tgt_holds.append(tgt_hold)
        tgt_hold = pd.concat(tgt_holds, axis=1).mean(axis=1)

        if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
            raise ValueError("first trial empty")

    except Exception as first_exception:
        print("First trial failed noon:", first_exception)

        # second trial
        try:
            tgt_holds = []
            for td_score_single in td_score:
                tgt_hold = act * solve_problem(
                    code_list=code_list,
                    x_last=last_hold,
                    score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                    stk_low=(td_mem - stk_perm)
                    .clip(0)
                    .clip(upper=last_hold + 2 * stk_buy_weight, lower=last_hold - 4 * config.STK_BUY_R)
                    .clip(lower=last_hold - sellable),
                    stk_high=(td_mem + stk_perm)
                    .clip(upper=last_hold + 2 * stk_buy_weight, lower=last_hold - 4 * config.STK_BUY_R)
                    .clip(lower=last_hold - sellable),
                    tot_weight=1.01,
                    sell_max=2 * config.TURN_MAX_NOON,
                    td_mem=(td_mem > 0).astype(int),
                    td_mem_weight=config.MEM_HOLD,
                    td_ind=td_citic,
                    td_ind_up=(zz_citic + (config.CITIC_LIMIT + config.CITIC_LIMIT_NOON)),
                    td_ind_down=(zz_citic - (config.CITIC_LIMIT + config.CITIC_LIMIT_NOON)),
                    td_cmvg=td_cmvg,
                    td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                    td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                    td_style=style_fac,
                    style_up=(zz_style + config.OTHER_LIMIT),
                    style_down=(zz_style - config.OTHER_LIMIT),
                    solver="SCIPY",
                    method=config.SOLVER_METHOD,
                )
                tgt_holds.append(tgt_hold)
            tgt_hold = pd.concat(tgt_holds, axis=1).mean(axis=1)

            if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
                raise ValueError("second trial empty")

        except Exception as second_exception:
            print("Second trial failed noon:", second_exception)

            # fallback
            if config.HOLD_INIT == "member":
                last_hold_init = td_mem.reindex(code_list).fillna(0)

            elif config.HOLD_INIT == "solve":
                # calculate initial last holding based on td_mem
                last_hold_init = td_mem.reindex(code_list).fillna(0)
                stk_buy_weight_init = pd.Series([0.1] * len(code_list), index=code_list)  # initial buy weight
                for code in zt_codes:
                    if code in code_list:
                        stk_buy_weight_init[code] = 0

                last_hold_inits = []
                for td_score_single in td_score:
                    last_hold_init = solve_problem(
                        code_list=code_list,
                        x_last=last_hold_init,
                        score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                        stk_low=(td_mem - stk_perm)
                        .clip(0)
                        .clip(upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R),
                        # .clip(lower=last_hold - sellable),
                        stk_high=(td_mem + stk_perm).clip(
                            upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R
                        ),
                        # .clip(lower=last_hold - sellable),
                        tot_weight=1.01,
                        sell_max=1,  # allow full adjustment
                        td_mem=(td_mem > 0).astype(int),
                        td_mem_weight=config.MEM_HOLD,
                        td_ind=td_citic,
                        td_ind_up=(zz_citic + (config.CITIC_LIMIT + config.CITIC_LIMIT_NOON)),
                        td_ind_down=(zz_citic - (config.CITIC_LIMIT + config.CITIC_LIMIT_NOON)),
                        td_cmvg=td_cmvg,
                        td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                        td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                        td_style=style_fac,
                        style_up=(zz_style + config.OTHER_LIMIT),
                        style_down=(zz_style - config.OTHER_LIMIT),
                        solver="SCIPY",
                        method=config.SOLVER_METHOD,
                    )
                    last_hold_inits.append(last_hold_init)
                # last_hold_init = pd.concat(last_hold_inits, axis=1).mean(axis=1)
                if len(last_hold_inits) == 1:
                    last_hold_init = last_hold_inits[0]
                else:
                    last_hold_init = last_hold_inits[0] * config.MIX_COEFFICIENT + last_hold_inits[1] * (1 - config.MIX_COEFFICIENT)
            tgt_hold = act * (last_hold * (1 - config.TURN_MAX_NOON) + last_hold_init * config.TURN_MAX_NOON)

    # get to buy and to sell series
    sort_index = td_score[0].sort_values(ascending=False).index  # sort by one of the td_score
    if len(s.hold_dict) == 0:  # first day build position, follow index
        to_buy_s = round(tgt_hold).replace(0, np.nan).reindex(sort_index).dropna()  # sort by score
        to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
        to_sell_s = pd.Series(dtype=float)
    else:
        last_hold = hold_df["amount"].reindex(code_list).fillna(0)
        to_trade_s = round(tgt_hold - last_hold).replace(0, np.nan)
        to_buy_s = to_trade_s[to_trade_s > 0].reindex(sort_index).dropna()  # sort by score
        to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
        to_sell_s = -to_trade_s[to_trade_s < 0].reindex(sort_index).dropna().iloc[::-1]  # sort by score (different from solve_strategy)
        to_sell_s = round(to_sell_s / td_preclose.reindex(to_sell_s.index))  # transform amount to quantity

    return to_buy_s, to_sell_s

def solve_strategy_second(s, act, sellable_amount, **kwargs):
    """Solve strategy: Optimize portfolio based on various constraints and scores."""

    code_list = kwargs.get('code_list')
    code_list_all = kwargs.get('code_list_all')
    zt_codes = kwargs.get('zt_codes')
    td_score = kwargs.get('td_score')
    td_mem = kwargs.get('td_mem')
    stk_perm = kwargs.get('stk_perm')
    td_citic = kwargs.get('td_citic')
    zz_citic = kwargs.get('zz_citic')
    td_cmvg = kwargs.get('td_cmvg')
    zz_cmvg = kwargs.get('zz_cmvg')
    style_fac = kwargs.get('style_fac')
    zz_style = kwargs.get('zz_style')
    td_preclose = kwargs.get('td_preclose')

    # ensure td_score is a list
    td_score = td_score if isinstance(td_score, list) else [td_score]

    hold_df, _ = s.close_today()
    last_hold = hold_df["amount"].reindex(code_list).fillna(0) / act
    sellable = sellable_amount.set_index("code")["sellable_amount"].reindex(code_list).fillna(0) / act

    # solve optimization problem
    stk_buy_weight = pd.Series([config.STK_BUY_R] * len(code_list), index=code_list)
    for code in zt_codes:
        if code in code_list:
            stk_buy_weight[code] = 0

    # first trial
    try:
        tgt_holds = []
        for td_score_single in td_score:
            tgt_hold = act * solve_problem(
                code_list=code_list,
                x_last=last_hold,
                score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                stk_low=(td_mem - stk_perm)
                .clip(0)
                .clip(upper=last_hold + stk_buy_weight, lower=last_hold - 2 * config.STK_BUY_R)
                .clip(lower=last_hold - sellable),
                stk_high=(td_mem + stk_perm)
                .clip(upper=last_hold + stk_buy_weight, lower=last_hold - 2 * config.STK_BUY_R)
                .clip(lower=last_hold - sellable),
                tot_weight=1.01,
                sell_max=config.TURN_MAX_SECOND,
                td_mem=(td_mem > 0).astype(int),
                td_mem_weight=config.MEM_HOLD,
                td_ind=td_citic,
                td_ind_up=(zz_citic + (config.CITIC_LIMIT)),
                td_ind_down=(zz_citic - (config.CITIC_LIMIT)),
                td_cmvg=td_cmvg,
                td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                td_style=style_fac,
                style_up=(zz_style + config.OTHER_LIMIT),
                style_down=(zz_style - config.OTHER_LIMIT),
                solver="SCIPY",
                method=config.SOLVER_METHOD,
            )
            tgt_holds.append(tgt_hold)
        tgt_hold = pd.concat(tgt_holds, axis=1).mean(axis=1)

        if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
            raise ValueError("first trial empty")

    except Exception as first_exception:
        print("First trial failed second:", first_exception)

        # second trial
        try:
            tgt_holds = []
            for td_score_single in td_score:
                tgt_hold = act * solve_problem(
                    code_list=code_list,
                    x_last=last_hold,
                    score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),
                    stk_low=(td_mem - stk_perm)
                    .clip(0)
                    .clip(upper=last_hold + 2 * stk_buy_weight, lower=last_hold - 4 * config.STK_BUY_R)
                    .clip(lower=last_hold - sellable),
                    stk_high=(td_mem + stk_perm)
                    .clip(upper=last_hold + 2 * stk_buy_weight, lower=last_hold - 4 * config.STK_BUY_R)
                    .clip(lower=last_hold - sellable),
                    tot_weight=1.01,
                    sell_max=2 * config.TURN_MAX_SECOND,
                    td_mem=(td_mem > 0).astype(int),
                    td_mem_weight=config.MEM_HOLD,
                    td_ind=td_citic,
                    td_ind_up=(zz_citic + (config.CITIC_LIMIT + config.CITIC_LIMIT_SECOND)),
                    td_ind_down=(zz_citic - (config.CITIC_LIMIT + config.CITIC_LIMIT_SECOND)),
                    td_cmvg=td_cmvg,
                    td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                    td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                    td_style=style_fac,
                    style_up=(zz_style + config.OTHER_LIMIT),
                    style_down=(zz_style - config.OTHER_LIMIT),
                    solver="SCIPY",
                    method=config.SOLVER_METHOD,
                )
                tgt_holds.append(tgt_hold)
            tgt_hold = pd.concat(tgt_holds, axis=1).mean(axis=1)

            if len(round(tgt_hold).replace(0, np.nan).dropna()) == 0:
                raise ValueError("second trial empty")

        except Exception as second_exception:
            print("Second trial failed second:", second_exception)

            # fallback
            if config.HOLD_INIT == "member":
                last_hold_init = td_mem.reindex(code_list).fillna(0)

            elif config.HOLD_INIT == "solve":
                # calculate initial last holding based on td_mem
                last_hold_init = td_mem.reindex(code_list).fillna(0)
                stk_buy_weight_init = pd.Series([0.1] * len(code_list), index=code_list)  # initial buy weight
                for code in zt_codes:
                    if code in code_list:
                        stk_buy_weight_init[code] = 0

                last_hold_inits = []
                for td_score_single in td_score:
                    last_hold_init = solve_problem(
                        code_list=code_list,
                        x_last=last_hold_init,
                        score=(td_score_single - td_score_single.min()) / (td_score_single.max() - td_score_single.min()),  # 0-1 normalized score (encourage full holding)
                        stk_low=(td_mem - stk_perm)
                        .clip(0)
                        .clip(upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R),
                        # .clip(lower=last_hold - sellable),
                        stk_high=(td_mem + stk_perm).clip(
                            upper=last_hold_init + stk_buy_weight_init, lower=last_hold_init - 2 * config.STK_BUY_R
                        ),
                        # .clip(lower=last_hold - sellable),
                        tot_weight=1.01,
                        sell_max=1,  # allow full adjustment
                        td_mem=(td_mem > 0).astype(int),
                        td_mem_weight=config.MEM_HOLD,
                        td_ind=td_citic,
                        td_ind_up=(zz_citic + (config.CITIC_LIMIT + config.CITIC_LIMIT_SECOND)),
                        td_ind_down=(zz_citic - (config.CITIC_LIMIT + config.CITIC_LIMIT_SECOND)),
                        td_cmvg=td_cmvg,
                        td_cmvg_up=(zz_cmvg + config.CMVG_LIMIT),
                        td_cmvg_down=(zz_cmvg - config.CMVG_LIMIT),
                        td_style=style_fac,
                        style_up=(zz_style + config.OTHER_LIMIT),
                        style_down=(zz_style - config.OTHER_LIMIT),
                        solver="SCIPY",
                        method=config.SOLVER_METHOD,
                    )
                    last_hold_inits.append(last_hold_init)
                # last_hold_init = pd.concat(last_hold_inits, axis=1).mean(axis=1)
                if len(last_hold_inits) == 1:
                    last_hold_init = last_hold_inits[0]
                else:
                    last_hold_init = last_hold_inits[0] * config.MIX_COEFFICIENT + last_hold_inits[1] * (1 - config.MIX_COEFFICIENT)
            tgt_hold = act * (last_hold * (1 - config.TURN_MAX_SECOND) + last_hold_init * config.TURN_MAX_SECOND)

    # get to buy and to sell series
    sort_index = td_score[0].sort_values(ascending=False).index  # sort by one of the td_score
    if len(s.hold_dict) == 0:  # first day build position, follow index
        to_buy_s = round(tgt_hold).replace(0, np.nan).reindex(sort_index).dropna()  # sort by score
        to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
        to_sell_s = pd.Series(dtype=float)
    else:
        last_hold = hold_df["amount"].reindex(code_list).fillna(0)
        to_trade_s = round(tgt_hold - last_hold).replace(0, np.nan)
        to_buy_s = to_trade_s[to_trade_s > 0].reindex(sort_index).dropna()  # sort by score
        to_buy_s = round(to_buy_s / td_preclose.reindex(to_buy_s.index))  # transform amount to quantity
        to_sell_s = -to_trade_s[to_trade_s < 0].reindex(sort_index).dropna().iloc[::-1]  # sort by score (different from solve_strategy)
        to_sell_s = round(to_sell_s / td_preclose.reindex(to_sell_s.index))  # transform amount to quantity

    return to_buy_s, to_sell_s

def topn_strategy(s, act, **kwargs):
    """Top-N strategy: Buy top scoring stocks and sell underperforming ones."""

    code_list = kwargs.get('code_list')
    code_list_all = kwargs.get('code_list_all')
    code_list_zt = kwargs.get('code_list_zt')
    td_score = kwargs.get('td_score')
    td_preclose = kwargs.get('td_preclose')
    
    tot_hold_num, daily_sell_num = config.TOT_HOLD_NUM, config.DAILY_SELL_NUM

    if isinstance(td_score, list):
        # get to sell series
        if len(s.hold_dict) == 0:
            last_hold, to_sell_amount, to_sell_s = pd.Series(dtype=float), 0, pd.Series(dtype=float)
        else:
            hold_df, _ = s.close_today()
            last_hold = hold_df["amount"]
            hold_score = td_score[0].reindex(hold_df.index).fillna(-np.inf).sort_values()
            top_codes = td_score[0].reindex(code_list_zt).sort_values(ascending=False).index[:tot_hold_num]
            sell_codes = [x for x in hold_score.index[:daily_sell_num] if x not in top_codes]
            st_sell = [x for x in hold_df.index if x not in code_list]
            sell_codes = list(set(sell_codes + st_sell))
            to_sell_s = hold_df["volume"].reindex(sell_codes).dropna()
            to_sell_amount = hold_df["amount"].reindex(sell_codes).sum()

        # get to buy series
        top_buy_codes = [c for c in td_score[0].reindex(code_list_zt).sort_values(ascending=False).index if c not in last_hold.index]
        sgl_buy_amount = act / tot_hold_num
        buy_num = round((s.cash + to_sell_amount) / sgl_buy_amount)
        if buy_num > 0:
            buy_amount_s = pd.Series([sgl_buy_amount] * buy_num, index=top_buy_codes[:buy_num])
            to_buy_s = (buy_amount_s / td_preclose.reindex(buy_amount_s.index)).round()
        else:
            to_buy_s = pd.Series()

        return to_buy_s, to_sell_s

    elif not isinstance(td_score, list):
        # get to sell series
        if len(s.hold_dict) == 0:
            last_hold, to_sell_amount, to_sell_s = pd.Series(dtype=float), 0, pd.Series(dtype=float)
        else:
            hold_df, _ = s.close_today()
            last_hold = hold_df["amount"]
            hold_score = td_score.reindex(hold_df.index).fillna(-np.inf).sort_values()
            top_codes = td_score.reindex(code_list_zt).sort_values(ascending=False).index[:tot_hold_num]
            sell_codes = [x for x in hold_score.index[:daily_sell_num] if x not in top_codes]
            st_sell = [x for x in hold_df.index if x not in code_list]
            sell_codes = list(set(sell_codes + st_sell))
            to_sell_s = hold_df["volume"].reindex(sell_codes).dropna()
            to_sell_amount = hold_df["amount"].reindex(sell_codes).sum()

        # get to buy series
        top_buy_codes = [c for c in td_score.reindex(code_list_zt).sort_values(ascending=False).index if c not in last_hold.index]
        sgl_buy_amount = act / tot_hold_num
        buy_num = round((s.cash + to_sell_amount) / sgl_buy_amount)
        if buy_num > 0:
            buy_amount_s = pd.Series([sgl_buy_amount] * buy_num, index=top_buy_codes[:buy_num])
            to_buy_s = (buy_amount_s / td_preclose.reindex(buy_amount_s.index)).round()
        else:
            to_buy_s = pd.Series()

        return to_buy_s, to_sell_s

# def record_trade(s, td_price, to_buy_s, to_sell_s, date, act_s, cash_s, buy_s, sell_s, hold_df_dict, trade_df_dict, flag, close_fresh=None):
#     """Record trade: Update portfolio and cash based on trades executed."""
#     s.fresh_price(td_price.to_dict())
#     buy_amount, sell_amount = s.daily_trade(s.cash, to_buy_s, to_sell_s)
#     sellable_amount = s.cal_sellable_amount()
#     if close_fresh is not None:
#         s.fresh_price(close_fresh.to_dict())
#     act_s[date] = s.cal_total()
#     cash_s[date] = s.cash
#     buy_s[date + flag] = buy_amount
#     sell_s[date + flag] = sell_amount
#     hold_df, trade_df = s.close_today()
#     hold_df_dict[date + flag] = hold_df
#     trade_df_dict[date + flag] = trade_df
#     return hold_df, sellable_amount
