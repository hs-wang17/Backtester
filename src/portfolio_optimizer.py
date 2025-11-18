import cvxpy as cp
import numpy as np
import pandas as pd


def solve_problem(
    code_list,
    x_last,
    score0,
    stk_low0,
    stk_high0,
    tot_amt0,
    sell_max0,
    td_mem0,
    td_mem_amt0,
    td_ind0,
    td_ind_up0,
    td_ind_down0,
    td_cmvg0,
    td_cmvg_up0,
    td_cmvg_down0,
    td_style,
    style_up0,
    style_down0,
    solver="SCIPY",
):
    def make_param(s, fill=0):
        return s.reindex(code_list).replace([np.inf, -np.inf], np.nan).fillna(fill).values

    x = cp.Variable(len(code_list))
    x.value = make_param(x_last)

    constraints = [
        x >= make_param(stk_low0),
        x <= make_param(stk_high0),
        cp.sum(cp.abs(x - x_last) - x + x_last) <= 2 * sell_max0,
        cp.sum(x) <= tot_amt0,
        x @ make_param(td_mem0) >= td_mem_amt0,
        x @ make_param(td_ind0) <= td_ind_up0.values,
        x @ make_param(td_ind0) >= td_ind_down0.values,
        x @ make_param(td_cmvg0) <= td_cmvg_up0.values,
        x @ make_param(td_cmvg0) >= td_cmvg_down0.values,
        x @ make_param(td_style) <= style_up0.values,
        x @ make_param(td_style) >= style_down0.values,
    ]

    prob = cp.Problem(cp.Maximize(x @ make_param(score0)), constraints)
    prob.solve(solver=solver, ignore_dpp=True)
    return pd.Series(x.value, index=code_list)
