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
    """
    Solve the portfolio optimization problem using CVXPY.

    Parameters
    ----------
    code_list : list
        List of stock codes
    x_last : pd.Series
        Last day's holding amount
    score0 : pd.Series
        Scores of each stock
    stk_low0 : pd.Series
        Lower bound of each stock's holding amount
    stk_high0 : pd.Series
        Upper bound of each stock's holding amount
    tot_amt0 : float
        Total amount of money available
    sell_max0 : float
        Maximum amount of money available for selling each day
    td_mem0 : pd.Series
        Memory of each stock
    td_mem_amt0 : float
        Amount of memory for each stock
    td_ind0 : pd.Series
        Industry of each stock
    td_ind_up0 : pd.Series
        Upper bound of each industry
    td_ind_down0 : pd.Series
        Lower bound of each industry
    td_cmvg0 : pd.Series
        CMV of each stock
    td_cmvg_up0 : pd.Series
        Upper bound of each CMV
    td_cmvg_down0 : pd.Series
        Lower bound of each CMV
    td_style : pd.Series
        Style of each stock
    style_up0 : pd.Series
        Upper bound of each style
    style_down0 : pd.Series
        Lower bound of each style
    solver : str
        Solver to use. Can be "SCIPY" or "GUROBI".

    Returns
    -------
    pd.Series
        The optimized holding amount of each stock
    """

    def make_param(s, fill=0):
        """
        Make a parameter for optimization.

        Parameters
        ----------
        s : pd.Series
            The parameter to make
        fill : float, optional
            The value to fill NaN with. Defaults to 0.

        Returns
        -------
        pd.Series
            The parameter with NaN values replaced
        """
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
