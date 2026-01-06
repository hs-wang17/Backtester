import cvxpy as cp
import numpy as np
import pandas as pd


def solve_problem(
    code_list,
    x_last,
    score,
    stk_low,
    stk_high,
    tot_amt,
    sell_max,
    td_mem,
    td_mem_amt,
    td_ind,
    td_ind_up,
    td_ind_down,
    td_cmvg,
    td_cmvg_up,
    td_cmvg_down,
    td_style,
    style_up,
    style_down,
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
    score : pd.Series
        Scores of each stock
    stk_low : pd.Series
        Lower bound of each stock's holding amount
    stk_high : pd.Series
        Upper bound of each stock's holding amount
    tot_amt : float
        Total amount of money available
    sell_max : float
        Maximum amount of money available for selling each day
    td_mem : pd.Series
        Memory of each stock
    td_mem_amt : float
        Amount of memory for each stock
    td_ind : pd.Series
        Industry of each stock
    td_ind_up : pd.Series
        Upper bound of each industry
    td_ind_down : pd.Series
        Lower bound of each industry
    td_cmvg : pd.Series
        CMV of each stock
    td_cmvg_up : pd.Series
        Upper bound of each CMV
    td_cmvg_down : pd.Series
        Lower bound of each CMV
    td_style : pd.Series
        Style of each stock
    style_up : pd.Series
        Upper bound of each style
    style_down : pd.Series
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
        # stock constraints
        x >= make_param(stk_low),
        x <= make_param(stk_high),
        # selling constraints
        cp.sum(cp.abs(x - x_last) - x + x_last) <= 2 * sell_max,
        # total amount constraints
        cp.sum(x) <= tot_amt,
        # membership constraints
        x @ make_param(td_mem) >= td_mem_amt,
        # industry constraints
        x @ make_param(td_ind) <= td_ind_up.values,
        x @ make_param(td_ind) >= td_ind_down.values,
        # cmvg constraints
        x @ make_param(td_cmvg) <= td_cmvg_up.values,
        x @ make_param(td_cmvg) >= td_cmvg_down.values,
        # style constraints
        x @ make_param(td_style) <= style_up.values,
        x @ make_param(td_style) >= style_down.values,
    ]

    prob = cp.Problem(cp.Maximize(x @ make_param(score)), constraints)
    prob.solve(solver=solver, ignore_dpp=True)
    return pd.Series(x.value, index=code_list, dtype=float)
