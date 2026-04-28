import cvxpy as cp
import numpy as np
import pandas as pd


def solve_problem(
    code_list,
    x_last,
    score,
    stk_low,
    stk_high,
    tot_weight,
    sell_max,
    td_mem,  # index member weight vector (dummy variable)
    td_mem_weight,
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
    method="basic",
):
    """
    Solve the portfolio optimization problem using CVXPY.

    Parameters
    ----------
    code_list : list
        List of stock codes
    x_last : pd.Series
        Last day's holding weight
    score : pd.Series
        Scores of each stock
    stk_low : pd.Series
        Lower bound of each stock's holding weight
    stk_high : pd.Series
        Upper bound of each stock's holding weight
    tot_weight : float
        Total weight of money available
    sell_max : float
        Maximum weight of money available for selling each day
    td_mem : pd.Series
        Memember of each stock (dummy variable)
    td_mem_weight : float
        Weight of member for each stock
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
        The optimized holding weight of each stock
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

    if method == "basic":
        x = cp.Variable(len(code_list))
        x.value = make_param(x_last)

        constraints = [
            # stock constraints
            x >= make_param(stk_low),
            x <= make_param(stk_high),
            # selling constraints
            cp.sum(cp.abs(x - x_last) - (x - x_last)) <= 2 * sell_max,
            # total weight constraints
            cp.sum(x) <= tot_weight,
            # membership constraints
            x @ make_param(td_mem) >= td_mem_weight,
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

    elif method == "twostage":
        # --- 1. 变量与基础参数 ---
        x = cp.Variable(len(code_list))
        x.value = make_param(x_last)

        # 预计算参数
        val_ind = make_param(td_ind)
        val_cmvg = make_param(td_cmvg)
        val_style = make_param(td_style)

        # --- 2. 定义松弛变量 (Slacks) ---
        # 这些变量代表“不得不”违背约束的量，必须非负
        s_ind_up = cp.Variable(1, nonneg=True)  # 行业上限突破量
        s_ind_down = cp.Variable(1, nonneg=True)  # 行业下限突破量
        s_cmvg_up = cp.Variable(1, nonneg=True)
        s_cmvg_down = cp.Variable(1, nonneg=True)
        s_style_up = cp.Variable(1, nonneg=True)
        s_style_down = cp.Variable(1, nonneg=True)

        # --- 3. 构建约束体系 ---
        # 3.1 物理硬约束 (绝对不能违背)
        # 包含：个股上下限、总资金、卖出限额、成分股要求
        hard_constraints = [
            x >= make_param(stk_low),
            x <= make_param(stk_high),
            cp.sum(x) <= tot_weight,
            cp.sum(cp.abs(x - x_last) - x + x_last) <= 2 * sell_max,
            x @ make_param(td_mem) >= td_mem_weight,
        ]

        # 3.2 弹性约束 (允许通过松弛变量违背)
        # 逻辑：实际值 <= 上限 + 松弛量 (如果实际值很大，松弛量自动变大)
        elastic_constraints = [
            x @ val_ind <= td_ind_up.values + s_ind_up,
            x @ val_ind >= td_ind_down.values - s_ind_down,
            x @ val_cmvg <= td_cmvg_up.values + s_cmvg_up,
            x @ val_cmvg >= td_cmvg_down.values - s_cmvg_down,
            x @ val_style <= style_up.values + s_style_up,
            x @ val_style >= style_down.values - s_style_down,
        ]

        # --- 4. 第一阶段求解：寻找可行性边界 (Phase 1) ---
        # 目标：最小化所有松弛变量的总和 (尽可能满足约束)
        # 注意：这里完全不看 score，只看可行性
        total_violation = cp.sum(s_ind_up + s_ind_down + s_cmvg_up + s_cmvg_down + s_style_up + s_style_down)
        prob_phase1 = cp.Problem(cp.Minimize(total_violation), hard_constraints + elastic_constraints)

        # 尝试求解 Phase 1
        try:
            # 使用轻量级求解器快速探测边界
            prob_phase1.solve(solver=solver, ignore_dpp=True)
            # 获取最小违规量 (如果是0，说明原问题本身就是可解的；如果是正数，说明原问题无解，这是最小代价)
            min_violation_val = prob_phase1.value
            # 防止数值精度误差导致不可行，稍微放宽一点点 (1e-5)
            if min_violation_val is None:
                min_violation_val = 0
            allowed_violation = max(0, min_violation_val) + 1e-5
        except:
            # 如果第一阶段都挂了，说明硬约束(sell_max等)有冲突，直接设为无限制
            allowed_violation = np.inf

        # --- 5. 第二阶段构建：在可行域内最大化收益 (Phase 2) ---
        # 将“总违规量 <= 第一阶段算出的最小值”作为一个新的硬约束加入
        final_constraints = hard_constraints + elastic_constraints + [total_violation <= allowed_violation]

        # 目标函数回归最原始的 Maximize Score (不含任何惩罚系数)
        prob = cp.Problem(cp.Maximize(x @ make_param(score)), final_constraints)

    prob.solve(solver=solver, ignore_dpp=True)
    return pd.Series(x.value, index=code_list, dtype=float)
