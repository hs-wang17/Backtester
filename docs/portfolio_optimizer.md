# 组合优化模块详解

`src/portfolio_optimizer.py` 模块提供了基于 CVXPY 的组合优化功能，用于在各种约束条件下构建最优投资组合。

## 核心功能

- 使用 CVXPY 求解组合优化问题
- 支持多种约束条件：个股上下限、行业偏离、市值偏离、风格因子偏离等
- 支持两种优化方法：基础方法和两阶段方法
- 处理约束条件的可行性问题

## 主要函数

### `solve_problem()`

**功能**：使用 CVXPY 求解投资组合优化问题

**参数**：

| 参数 | 类型 | 描述 |
|------|------|------|
| `code_list` | list | 股票代码列表 |
| `x_last` | pd.Series | 上一日持仓权重 |
| `score` | pd.Series | 股票评分 |
| `stk_low` | pd.Series | 个股权重下限 |
| `stk_high` | pd.Series | 个股权重上限 |
| `tot_weight` | float | 总可用权重 |
| `sell_max` | float | 每日最大卖出权重 |
| `td_mem` | pd.Series | 成分股标识 |
| `td_mem_weight` | float | 成分股最低权重 |
| `td_ind` | pd.Series | 行业因子暴露 |
| `td_ind_up` | pd.Series | 行业因子上限 |
| `td_ind_down` | pd.Series | 行业因子下限 |
| `td_cmvg` | pd.Series | 市值因子暴露 |
| `td_cmvg_up` | pd.Series | 市值因子上限 |
| `td_cmvg_down` | pd.Series | 市值因子下限 |
| `td_style` | pd.Series | 风格因子暴露 |
| `style_up` | pd.Series | 风格因子上限 |
| `style_down` | pd.Series | 风格因子下限 |
| `solver` | str | 求解器，可选 "SCIPY" 或 "GUROBI" |
| `method` | str | 优化方法，可选 "basic" 或 "twostage" |

**返回值**：
- `pd.Series`：优化后的持仓权重

## 优化方法

### 1. 基础方法 (basic)

**原理**：直接构建优化问题，包含所有约束条件，目标是最大化组合评分。

**约束条件**：
- 个股权重上下限
- 每日卖出限制
- 总权重限制
- 成分股权重限制
- 行业因子偏离限制
- 市值因子偏离限制
- 风格因子偏离限制

### 2. 两阶段方法 (twostage)

**原理**：
1. 第一阶段：最小化约束违背量，找到可行域边界
2. 第二阶段：在可行域内最大化组合评分

**优势**：
- 处理约束条件可能冲突的情况
- 提高优化的稳定性和可靠性
- 避免因约束冲突导致优化失败

## 代码实现

### 辅助函数

```python
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
```

### 基础方法实现

```python
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
```

### 两阶段方法实现

```python
elif method == "twostage":
    # --- 1. 变量与基础参数 ---
    x = cp.Variable(len(code_list))
    x.value = make_param(x_last)

    # 预计算参数
    val_ind = make_param(td_ind)
    val_cmvg = make_param(td_cmvg)
    val_style = make_param(td_style)

    # --- 2. 定义松弛变量 (Slacks) ---
    # 这些变量代表"不得不"违背约束的量，必须非负
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
    # 将"总违规量 <= 第一阶段算出的最小值"作为一个新的硬约束加入
    final_constraints = hard_constraints + elastic_constraints + [total_violation <= allowed_violation]

    # 目标函数回归最原始的 Maximize Score (不含任何惩罚系数)
    prob = cp.Problem(cp.Maximize(x @ make_param(score)), final_constraints)
```

### 求解与返回

```python
prob.solve(solver=solver, ignore_dpp=True)
return pd.Series(x.value, index=code_list, dtype=float)
```

## 约束条件详解

### 1. 个股约束

| 约束 | 说明 | 公式 |
|------|------|------|
| 个股下限 | 个股最小持仓权重 | `x >= stk_low` |
| 个股上限 | 个股最大持仓权重 | `x <= stk_high` |
| 卖出限制 | 每日最大卖出权重 | `cp.sum(cp.abs(x - x_last) - (x - x_last)) <= 2 * sell_max` |

### 2. 总权重约束

| 约束 | 说明 | 公式 |
|------|------|------|
| 总权重 | 总可用资金权重 | `cp.sum(x) <= tot_weight` |

### 3. 成分股约束

| 约束 | 说明 | 公式 |
|------|------|------|
| 成分股最低权重 | 成分股持仓比例要求 | `x @ td_mem >= td_mem_weight` |

### 4. 行业因子约束

| 约束 | 说明 | 公式 |
|------|------|------|
| 行业上限 | 行业因子暴露上限 | `x @ td_ind <= td_ind_up` |
| 行业下限 | 行业因子暴露下限 | `x @ td_ind >= td_ind_down` |

### 5. 市值因子约束

| 约束 | 说明 | 公式 |
|------|------|------|
| 市值上限 | 市值因子暴露上限 | `x @ td_cmvg <= td_cmvg_up` |
| 市值下限 | 市值因子暴露下限 | `x @ td_cmvg >= td_cmvg_down` |

### 6. 风格因子约束

| 约束 | 说明 | 公式 |
|------|------|------|
| 风格上限 | 风格因子暴露上限 | `x @ td_style <= style_up` |
| 风格下限 | 风格因子暴露下限 | `x @ td_style >= style_down` |

## 示例代码

### 基本使用

```python
from src.portfolio_optimizer import solve_problem
import pandas as pd
import numpy as np

# 准备数据
code_list = ['000001', '000002', '000003', '000004', '000005']
x_last = pd.Series([0.1, 0.1, 0.1, 0.1, 0.1], index=code_list)
score = pd.Series([0.9, 0.8, 0.7, 0.6, 0.5], index=code_list)
stk_low = pd.Series([0.0, 0.0, 0.0, 0.0, 0.0], index=code_list)
stk_high = pd.Series([0.2, 0.2, 0.2, 0.2, 0.2], index=code_list)
tot_weight = 1.0
sell_max = 0.2

# 模拟因子数据
td_mem = pd.Series([1, 1, 0, 0, 0], index=code_list)
td_mem_weight = 0.5
td_ind = pd.DataFrame({
    'industry1': [1, 0, 1, 0, 0],
    'industry2': [0, 1, 0, 1, 1]
}, index=code_list)
td_ind_up = pd.Series([0.6, 0.6], index=['industry1', 'industry2'])
td_ind_down = pd.Series([0.4, 0.4], index=['industry1', 'industry2'])
td_cmvg = pd.Series([100, 200, 300, 400, 500], index=code_list)
td_cmvg_up = pd.Series([350])
td_cmvg_down = pd.Series([250])
td_style = pd.DataFrame({
    'value': [0.5, 0.6, 0.7, 0.8, 0.9],
    'growth': [0.9, 0.8, 0.7, 0.6, 0.5]
}, index=code_list)
style_up = pd.Series([0.7, 0.7], index=['value', 'growth'])
style_down = pd.Series([0.3, 0.3], index=['value', 'growth'])

# 求解优化问题
result = solve_problem(
    code_list=code_list,
    x_last=x_last,
    score=score,
    stk_low=stk_low,
    stk_high=stk_high,
    tot_weight=tot_weight,
    sell_max=sell_max,
    td_mem=td_mem,
    td_mem_weight=td_mem_weight,
    td_ind=td_ind,
    td_ind_up=td_ind_up,
    td_ind_down=td_ind_down,
    td_cmvg=td_cmvg,
    td_cmvg_up=td_cmvg_up,
    td_cmvg_down=td_cmvg_down,
    td_style=td_style,
    style_up=style_up,
    style_down=style_down,
    solver="SCIPY",
    method="basic"
)

# 查看结果
print("优化后的持仓权重:")
print(result)
print("\n总权重:", result.sum())
```

### 使用两阶段方法

```python
# 使用两阶段方法求解
result = solve_problem(
    # 相同参数...
    method="twostage"
)

print("两阶段方法优化后的持仓权重:")
print(result)
```

## 性能优化

1. **参数预计算**：在两阶段方法中，预计算因子暴露值，减少重复计算
2. **松弛变量**：使用松弛变量处理约束冲突，提高优化稳定性
3. **求解器选择**：支持 SCIPY 和 GUROBI 求解器，可根据问题复杂度选择
4. **初始化**：设置变量初始值为上一日权重，加速收敛

## 扩展建议

1. **支持更多约束类型**：可添加流动性约束、波动率约束等
2. **多目标优化**：可实现多目标优化，如同时最大化收益和最小化风险
3. **自定义目标函数**：可支持自定义目标函数，如风险调整收益最大化
4. **并行优化**：可实现并行优化，提高大规模问题的求解速度
5. **优化历史记录**：可添加优化历史记录，便于分析优化过程