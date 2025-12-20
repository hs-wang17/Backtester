# 投资组合优化器 (Portfolio Optimizer) 详细说明

## 概述

`portfolio_optimizer.py` 是一个基于凸优化 (Convex Optimization) 的投资组合优化模块，使用 CVXPY 库来实现复杂的约束条件下的投资组合权重分配。该模块旨在最大化投资组合的预期收益，同时满足多种风险控制和合规性约束。

## 算法原理

### 目标函数

投资组合优化的核心目标是最大化投资组合的预期收益：

$$\max_{\boldsymbol{x}\in\mathbb{R}^I} \quad \boldsymbol{x}^\top \boldsymbol{s}.$$

其中：

- $\boldsymbol{x}\in\mathbb{R}^I$ 是优化变量，表示各股票的持仓金额向量。
- $\boldsymbol{s}\in\mathbb{R}^I$ 是各股票的预期收益向量（预测标签）。

### 约束条件

#### 1. 个股持仓限制

对于每只股票 $i\in\mathcal{I}$，持仓金额必须在预设的上下界之间：

$$x_i^{\mathrm{low}} \leqslant x_i \leqslant x_i^{\mathrm{high}}, \quad \forall i\in\mathcal{I}.$$

#### 2. 总资金约束

投资组合的总持仓金额不能超过可用资金：

$$\sum_{i\in\mathcal{I}} x_i \leqslant \mathrm{tot\_amt}.$$

#### 3. 卖出约束

每日卖出金额受限制：

$$\sum_{i} |x_i - x_i^{\mathrm{last}}| - x_i + x_i^{\mathrm{last}} \leqslant 2 \cdot \mathrm{sell\_max}.$$

这个约束的数学含义是：

- $|x_i - x_i^{\mathrm{last}}| - x_i + x_i^{\mathrm{last}}$ 等价于 $\max(0, x_i^{\mathrm{last}} - x_i)$。
- 即只计算卖出部分的金额，总卖出金额不能超过 $\mathrm{sell\_max}$。

#### 4. 行业约束

投资组合在各个行业 $j\in\mathcal{J}$ 的暴露必须在预设范围内：

$$\mathrm{ind\_down}_j \leqslant \sum_{i \in \mathcal{I}} x_i \cdot \mathrm{ind}_{i,j} \leqslant \mathrm{ind\_up}_j, \quad \forall j\in\mathcal{J}.$$

其中 $\mathrm{ind}_{i,j}$ 表示股票 $i$ 属于行业 $j$ 的标识（0 或 1）。

#### 5. 市值约束

投资组合在各个市值区间 $k\in\mathcal{K}$ 的暴露必须在预设范围内：

$$\mathrm{cmvg\_down}_k \leqslant \sum_{i \in \mathcal{I}} x_i \cdot \mathrm{cmvg}_{i,k} \leqslant \mathrm{cmvg\_up}_k, \quad \forall k\in\mathcal{K}.$$

其中 $\mathrm{cmvg}_{i,k}$ 表示股票 $i$ 在市值区间 $k$ 中的暴露。

#### 6. 风格约束

投资组合在各个风格 $l\in\mathcal{L}$ 的暴露必须在预设范围内：

$$\mathrm{style\_down}_l \leqslant \sum_{i \in \mathcal{I}} x_i \cdot \mathrm{style}_{i,l} \leqslant \mathrm{style\_up}_l, \quad \forall l\in\mathcal{L}.$$

其中 $\mathrm{style}_{i,l}$ 表示股票 $i$ 在风格 $l$ 上的暴露。

#### 7. 成员约束

确保对成员股票的最小配置：

$$\sum_{i \in \mathcal{I}} x_i \cdot \mathrm{mem}_i \geqslant \mathrm{mem\_amt}.$$

其中 $\mathrm{mem}_i$ 表示股票 $i$ 是否为成员股票。

## 代码结构分析

### 核心函数：solve_problem()

```python
def solve_problem(
    code_list, x_last, s0, stk_low0, stk_high0, tot_amt0, sell_max0,
    td_mem0, td_mem_amt0, td_ind0, td_ind_up0, td_ind_down0,
    td_cmvg0, td_cmvg_up0, td_cmvg_down0, td_style, style_up0, style_down0,
    solver="SCIPY"
):
```

#### 参数说明

| 参数            | 类型      | 说明                  |
| --------------- | --------- | --------------------- |
| `code_list`     | list      | 股票代码列表          |
| `x_last`        | pd.Series | 前一交易日持仓金额    |
| `s0`            | pd.Series | 各股票预期收益评分    |
| `stk_low0`      | pd.Series | 个股持仓下限          |
| `stk_high0`     | pd.Series | 个股持仓上限          |
| `tot_amt0`      | float     | 总可用资金            |
| `sell_max0`     | float     | 每日最大卖出金额      |
| `td_mem0`       | pd.Series | 成员股票标识          |
| `td_mem_amt0`   | float     | 成员股票最小配置金额  |
| `td_ind0`       | pd.Series | 行业分类              |
| `td_ind_up0`    | pd.Series | 行业暴露上限          |
| `td_ind_down0`  | pd.Series | 行业暴露下限          |
| `td_cmvg0`      | pd.Series | 市值分类              |
| `td_cmvg_up0`   | pd.Series | 市值暴露上限          |
| `td_cmvg_down0` | pd.Series | 市值暴露下限          |
| `td_style`      | pd.Series | 风格因子              |
| `style_up0`     | pd.Series | 风格因子暴露上限      |
| `style_down0`   | pd.Series | 风格因子暴露下限      |
| `solver`        | str       | 优化求解器（"SCIPY"） |

### 预处理辅助函数：make_param()

```python
def make_param(s, fill=0):
    return s.reindex(code_list).replace([np.inf, -np.inf], np.nan).fillna(fill).values
```

该函数用于数据预处理：

1. **重索引**：确保参数与股票代码列表对齐
2. **处理无穷值**：将无穷大值替换为 NaN
3. **填充缺失值**：用指定值（默认为 0）填充 NaN
4. **返回数组**：转换为 NumPy 数组供 CVXPY 使用

### 求解过程

```python
# 1. 定义优化变量
x = cp.Variable(len(code_list))
x.value = make_param(x_last)  # 设置初始解

# 2. 构建约束条件
constraints = [
    # 个股约束
    x >= make_param(stk_low0),
    x <= make_param(stk_high0),

    # 卖出约束
    cp.sum(cp.abs(x - x_last) - x + x_last) <= 2 * sell_max0,

    # 总资金约束
    cp.sum(x) <= tot_amt0,

    # 成员约束
    x @ make_param(td_mem0) >= td_mem_amt0,

    # 行业约束
    x @ make_param(td_ind0) <= td_ind_up0.values,
    x @ make_param(td_ind0) >= td_ind_down0.values,

    # 市值约束
    x @ make_param(td_cmvg0) <= td_cmvg_up0.values,
    x @ make_param(td_cmvg0) >= td_cmvg_down0.values,

    # 风格约束
    x @ make_param(td_style) <= style_up0.values,
    x @ make_param(td_style) >= style_down0.values,
]

# 3. 构建并求解优化问题
prob = cp.Problem(cp.Maximize(x @ make_param(s0)), constraints)
prob.solve(solver=solver, ignore_dpp=True)
```
