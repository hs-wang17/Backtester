# 策略分析模块详解

`src/analysis.py` 模块提供了完整的策略绩效分析功能，用于评估回测结果的各项指标。

## 核心功能

- 计算策略的绝对收益指标
- 计算策略的相对收益指标
- 分年度分析超额收益
- 生成详细的绩效评估报告

## 主要函数

### `analyse(net_value)`

**功能**：分析策略绩效，计算各项指标

**参数**：
- `net_value`：DataFrame，包含 'strategy' 和 'zs' 列，分别表示策略净值和基准净值

**返回值**：
- `info`：pd.Series，包含策略各项绩效指标
- `net_value_df`：pd.DataFrame，策略与基准净值
- `relative_net_value`：pd.Series，超额净值曲线

## 指标计算详解

### 1. 绝对收益指标

| 指标 | 计算公式 | 说明 |
|------|----------|------|
| 年化收益 | `abs_ret.mean() * 250` | 日收益率的平均值乘以250个交易日 |
| 年化波动 | `abs_ret.std() * np.sqrt(250)` | 日收益率的标准差乘以根号250 |
| 夏普比率 | `abs_mean_ret / abs_std_ret` | 年化收益除以年化波动 |
| 累计收益 | `abs_ret.cumsum().iloc[-1]` | 日收益率的累计和 |
| 最大回撤 | `(abs_cum_ret.cummax() - abs_cum_ret).max()` | 累计收益的峰值到谷值的最大跌幅 |
| 平均回撤 | `abs_dd[abs_dd > 0].mean()` | 所有正回撤的平均值 |
| 胜率(天) | `(abs_ret > 0).mean()` | 日收益率为正的比例 |

### 2. 相对收益指标

| 指标 | 计算公式 | 说明 |
|------|----------|------|
| 超额年化收益 | `rel_ret.mean() * 250` | 策略与基准日收益率差的平均值乘以250 |
| 超额年化波动 | `rel_ret.std() * np.sqrt(250)` | 超额日收益率的标准差乘以根号250 |
| 信息比率 | `rel_mean_ret / rel_std_ret` | 超额年化收益除以超额年化波动 |
| 超额累计收益 | `rel_ret.cumsum().iloc[-1]` | 超额日收益率的累计和 |
| 超额最大回撤 | `(rel_cum_ret.cummax() - rel_cum_ret).max()` | 超额累计收益的峰值到谷值的最大跌幅 |
| 超额平均回撤 | `rel_dd[rel_dd > 0].mean()` | 所有正超额回撤的平均值 |
| 超额胜率(天) | `(rel_ret > 0).mean()` | 超额日收益率为正的比例 |

### 3. 分年度指标

| 指标 | 说明 |
|------|------|
| 逐年超额年化收益 | 每年的超额年化收益 |
| 逐年超额年化波动 | 每年的超额年化波动 |
| 逐年超额最大回撤 | 每年的超额最大回撤 |
| 逐年信息比率 | 每年的信息比率 |

## 代码实现

```python
def analyse(net_value):
    """
    Backtest Performance Analyzer
    ------------------------------------------------
    输入：
        net_value: DataFrame 必须包含 'strategy' 和 'zs'
    输出：
        info: pd.Series                     - 策略指标
        net_value_df: pd.DataFrame          - 策略与基准净值
        relative_net_value: pd.Series       - 超额净值曲线
    """
    # 绝对指标
    abs_ret = net_value["strategy"].pct_change().dropna()
    abs_mean_ret = abs_ret.mean() * 250
    abs_std_ret = abs_ret.std() * np.sqrt(250)
    abs_sharpe = abs_mean_ret / abs_std_ret if abs_std_ret > 0 else np.nan  # 夏普指数
    abs_cum_ret = abs_ret.cumsum()
    abs_dd = abs_cum_ret.cummax() - abs_cum_ret
    abs_max_dd = abs_dd.max()
    abs_mean_dd = abs_dd[abs_dd > 0].mean()
    abs_win_rate = (abs_ret > 0).mean()

    # 相对指标
    zs_ret = net_value["zs"].pct_change().dropna()
    rel_ret = abs_ret - zs_ret
    if config.REMOVE_ABNORMAL:
        rel_ret.loc["20240130":"20240219"] = 0.0
    rel_mean_ret = rel_ret.mean() * 250
    rel_std_ret = rel_ret.std() * np.sqrt(250)
    rel_sharpe = rel_mean_ret / rel_std_ret if rel_std_ret > 0 else np.nan  # 信息比率
    rel_cum_ret = rel_ret.cumsum()
    rel_dd = rel_cum_ret.cummax() - rel_cum_ret
    if config.REMOVE_ABNORMAL:
        rel_dd.loc["20240130":"20240219"] = 0.0
    rel_max_dd = rel_dd.max()
    rel_mean_dd = rel_dd[rel_dd > 0].mean()
    rel_win_rate = (rel_ret > 0).mean()

    # 部分相对指标（分年度）
    df = pd.concat([abs_ret, zs_ret], axis=1)
    df.columns = ["strategy", "zs"]
    df["excess"] = df["strategy"] - df["zs"]
    df.index = pd.to_datetime(df.index.astype(str), format="%Y%m%d")
    ex_ret, ex_std, ex_max_dd, ir = {}, {}, {}, {}
    for year, y_ret in df["excess"].groupby(df.index.year):
        mean_y = y_ret.mean() * 250
        std_y = y_ret.std() * np.sqrt(250)
        ex_ret[year] = mean_y
        ex_std[year] = std_y
        ex_max_dd[year] = (y_ret.cumsum().cummax() - y_ret.cumsum()).max()
        ir[year] = mean_y / std_y if std_y > 0 else np.nan

    info = pd.Series(
        {
            "年化收益": abs_mean_ret,
            "年化波动": abs_std_ret,
            "夏普比率": abs_sharpe,
            "累计收益": abs_cum_ret.iloc[-1],
            "最大回撤": abs_max_dd,
            "平均回撤": abs_mean_dd,
            "胜率(天)": abs_win_rate,
            "超额年化收益": rel_mean_ret,
            "超额年化波动": rel_std_ret,
            "信息比率": rel_sharpe,
            "超额累计收益": rel_cum_ret.iloc[-1],
            "超额最大回撤": rel_max_dd,
            "超额平均回撤": rel_mean_dd,
            "超额胜率(天)": rel_win_rate,
            "逐年超额年化收益": ex_ret,
            "逐年超额年化波动": ex_std,
            "逐年超额最大回撤": ex_max_dd,
            "逐年信息比率": ir,
        }
    )

    net_value = pd.concat([net_value["strategy"], net_value["zs"]], axis=1)
    relative_net_value = 1 + rel_cum_ret

    return info, net_value, relative_net_value
```

## 异常处理

代码中包含了对异常时期数据的处理：

```python
if config.REMOVE_ABNORMAL:
    rel_ret.loc["20240130":"20240219"] = 0.0
    rel_dd.loc["20240130":"20240219"] = 0.0
```

这段代码用于移除 2024 年 1 月 30 日至 2 月 19 日期间的异常数据，将该期间的超额收益和超额回撤设为 0。

## 示例代码

### 基本使用

```python
from src.analysis import analyse
import pandas as pd

# 准备净值数据
net_value = pd.DataFrame({
    'strategy': [1.0, 1.02, 1.05, 1.03, 1.06],
    'zs': [1.0, 1.01, 1.02, 1.01, 1.03]
}, index=['20230101', '20230102', '20230103', '20230104', '20230105'])

# 分析绩效
info, net_value_df, relative_net_value = analyse(net_value)

# 查看结果
print("年化收益:", info["年化收益"])
print("夏普比率:", info["夏普比率"])
print("最大回撤:", info["最大回撤"])
print("信息比率:", info["信息比率"])
```

### 与回测引擎结合使用

```python
from src.backtest import run_backtest

# 运行回测
result = run_backtest()

# 查看分析结果
info = result["info"]
print("策略绩效指标:")
print(info)

# 查看逐年超额收益
print("\n逐年超额年化收益:")
for year, value in info["逐年超额年化收益"].items():
    print(f"{year}: {value:.2%}")
```

## 输出格式

分析结果 `info` 是一个 pandas Series，包含以下字段：

| 字段 | 类型 | 描述 |
|------|------|------|
| 年化收益 | float | 策略的年化收益率 |
| 年化波动 | float | 策略的年化波动率 |
| 夏普比率 | float | 策略的夏普比率 |
| 累计收益 | float | 策略的累计收益率 |
| 最大回撤 | float | 策略的最大回撤 |
| 平均回撤 | float | 策略的平均回撤 |
| 胜率(天) | float | 策略日收益率为正的比例 |
| 超额年化收益 | float | 策略相对基准的年化超额收益 |
| 超额年化波动 | float | 策略相对基准的年化超额波动率 |
| 信息比率 | float | 策略的信息比率 |
| 超额累计收益 | float | 策略相对基准的累计超额收益 |
| 超额最大回撤 | float | 策略相对基准的最大超额回撤 |
| 超额平均回撤 | float | 策略相对基准的平均超额回撤 |
| 超额胜率(天) | float | 策略超额日收益率为正的比例 |
| 逐年超额年化收益 | dict | 每年的超额年化收益 |
| 逐年超额年化波动 | dict | 每年的超额年化波动率 |
| 逐年超额最大回撤 | dict | 每年的超额最大回撤 |
| 逐年信息比率 | dict | 每年的信息比率 |

## 性能优化

1. **向量化计算**：使用 pandas 和 numpy 的向量化操作，提高计算效率
2. **数据处理**：使用 dropna() 处理缺失数据，确保计算的准确性
3. **异常处理**：对异常时期的数据进行特殊处理，提高分析的可靠性

## 扩展建议

1. **添加更多指标**：可添加更多绩效指标，如 Calmar 比率、Sortino 比率等
2. **支持多基准**：可支持多个基准的比较分析
3. **风险分解**：可添加风险分解功能，分析不同因子对风险的贡献
4. **压力测试**：可添加压力测试功能，评估策略在极端市场环境下的表现