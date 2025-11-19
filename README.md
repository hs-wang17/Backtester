# 📈 Backtester 项目

本项目是一个基于 Python 的量化投资策略回测框架，旨在对多因子策略进行历史回测、组合优化、持仓管理和可视化分析。框架支持股票日度数据回测，并集成了交易约束、持仓优化和策略绩效评估功能。

---

## 功能特性 ✨

1. **账户与交易管理 💰**

   - `account.py` 提供账户和股票持仓管理类，实现买入、卖出、每日刷新及交易日志记录。
   - 支持交易费、最小交易量、单位交易量等约束。

2. **数据加载 📊**

   - `data_loader.py` 负责从 feather 文件和 CSV 文件加载日度行情、前复权因子、涨跌停价及指数数据。
   - 支持每日可交易股票筛选及复权处理。

3. **组合优化 🧮**

   - `portfolio_optimizer.py` 使用 CVXPY 求解组合优化问题。
   - 支持多种约束，包括持仓上下限、行业/风格暴露、最大换手率、指数成分偏离等。

4. **回测引擎 ⚡**

   - `backtest.py` 提供完整回测流程：

     - 开盘前刷新账户信息
     - 基于组合优化生成每日目标持仓
     - 执行买卖操作并更新账户
     - 记录每日账户净值、现金、持仓和交易明细

   - 支持多策略和指数比较。

5. **策略分析 📈**

   - `analysis.py` 提供绩效指标计算，包括年化收益、波动、夏普比率、最大回撤、Calmar 比率、信息比率等。
   - 计算超额收益及连续亏损天数等指标。

6. **可视化 🎨**

   - `plot.py` 支持 Matplotlib PNG 和 Plotly 交互 HTML 绘图。
   - 可展示策略净值、超额收益及关键回测指标。

7. **工具函数 🔧**

   - `utils.py` 提供每日行情和支持数据缓存、加载工具，提升回测效率。

---

## 安装依赖 🛠

```bash
# 建议使用 Python 3.12
pip install pandas numpy matplotlib plotly cvxpy tqdm
```

---

## 项目结构 💼

```
backtester/
│-- run.py                # 回测入口
└-- src
    │-- account.py        # 账户和股票管理 💰
    │-- analysis.py       # 回测结果分析 📈
    │-- backtest.py       # 回测主逻辑 ⚡
    │-- config.py         # 配置参数 ⚙️
    │-- data_loader.py    # 数据加载 📊
    │-- plot.py           # 回测可视化 🎨
    │-- portfolio_optimizer.py  # 组合优化 🧮
    └-- utils.py          # 工具函数 🔧
```

## 使用说明 📝

1. **配置数据与参数 ⚙️**

   - 修改 `config.py` 中的 `DATA_PATH`、`DAILY_DATA_PATH`、`SCORES_PATH` 等路径为本地数据文件路径。
   - 设置回测参数，如初始资金、持仓限制、买入比例等。

2. **准备数据 📂**

   - 日度行情数据 feather 文件：`stk_close`, `stk_preclose`, `stk_adjfactor`, `stk_ztprice`, `stk_dtprice` 等。
   - 指数数据 feather 文件：`idx_close`。
   - 策略评分 CSV 文件：`predictions/SCORES_PATH.csv`。
   - 支持文件 feather 文件：风格、行业、指数成分等。

3. **运行回测 ▶️**

```bash
python run.py --scores_path "SCORES_PATH.csv"
```

- 回测完成后，会生成以下结果文件：

  - PNG 报告 📄

    ![策略回测 PNG](./image/chart.png)

  - HTML 交互报告 🌐

    可在浏览器中交互查看净值曲线、超额收益及关键指标。

4. **查看回测数据 📊**

   - `run_backtest()` 返回回测结果字典：

     ```python
     result = run_backtest()
     print(result["info"])  # 回测总览指标
     print(result["tot_account_s"])  # 每日账户净值、现金、买卖金额
     print(result["hold_style"])  # 持仓风格偏离
     ```

## 回测指标说明 📊

- **年化收益 / 年化波动 / 夏普比率**
- **累计收益 / 最大回撤 / Calmar 比率**
- **超额年化收益 / 信息比率 / 超额最大回撤**
- **最大连续亏损天数 / 胜率(天)**

---

## 快速示例 💡

```python
from src.backtest import run_backtest

result = run_backtest()

# 查看回测总览
print(result["info"])
```

- PNG 报告路径示例：

  ```
  results/backtests/StockPredictor.png
  ```

- HTML 交互报告路径示例：

  ```
  results/backtests/StockPredictor.html
  ```

---

## 开发与扩展 🔧

- 可扩展多策略回测，只需替换 `scores` 文件。
- 可增加交易约束，如单日最大交易量、不同交易费率等。
- 可将组合优化替换为其他优化模型或引入机器学习预测信号。
