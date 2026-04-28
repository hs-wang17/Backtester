# 回测引擎详解

`src/backtest.py` 模块是整个回测系统的核心，负责执行完整的回测流程，包括数据加载、策略执行、交易管理和结果分析。

## 核心功能

### 1. 数据加载与预处理

- 加载日度行情数据（收盘价、涨跌停价、前收盘价、复权因子）
- 加载策略评分数据
- 计算当日可交易股票列表
- 处理复权和价格调整

### 2. 回测流程

1. **初始化**：创建账户对象，设置初始资金
2. **每日循环**：
   - 获取当日数据
   - 开盘前刷新账户信息
   - 执行策略生成买卖信号
   - 执行交易操作
   - 记录持仓和交易数据
   - 计算持仓风格偏离
3. **结果分析**：计算绩效指标，生成可视化报告

### 3. 支持的策略

- **solve 策略**：使用 CVXPY 进行组合优化
- **topn 策略**：基于评分选择 Top N 股票

## 主要函数

### 1. `load_daily_data(name)`

**功能**：加载日度数据文件

**参数**：
- `name`：数据文件名（不包含扩展名）

**返回值**：
- `pd.DataFrame`：加载的数据

### 2. `run_backtest()`

**功能**：执行完整的回测流程

**参数**：
- 无（使用 config 模块中的配置参数）

**返回值**：
- 字典：包含回测结果信息

## 回测流程详解

### 1. 数据准备阶段

```python
# 加载基础数据
high_limit = load_daily_data("stk_ztprice").replace(0, np.nan).ffill()
low_limit = load_daily_data("stk_dtprice").replace(0, np.nan).ffill()
pre_close = load_daily_data("stk_preclose").replace(0, np.nan).ffill()
adj_factor = load_daily_data("stk_adjfactor").replace(0, np.nan).ffill()
close = load_daily_data("stk_close").replace(0, np.nan).ffill()

# 计算涨停股票
last_zt_df = (close == high_limit).shift(1).fillna(False).astype(int)

# 调整价格限制
upper_price = pre_close + 0.9 * (high_limit - pre_close)
lower_price = pre_close + 0.9 * (low_limit - pre_close)
adj = adj_factor / adj_factor.shift(1)

# 加载指数数据
zs_day = load_daily_data("idx_close")[config.IDX_NAME_CN].dropna()

# 加载 VWAP/TWAP 数据
if config.AFTERNOON_START:
    if not config.TWAP_MODE:
        vwap_df = pd.read_feather(os.path.join(config.DATA_PATH, "vwap_noon.fea"))
    else:
        vwap_df = pd.read_feather(os.path.join(config.DATA_PATH, "twap_noon.fea"))
else:
    if not config.TWAP_MODE:
        vwap_df = pd.read_feather(os.path.join(config.DATA_PATH, "vwap.fea"))
    else:
        vwap_df = pd.read_feather(os.path.join(config.DATA_PATH, "twap.fea"))
```

### 2. 策略评分处理

```python
scores, index_sets, col_sets = [], [], []
for path in config.SCORES_PATH:
    if config.AFTERNOON_START or config.CALL_START:
        scores_single = pd.read_csv(path, index_col=0).T.sort_index().dropna(how="all")
    else:
        scores_single = pd.read_csv(path, index_col=0).T.sort_index().shift(1).dropna(how="all")
    scores_single.columns = scores_single.columns.astype(str).str.zfill(6)
    scores_single = scores_single[scores_single.columns[scores_single.columns.str[0].isin(["0", "3", "6"])]]
    scores_single.index = scores_single.index.astype(str)
    scores.append(scores_single)
    index_sets.append(set(scores_single.index))
    col_sets.append(set(scores_single.columns))

# 计算共同日期和股票
common_dates = sorted(set.intersection(*index_sets) & set(vwap_df.index.astype(str)))
common_cols = sorted(set.intersection(*col_sets))
scores = [df.loc[common_dates, common_cols] for df in scores]
date_list = common_dates[config.START_DATE_SHIFT :]
```

### 3. 账户初始化

```python
s = account(config.INITIAL_MONEY)
s.cal_total()

# 初始化结果存储
act_s = {}  # 账户总资产
cash_s = {}  # 现金
buy_s = {}   # 买入金额
sell_s = {}  # 卖出金额
hold_df_dict = {}  # 每日持仓
trade_df_dict = {}  # 每日交易
hold_style_dict = {}  # 每日持仓风格偏离
```

### 4. 每日回测循环

```python
for date in tqdm(date_list, desc="Backtesting"):
    # 获取当日数据
    td_open, td_close, td_preclose, td_adj, td_score, td_upper, td_lower, last_zt = get_daily_price(
        str(date), vwap_df, close, pre_close, adj, scores, upper_price, lower_price, last_zt_df
    )

    # 获取当日支持数据
    if config.TRADE_SUPPORT == 5:
        td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support5(str(date))
    elif config.TRADE_SUPPORT == 7:
        td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support7(str(date))
    else:
        td_citic, td_cmvg, td_mem, zz_citic, zz_cmvg, style_fac, zz_style, sub_code_list = get_daily_support_barra(str(date))

    # 处理指数成分股
    mem_hs300, mem_zz500, mem_zz1000, mem_zz2000 = td_mem
    if config.IDX_NAME == "hs300":
        td_mem = mem_hs300
    elif config.IDX_NAME == "zz500":
        td_mem = mem_zz500
    elif config.IDX_NAME == "zz1000":
        td_mem = mem_zz1000
    elif config.IDX_NAME == "zz2000":
        td_mem = mem_zz2000

    # 计算可交易股票
    code_list_all = pd.concat([td_upper, td_lower, td_close, td_open], axis=1).dropna(how="any").index.tolist()
    code_list = [
        x for x in code_list_all if (x in sub_code_list) and (x[0] not in ["4", "8"])
    ]
    zt_codes = last_zt[last_zt == 1].index.tolist()
    code_list_zt = [x for x in code_list if x not in zt_codes]

    # 计算个股权限
    stk_perm = (td_mem + td_mem.max()) * (config.STK_HOLD_LIMIT / (2 * td_mem.max()))

    # 开盘前刷新
    act = s.refresh_open(td_upper, td_lower, td_preclose.to_dict(), td_adj)

    # 准备参数
    params = {
        'code_list': code_list,
        'code_list_all': code_list_all,
        'zt_codes': zt_codes,
        'code_list_zt': code_list_zt,
        'td_score': td_score,
        'td_mem': td_mem,
        'stk_perm': stk_perm,
        'td_citic': td_citic,
        'zz_citic': zz_citic,
        'td_cmvg': td_cmvg,
        'zz_cmvg': zz_cmvg,
        'style_fac': style_fac,
        'zz_style': zz_style,
        'td_preclose': td_preclose,
    }

    # 执行策略
    if config.STRATEGY == "solve":
        to_buy_s, to_sell_s = solve_strategy(s, act, **params)
    elif config.STRATEGY == "topn":
        to_buy_s, to_sell_s = topn_strategy(s, act, **params)

    # 执行交易
    hold_df, _ = record_trade(
        s, td_open, to_buy_s, to_sell_s, date, act_s, cash_s, buy_s, sell_s, hold_df_dict, trade_df_dict, "", td_close
    )

    # 计算持仓风格偏离
    hold_weight = hold_df["amt"] / hold_df["amt"].sum()
    td_citic_diff = td_citic.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_citic
    td_cmvg_diff = td_cmvg.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_cmvg
    td_style_diff = style_fac.reindex(hold_weight.index).fillna(0).T.dot(hold_weight) - zz_style
    td_mem_hs300_hold = hold_weight.reindex(mem_hs300[mem_hs300 > 0].index).fillna(0).sum()
    td_mem_zz500_hold = hold_weight.reindex(mem_zz500[mem_zz500 > 0].index).fillna(0).sum()
    td_mem_zz1000_hold = hold_weight.reindex(mem_zz1000[mem_zz1000 > 0].index).fillna(0).sum()
    td_mem_zz2000_hold = hold_weight.reindex(mem_zz2000[mem_zz2000 > 0].index).fillna(0).sum()
    td_hold_num = len(hold_weight)
    td_turnover = (buy_s[date] + sell_s[date]) / act_s[date] * 0.5
    
    # 计算加权排名
    if isinstance(td_score, list):
        hold_weight_aligned = hold_weight.reindex(td_score[0].index).fillna(0)
        amt_weighted_rank = hold_weight_aligned @ td_score[0].rank(ascending=False)
    else:
        hold_weight_aligned = hold_weight.reindex(td_score.index).fillna(0)
        amt_weighted_rank = hold_weight_aligned @ td_score.rank(ascending=False)

    # 存储风格偏离数据
    td_diff = pd.concat([td_citic_diff, td_cmvg_diff, td_style_diff])
    td_diff["mem_hs300_hold"] = td_mem_hs300_hold
    td_diff["mem_zz500_hold"] = td_mem_zz500_hold
    td_diff["mem_zz1000_hold"] = td_mem_zz1000_hold
    td_diff["mem_zz2000_hold"] = td_mem_zz2000_hold
    td_diff["hold_num"] = td_hold_num
    td_diff["turnover"] = td_turnover
    td_diff["amt_weighted_rank"] = amt_weighted_rank
    hold_style_dict[date] = td_diff
```

### 5. 结果汇总与分析

```python
# 汇总结果
total_s = pd.concat(
    [pd.Series(act_s), pd.Series(cash_s)],
    axis=1,
    keys=["total_act", "cash"],
)
nv = pd.concat([zs_day.reindex(total_s.index), total_s["total_act"]], axis=1, keys=["zs", "strategy"])
nv = nv / nv.iloc[0]
hold_style = pd.DataFrame(hold_style_dict).T
info, nv, rel_nv = analyse(nv)

# 生成报告
if config.PLOT:
    # 保存持仓数据
    all_hold_df = pd.DataFrame()
    for date, daily_hold_df in hold_df_dict.items():
        daily_hold_df_copy = daily_hold_df.copy()
        daily_hold_df_copy["date"] = date
        all_hold_df = pd.concat([all_hold_df, daily_hold_df_copy], ignore_index=False)

    # 保存结果文件
    rel_nv.to_csv(
        config.RESULT_PATH + "/" + config.STRATEGY_NAME + file_name_suffix + f"_trade_support{config.TRADE_SUPPORT}_rel_nv.csv",
        index_label="date",
    )
    if config.STRATEGY == "solve":
        all_hold_df.to_csv(
            config.RESULT_PATH + "/" + config.STRATEGY_NAME + file_name_suffix + f"_trade_support{config.TRADE_SUPPORT}_hold_df.csv",
            index_label="code",
        )
    elif config.STRATEGY == "topn":
        all_hold_df.to_csv(config.RESULT_PATH + "/" + config.STRATEGY_NAME + file_name_suffix + f"_topn_hold_df.csv", index_label="code")

    # 生成图表
    plot(nv, rel_nv, info, strategy=config.STRATEGY_NAME, scores_path=config.SCORES_PATH, hold_style=hold_style)
else:
    # 保存参数优化结果
    json_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../para_optimizer_ef/scores/",
        f"{config.PARA_NAME}.json"
    )
    # 保存结果到 JSON 文件
```

## 配置参数

回测引擎使用 `src/config.py` 中的配置参数，主要包括：

| 参数 | 类型 | 描述 | 默认值 |
|------|------|------|--------|
| `INITIAL_MONEY` | float | 初始资金 | 10010000.0 |
| `TOT_HOLD_NUM` | int | 总持仓数量 | 200 |
| `DAILY_SELL_NUM` | int | 每日卖出数量 | 20 |
| `TRADE_SUPPORT` | int | 交易约束类型 | 5 |
| `STRATEGY` | str | 策略类型 | "solve" |
| `SCORES_PATH` | list | 策略评分文件路径 | [] |
| `CITIC_LIMIT` | float | 行业偏离限制 | 0.06 |
| `CMVG_LIMIT` | float | 市值偏离限制 | 0.2 |
| `STK_HOLD_LIMIT` | float | 个股持仓限制 | 0.0106 |
| `OTHER_LIMIT` | float | 风格因子偏离限制 | 1.08 |
| `STK_BUY_R` | float | 个股买入比例 | 0.0072 |
| `TURN_MAX` | float | 最大换手率 | 0.09 |
| `MEM_HOLD` | float | 成分股持仓比例 | 0.2 |
| `PLOT` | bool | 是否生成图表 | True |
| `AFTERNOON_START` | bool | 是否下午开始交易 | False |
| `START_DATE_SHIFT` | int | 开始日期偏移天数 | 0 |

## 输出文件

回测完成后，会生成以下输出文件：

| 文件类型 | 路径 | 描述 |
|----------|------|------|
| 相对净值文件 | `results/backtests/{STRATEGY_NAME}_trade_support{TRADE_SUPPORT}_rel_nv.csv` | 策略与基准的相对净值 |
| 持仓文件 | `results/backtests/{STRATEGY_NAME}_trade_support{TRADE_SUPPORT}_hold_df.csv` | 每日持仓明细 |
| 图表文件 | `results/backtests/{STRATEGY_NAME}_trade_support{TRADE_SUPPORT}.png` | 回测结果图表 |
| 参数优化结果 | `para_optimizer_ef/scores/{PARA_NAME}.json` | 参数优化结果（PLOT=False时） |

## 示例代码

### 基本回测

```python
from src.backtest import run_backtest

# 运行回测
result = run_backtest()

# 查看回测结果
print("年化收益:", result["info"]["年化收益"])
print("夏普比率:", result["info"]["夏普比率"])
print("最大回撤:", result["info"]["最大回撤"])
```

### 自定义配置

```python
import src.config as config
from src.backtest import run_backtest

# 修改配置
config.INITIAL_MONEY = 5000000.0  # 500万初始资金
config.TOT_HOLD_NUM = 150  # 总持仓150只股票
config.STRATEGY = "topn"  # 使用TopN策略

# 运行回测
result = run_backtest()
```

## 性能优化

1. **数据加载**：使用 feather 格式存储数据，提高加载速度
2. **并行处理**：使用 tqdm 显示进度，提高用户体验
3. **内存管理**：及时清理不再需要的数据，减少内存占用
4. **向量化操作**：使用 pandas 进行数据处理，提高计算效率

## 扩展建议

1. **支持更多数据源**：可扩展支持更多类型的数据源
2. **添加更多策略**：可实现更多类型的策略，如动量策略、反转策略等
3. **支持更多交易约束**：可添加更多交易约束，如流动性约束、波动率约束等
4. **增强风险管理**：可添加更复杂的风险管理功能，如 VaR 计算、压力测试等