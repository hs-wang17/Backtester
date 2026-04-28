# 账户管理模块详解

`src/account.py` 模块提供了完整的账户管理和股票交易功能，是整个回测系统的核心组件之一。

## 核心类

### 1. `stk` 类

#### 功能
管理单个股票的基本信息和交易操作。

#### 主要属性

| 属性 | 类型 | 描述 | 默认值 |
|------|------|------|--------|
| `code` | str | 股票代码 | 必填 |
| `price` | float | 当前价格 | 必填 |
| `up_price` | float | 涨停价 | 必填 |
| `low_price` | float | 跌停价 | 必填 |
| `sellable_vol` | float | 可卖数量 | 0 |
| `trade_fee` | float | 交易费用和冲击成本 | 0.001 |
| `volume` | float | 持有数量 | 0 |
| `amt` | float | 持有金额 | 0 |
| `minimum_vol` | float | 最小买入数量 | 科创板200股，其他100股 |
| `unit_vol` | float | 交易单位 | 科创板1股，其他100股 |

#### 主要方法

| 方法 | 参数 | 返回值 | 描述 |
|------|------|--------|------|
| `update_price(price)` | price: float | None | 更新股票价格并计算持有金额 |
| `update_info(price, up_price, low_price)` | price: float<br>up_price: float<br>low_price: float | None | 更新股票价格、涨停价和跌停价 |
| `buy(volume)` | volume: float | float | 买入指定数量的股票，返回花费金额 |
| `sell(volume)` | volume: float | float | 卖出指定数量的股票，返回获得金额 |

### 2. `account` 类

#### 功能
管理整个账户的资金、持仓和交易记录。

#### 主要属性

| 属性 | 类型 | 描述 | 默认值 |
|------|------|------|--------|
| `cash` | float | 可用资金 | 初始资金 |
| `total_account` | float | 账户总资产 | 初始资金 |
| `hold_dict` | dict | 持仓股票字典 | {} |
| `trade_dict` | dict | 交易记录字典 | {} |
| `date` | str | 当前日期 | None |

#### 主要方法

| 方法 | 参数 | 返回值 | 描述 |
|------|------|--------|------|
| `cal_total()` | 无 | float | 计算账户总资产 |
| `refresh_open(td_upper, td_lower, td_preclose, td_adj)` | td_upper: pd.Series<br>td_lower: pd.Series<br>td_preclose: pd.Series<br>td_adj: pd.Series | float | 开盘前刷新账户信息 |
| `cal_sellable_amt()` | 无 | pd.DataFrame | 计算可卖金额 |
| `log_trade(code, price, vol)` | code: str<br>price: float<br>vol: float | None | 记录交易 |
| `buy_stk(code, vol)` | code: str<br>vol: float | float | 买入股票 |
| `sell_stk(code, vol)` | code: str<br>vol: float | float | 卖出股票 |
| `fresh_price(price_s)` | price_s: dict | None | 刷新所有股票价格 |
| `daily_trade(cash_avail, to_buy_s, to_sell_s)` | cash_avail: float<br>to_buy_s: dict<br>to_sell_s: dict | (float, float) | 执行每日交易 |
| `close_today()` | 无 | (pd.DataFrame, pd.DataFrame) | 收盘处理，返回持仓和交易数据 |

## 交易逻辑

### 1. 买入逻辑

1. 检查股票是否在持仓中，若不在则创建新的 `stk` 对象
2. 检查股票是否处于涨跌停状态
3. 计算可买入数量（考虑交易单位和可用资金）
4. 执行买入操作，更新持仓和资金
5. 记录交易日志

### 2. 卖出逻辑

1. 检查股票是否在持仓中
2. 检查股票是否处于涨跌停状态
3. 计算可卖出数量（考虑最小交易量和可卖数量）
4. 执行卖出操作，更新持仓和资金
5. 记录交易日志
6. 若持仓为0，从持仓字典中删除该股票

### 3. 每日交易流程

1. 先执行所有卖出操作
2. 再执行买入操作，直到可用资金耗尽
3. 确保交易符合最小交易量要求
4. 避免在涨跌停时交易

## 交易约束处理

1. **最小交易量**：科创板股票最小交易单位为200股，其他股票为100股
2. **交易单位**：科创板股票以1股为单位，其他股票以100股为单位
3. **涨跌停限制**：避免在涨跌停时进行交易
4. **交易费用**：卖出时收取交易费用和冲击成本

## 示例代码

### 基本使用

```python
from src.account import account

# 创建账户，初始资金1000万
s = account(10000000.0)

# 开盘前刷新账户信息
total = s.refresh_open(td_upper, td_lower, td_preclose, td_adj)

# 执行每日交易
total_buy, total_sell = s.daily_trade(cash_avail, to_buy_s, to_sell_s)

# 收盘处理
hold_df, trade_df = s.close_today()

# 计算账户总资产
total_asset = s.cal_total()
```

### 交易记录查询

```python
# 查看交易记录
trade_df = pd.DataFrame()
for code, trades in s.trade_dict.items():
    for trade in trades:
        vol, price, date = trade
        trade_df = trade_df.append({
            'code': code,
            'volume': vol,
            'price': price,
            'date': date
        }, ignore_index=True)
```

## 注意事项

1. **复权处理**：`refresh_open` 方法会自动处理股票复权
2. **交易费用**：仅在卖出时收取交易费用
3. **可卖数量**：当日买入的股票下一交易日才能卖出
4. **数据格式**：确保输入的数据格式正确，特别是价格和数量
5. **异常处理**：代码中包含基本的异常处理，如键错误

## 性能优化

1. **数据结构**：使用字典存储持仓和交易记录，提高查询效率
2. **向量化操作**：使用 pandas 进行数据处理，提高计算效率
3. **内存管理**：及时清理不再需要的数据，减少内存占用

## 扩展建议

1. **支持更多交易费用模型**：可扩展支持不同的交易费用计算方式
2. **添加止损止盈功能**：可根据策略需求添加止损止盈逻辑
3. **支持融资融券**：可扩展支持融资融券交易
4. **添加交易滑点模型**：可根据市场流动性添加更复杂的滑点模型