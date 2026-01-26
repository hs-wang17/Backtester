# Barra 因子对话记录（导出）

## 需求

**用户：**

`src/main.py` 40个基础因子的代码已经计算完，补充 `barrafactor` 类的计算代码  
1. 每个 `barrafactor` 计算前要把 `basefactor` 做去极值化，做中心化处理  
2. 几个 `basefactor` 合成 `barrafactor` 时计算一下 `corr_matrix` 并 `print`  
3. 添加一个 `check` 代码，检查每天的 barra 因子覆盖度和极值情况，以及每个 `barrafactor` 的 `corr_matrix` 情况（几个组合的 `basefactor` 应该是正相关的）

**助手：**

- 已在 `src/main.py` 补齐 `BarraFactor` 的计算与检查逻辑：
  - `BarraFactor` 组合前对每个 `basefactor` 做去极值(1%/99% winsorize)+中心化(减均值)，并按“可用因子权重归一化”合成（`src/main.py:81`，`src/main.py:105`）
  - 每个由多个 `basefactor` 组合的 `barrafactor`，会计算并 `print` 组合前（同样去极值+中心化后）的 `corr_matrix`（`src/main.py:241`）
  - 增加 daily check：每天逐个 barra 因子打印覆盖度与极值统计，并对组合因子输出/检查相关矩阵（负相关会额外提示）（`src/main.py:241`，主流程调用在 `src/main.py:2194`）
- 环境变量：
  - 关闭检查：`BARRA_CHECK=0 python src/main.py`

## 新需求：行业分类 + 市值正交化

**用户：**

输出示例（行业分类信息）：

```text
shape: (2_663, 5_409)
┌──────────┬──────────┬──────────┬──────────┬───┬──────────┬──────────┬──────────┬───────────────────┐
│ 000001   ┆ 000002   ┆ 000004   ┆ 000005   ┆ … ┆ 688819   ┆ 688981   ┆ 689009   ┆ __index_level_0__ │
│ ---      ┆ ---      ┆ ---      ┆ ---      ┆   ┆ ---      ┆ ---      ┆ ---      ┆ ---               │
│ str      ┆ str      ┆ str      ┆ str      ┆   ┆ str      ┆ str      ┆ str      ┆ str               │
╞══════════╪══════════╪══════════╪══════════╪═══╪══════════╪══════════╪══════════╪═══════════════════╡
│ 01031721 ┆ 01031723 ┆ 01031718 ┆ 01031723 ┆ … ┆ null     ┆ null     ┆ null     ┆ 20150105          │
│ 01031721 ┆ 01031723 ┆ 01031718 ┆ 01031723 ┆ … ┆ null     ┆ null     ┆ null     ┆ 20150106          │
│ 01031721 ┆ 01031723 ┆ 01031718 ┆ 01031723 ┆ … ┆ null     ┆ null     ┆ null     ┆ 20150107          │
│ 01031721 ┆ 01031723 ┆ 01031718 ┆ 01031723 ┆ … ┆ null     ┆ null     ┆ null     ┆ 20150108          │
│ 01031721 ┆ 01031723 ┆ 01031718 ┆ 01031723 ┆ … ┆ null     ┆ null     ┆ null     ┆ 20150109          │
│ …        ┆ …        ┆ …        ┆ …        ┆ … ┆ …        ┆ …        ┆ …        ┆ …                 │
│ 01031721 ┆ 01031723 ┆ 01031727 ┆ null     ┆ … ┆ 01031711 ┆ 01031725 ┆ 01031710 ┆ 20251210          │
│ 01031721 ┆ 01031723 ┆ 01031727 ┆ null     ┆ … ┆ 01031711 ┆ 01031725 ┆ 01031710 ┆ 20251211          │
│ 01031721 ┆ 01031723 ┆ 01031727 ┆ null     ┆ … ┆ 01031711 ┆ 01031725 ┆ 01031710 ┆ 20251212          │
│ 01031721 ┆ 01031723 ┆ 01031727 ┆ null     ┆ … ┆ 01031711 ┆ 01031725 ┆ 01031710 ┆ 20251215          │
│ 01031721 ┆ 01031723 ┆ 01031727 ┆ null     ┆ … ┆ 01031711 ┆ 01031725 ┆ 01031710 ┆ 20251216          │
└──────────┴──────────┴──────────┴──────────┴───┴──────────┴──────────┴──────────┴───────────────────┘
```

将 `basefactor` 合成 `barrafactor` 前，对 `basefactor` 做行业+市值正交化处理。

**助手：**

- 已在 `src/main.py` 把“行业 + 市值正交化”接到合成 `Barrafactor` 之前：
  - 行业来源：优先用 `stk_citic3_code`，否则依次回退到 `stk_citic2_code` / `stk_citic1_code`（从 `/mnt/raid0/nfs_from6_readonly/data_frames` 读当日最后一条并转 long）
  - 市值暴露：用 `SIZE`（`log(stk_market_value)`）作为回归自变量
  - 处理顺序：对每个 `basefactor` 先去极值(1%/99%)+中心化 → 再做截面回归 `y ~ 1 + SIZE + industry_dummies` 取残差 → 用残差去合成 `barrafactor`
  - `corr_matrix`：默认在 `process_single_day` 打印组合因子的相关矩阵；可用 `BARRA_PRINT_CORR=0` 关闭
  - `check`：仍会打印每天每个 barra 因子的覆盖度/极值；corr 只在发现负相关时额外打印矩阵并 warn（`BARRA_CHECK=0` 可关闭整个检查）

## 调整：固定行业源 + WLS

**用户：**

1. 直接使用 `stk_citic1_code`，且 df 存的是 `uint`，做一下 cast  
2. 做行业市值正交化该用 wls 回归，weight 使用 `stk_market_value`

**助手：**

- 已按两点改完（`src/main.py`）：
  - 行业暴露：只用 `stk_citic1_code`，并在转 long 时对行业列做 `cast(pl.UInt32, strict=False)`
  - 行业+市值正交化：改为 **WLS 回归**，模型 `y ~ 1 + SIZE + industry_dummies`，权重 `w = stk_market_value`（当日 `stk_market_value` 转 long 后对齐到 `sec_id`），残差用于后续合成 `Barrafactor`

