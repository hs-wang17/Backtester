# para_optimizer_gp - 基于贝叶斯优化的量化投资组合参数优化系统

## 项目概述

`para_optimizer_gp` 是一个基于贝叶斯优化（Bayesian Optimization）和高斯过程（Gaussian Process）的量化投资组合参数优化系统。该系统专门用于优化量化交易策略中的关键参数，通过自动化调参来寻找最优的投资组合配置，以最大化信息比率（Information Ratio）为核心目标。

### 核心特性

- **智能优化算法**：采用贝叶斯优化算法，高效探索多维参数空间
- **约束优化**：在尽量保证超额收益不降低、超额波动不增加的前提下进行优化
- **实时回测**：集成完整的回测引擎，实时评估参数效果
- **风险控制**：内置多种风险约束，确保优化结果的可行性
- **结果持久化**：自动保存最优参数和详细的优化历史记录

## 项目架构

```
para_optimizer_gp/
├── run.py                  # 主程序入口
├── config.py               # 配置文件管理
├── param_manager.py        # 参数管理器
├── optimizer.py            # 贝叶斯优化器
├── backtest.py             # 回测引擎
├── portfolio_optimizer.py  # 投资组合优化器
├── account.py              # 账户管理
├── analysis.py             # 性能分析
├── utils.py                # 工具函数
└── results/                # 结果存储目录
    └── optimal_params.json # 最优参数文件
```

## 优化目标与约束

### 优化目标

**主要目标**：最大化信息比率（IR, Information Ratio）

**约束条件**：

1. **超额收益约束**：确保优化后的超额收益不低于基准
2. **超额波动约束**：确保优化后的超额波动不高于基准
3. **惩罚函数**：对违反约束的解施加惩罚

### 参数空间

| 参数           | 含义           | 优化范围      | 默认值 |
| -------------- | -------------- | ------------- | ------ |
| CITIC_LIMIT    | 行业偏离限制   | [1e-8, 2.0]   | 0.06   |
| CMVG_LIMIT     | 市值偏离限制   | [1e-8, 2.0]   | 0.2    |
| STK_HOLD_LIMIT | 个股持仓限制   | [0.001, 0.02] | 0.0106 |
| OTHER_LIMIT    | 其他指标限制   | [1e-8, 2.0]   | 1.08   |
| STK_BUY_R      | 个股买入比例   | [0.001, 0.02] | 0.0072 |
| TURN_MAX       | 最大换手率     | [0.03, 0.2]   | 0.09   |
| MEM_HOLD       | 成员股持仓限制 | [1e-8, 0.4]   | 0.0    |

## 核心模块详解

### 1. 主程序入口 (run.py)

**功能**：程序的入口点，协调整个优化流程

**主要流程**：

```python
def main():
    # 1. 加载已有参数信息
    saved_info = param_manager.load_params()

    # 2. 创建回测包装器
    backtest_wrapper = create_backtest_wrapper()

    # 3. 初始化贝叶斯优化器
    optimizer = HyperparameterOptimizer(
        backtest_func=backtest_wrapper,
        param_manager=param_manager,
        n_calls=50,           # 总优化次数
        n_random_starts=10,   # 随机初始点数
        random_state=42       # 随机种子
    )

    # 4. 执行优化
    result = optimizer.optimize()
    best_result = optimizer.get_best_result()

    # 5. 保存最优参数
    param_manager.save_params(best_result["info"])
```

**使用方法**：

```bash
python run.py --scores_path /path/to/prediction_scores.csv
```

### 2. 配置管理 (config.py)

**功能**：统一管理所有配置参数

**主要配置项**：

#### (1) 数据路径配置

```python
DATA_PATH = "/home/user0/project/backtester/data"                       # 基础数据路径
DAILY_DATA_PATH = "/home/user0/data/data_frames"                        # 日线数据路径
SUPPORT_PATH = "/home/user0/project/backtester/data/trade_support5"     # 支持数据路径
SCORES_PATH = "/home/user0/results/predictions/StockPredictor_20251119_043804_combined_predictions.csv"     # 预测分数路径
```

#### (2) 约束参数配置

```python
INITIAL_MONEY = 200000000.0     # 初始资金
CITIC_LIMIT = 0.06              # 行业限制 (范围0-2)
CMVG_LIMIT = 0.2                # 市值限制 (范围0-2)
STK_HOLD_LIMIT = 0.0106         # 个股持仓限制 (范围0-0.02)
OTHER_LIMIT = 1.08              # 其他指标限制 (范围0-2)
STK_BUY_R = 0.0072              # 个股买入比例 (范围0.001-0.02)
TURN_MAX = 0.09                 # 个股最大买入比例 (范围0.03-0.2)
MEM_HOLD = 0.0                  # 成员股持仓限制 (范围0-0.4)
```

### 3. 参数管理器 (param_manager.py)

**功能**：管理超参数的定义、范围、保存和加载

**核心类**：

#### (1) HyperParams 数据类

```python
@dataclass
class HyperParams:
    CITIC_LIMIT: float = 0.06      # 行业限制
    CMVG_LIMIT: float = 0.2        # 市值限制
    STK_HOLD_LIMIT: float = 0.0106 # 个股持仓限制
    OTHER_LIMIT: float = 1.08      # 其他指标限制
    STK_BUY_R: float = 0.0072      # 个股买入比例
    TURN_MAX: float = 0.09         # 个股最大买入比例
    MEM_HOLD: float = 0.0          # 成员股持仓限制
```

#### (2) ParamManager 管理类

```python
class ParamManager:
    def __init__(self, param_file="results/optimal_params.json"):
        self.param_file = param_file
        self.params = HyperParams()
        self.param_ranges = {
            "CITIC_LIMIT": (1e-8, 2.0-1e-8),
            "CMVG_LIMIT": (1e-8, 2.0-1e-8),
            "STK_HOLD_LIMIT": (0.001+1e-8, 0.02-1e-8),
            # ... 其他参数范围
        }

    def set_params(self, params_dict):      # 设置参数
    def get_params(self) -> HyperParams:    # 获取参数对象
    def get_param_dict(self) -> Dict:       # 获取参数字典
    def save_params(self, info):            # 保存参数
    def load_params(self):                  # 加载参数
    def get_param_bounds(self) -> list:     # 获取参数边界
```

### 4. 贝叶斯优化器 (optimizer.py)

**功能**：使用贝叶斯优化算法自动寻找最优参数组合

**核心算法**：

```python
class HyperparameterOptimizer:
    def __init__(self, backtest_func, param_manager, n_calls=50, n_random_starts=10):
        # 定义搜索空间
        self.space = [
            Real(*param_manager.param_ranges["CITIC_LIMIT"], name="CITIC_LIMIT"),
            Real(*param_manager.param_ranges["CMVG_LIMIT"], name="CMVG_LIMIT"),
            # ... 其他参数空间
        ]

    def objective(self, params):
        """目标函数：最大化信息比率"""
        # 1. 更新参数配置
        self.param_manager.set_params(param_dict)

        # 2. 执行回测
        result = self.backtest_func()
        info = result["info"]
        ir = info.get("信息比率", 0)
        ex_ret = info.get("超额年化收益", 0)
        ex_std = info.get("超额年化波动", 0)

        # 3. 计算目标分数（带约束）
        score = ir
        if self.best_info is not None:
            # 约束1：超额收益不降低
            if ex_ret < base_ex_ret:
                score -= (base_ex_ret - ex_ret) * self.penalty_weight_ret
            # 约束2：超额波动不增加
            if ex_std > base_ex_std:
                score -= (ex_std - base_ex_std) * self.penalty_weight_std

        return -score  # 返回负值用于最小化

    def optimize(self):
        """执行贝叶斯优化"""
        result = gp_minimize(
            func=self.wrapped_objective,
            dimensions=self.space,
            n_calls=self.n_calls,
            n_random_starts=self.n_random_starts,
            random_state=self.random_state,
            callback=[checkpoint_saver],
            verbose=True
        )
        return result
```

### 5. 回测引擎 (backtest.py)

**功能**：执行完整的量化策略回测

**详细说明**：[查看【回测引擎】文档](./backtest.md)

### 6. 投资组合优化器 (portfolio_optimizer.py)

**功能**：使用凸优化求解最优投资组合权重

**详细说明**：[查看【投资组合优化器】文档](./portfolio_optimizer.md)

### 7. 账户管理 (account.py)

**功能**：模拟真实交易账户的资金和持仓管理

**详细说明**：[查看【账户管理】文档](./account.md)

## 使用指南

### 环境要求

```python
# 核心依赖
numpy >= 1.21.0
pandas >= 1.3.0
scipy >= 1.7.0
scikit-learn >= 1.0.0
scikit-optimize >= 0.9.0
cvxpy >= 1.2.0
tqdm >= 4.62.0
```

### 输出结果

#### 最优参数文件 (optimal_params.json)

```json
{
  "params": {
    "CITIC_LIMIT": 1.5930859677896063,
    "CMVG_LIMIT": 0.36686958606363185,
    "STK_HOLD_LIMIT": 0.015814123411362613,
    "OTHER_LIMIT": 1.193700313955971,
    "STK_BUY_R": 0.009470823387563176,
    "TURN_MAX": 0.04699574368956218,
    "MEM_HOLD": 0.18369955760136908
  },
  "info": {
    "信息比率": 1.619257873060897,
    "超额年化收益": 0.1650239175013364,
    "超额年化波动": 0.10191330253617373,
    "年化收益": 0.433883846648262,
    "年化波动": 0.3010621535905167,
    "夏普比率": 1.4411769846654734,
    "累计收益": 0.8988823034791775,
    "最大回撤": 0.23694523458923,
    "优化时间": "2025-12-08T11:14:25.157448"
  }
}
```

#### 优化历史文件

```json
[
    {
        "params": {"CITIC_LIMIT": 0.5, "CMVG_LIMIT": 0.3, ...},
        "ir": 1.2,
        "ex_ret": 0.15,
        "ex_std": 0.12,
        "timestamp": "2025-12-08T10:00:00"
    },
    ...
]
```

## 高级特性

### 1. 自适应约束权重（正在开发）

系统会根据优化进度动态调整约束权重：

```python
# 初期阶段：宽松约束，鼓励探索
penalty_weight_ret = 1.0e3
penalty_weight_std = 5.0e2

# 后期阶段：严格约束，确保可行
penalty_weight_ret = 2.0e3
penalty_weight_std = 1.0e3
```

### 2. 多阶段优化策略（正在开发）

```python
# 第一阶段：粗略搜索
optimizer_1 = HyperparameterOptimizer(n_calls=30, n_random_starts=15)

# 第二阶段：精细搜索
optimizer_2 = HyperparameterOptimizer(n_calls=50, n_random_starts=5)
```

### 3. 实时监控与干预

系统提供丰富的实时监控信息：

```python
# 实时输出每次迭代结果
print(f"参数: {param_dict}")
print(f"信息比率: {ir:.4f}, 超额收益: {ex_ret:.4f}, 超额波动: {ex_std:.4f}")
print(f"分数: {score:.4f}")
print("-" * 50)
```

### 4. 断点续传功能

支持优化过程中断后继续：

```python
# 创建检查点保存器
checkpoint_saver = CheckpointSaver("./checkpoint.pkl", store_objective=False)

# 从检查点恢复
result = gp_minimize(..., callback=[checkpoint_saver])
```

### 5. 自定义目标函数

```python
def custom_objective(params):
    # 自定义目标函数逻辑
    result = backtest_with_params(params)

    # 多目标优化
    ir = result['信息比率']
    sharpe = result['夏普比率']
    calmar = result['Calmar比率']

    # 加权组合
    score = 0.5 * ir + 0.3 * sharpe + 0.2 * calmar
    return score
```

_本文档最后更新时间：2025-12-08_
