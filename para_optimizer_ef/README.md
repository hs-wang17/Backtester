# 参数优化与有效前沿分析 (Parameter Optimization and Efficient Frontier Analysis)

## 项目概述

本项目实现了一个完整的参数优化与有效前沿分析框架，通过数学优化方法寻找投资策略的最优参数组合，并构建有效前沿来评估策略的风险收益特征。

### 核心功能

- **数据处理与预处理**：读取回测历史数据，进行标准化处理
- **上凸包计算**：通过凸包算法识别有效边界点
- **二次函数拟合**：使用逆二次模型拟合上凸包的最佳逼近曲线
- **支撑超平面优化**：计算有效前沿，确保所有有效点位于边界上
- **效率评分系统**：量化评估参数组合的效率表现
- **机器学习优化**：结合随机森林和差分进化算法进行参数寻优
- **分散化筛选**：使用 K-means 聚类确保参数多样性

## 技术架构

### 算法流程

```
数据输入 → 上凸包计算 → 二次函数拟合 → 支撑超平面优化 → 效率评分 → 机器学习优化 → 分散化筛选 → 结果输出
```

### 关键算法

1. **上凸包算法**：改进的凸包算法，支持容差参数调整
2. **逆二次模型**：`x = a*y² + b*y + c` 形式的二次函数拟合
3. **支撑超平面**：通过约束优化确保有效前沿的数学正确性
4. **差分进化算法**：全局优化方法，避免局部最优
5. **随机森林回归**：参数与效率得分的非线性建模

## 环境要求

### Python 依赖

```python
# 核心科学计算库
numpy >= 1.21.0
pandas >= 1.3.0
matplotlib >= 3.5.0
scipy >= 1.7.0

# 机器学习库
scikit-learn >= 1.0.0

# 可视化与进度条
tqdm >= 4.62.0
```

### 安装方式

```bash
# 使用pip安装
pip install numpy pandas matplotlib scipy scikit-learn tqdm

# 或使用conda
conda install numpy pandas matplotlib scipy scikit-learn tqdm
```

## 使用指南

### 1. 数据准备

确保您的回测历史数据以 JSON 格式存储，包含以下必要字段：

```json
{
  "信息比率": "IR值",
  "超额年化收益": "exret",
  "超额年化波动": "exstd",
  "年化收益": "ret",
  "最大回撤": "mdd",
  "超额最大回撤": "exmdd",
  "时间戳": "timestamp",
  "参数.CITIC_LIMIT": "citic_limit",
  "参数.CMVG_LIMIT": "cmvg_limit",
  "参数.STK_HOLD_LIMIT": "stock_hold_limit",
  "参数.OTHER_LIMIT": "other_limit",
  "参数.STK_BUY_R": "stock_buy_ratio",
  "参数.TURN_MAX": "turnover_max",
  "参数.MEM_HOLD": "memory_hold"
}
```

### 2. 运行 Jupyter Notebook

```bash
# 进入项目目录
cd /home/haris/project/backtester/para_optimizer_ef/

# 启动Jupyter
jupyter notebook run.ipynb
```

### 3. 脚本运行方式

```bash
# 直接运行Python脚本
python /home/haris/project/backtester/scripts/run_para_ef.py

# 或使用shell脚本
bash /home/haris/project/backtester/scripts/run_para_ef.sh
```

## 核心算法详解

### 1. 上凸包计算

```python
def get_upper_convex_hull(points, eps=0.01):
    """
    计算上凸包的改进算法

    参数:
    - points: 点集 [(x1,y1), (x2,y2), ...]
    - eps: 容差参数
        eps > 0: 更宽松的凸包
        eps = 0: 标准上凸包
        eps < 0: 更严格的凸包

    返回: 上凸包点列表
    """
```

**算法特点：**

- 支持容差调整，适应不同数据特征
- 基于向量叉积判断凸性
- 时间复杂度 O(n log n)

### 2. 二次函数拟合

使用逆二次模型：`exstd = a × exret² + b × exret + c`

```python
def inverse_quadratic_model(params, y):
    """二次函数模型: x = a*y^2 + b*y + c"""
    a, b, c = params
    return a * y**2 + b * y + c
```

### 3. 支撑超平面优化

通过约束优化确保有效前沿的正确性：

```python
def constraint_shift(shift, h_x, h_y, a, b, c):
    """约束条件：平移后的曲线必须在所有凸包点的左侧"""
    dx, dy = shift
    residuals = h_x - (a * (h_y - dy)**2 + b * (h_y - dy) + c + dx)
    return residuals
```

### 4. 效率评分

```python
def calculate_efficiency_score(row, a, b, c):
    """
    计算效率得分：有效前沿对应的最小波动率 - 实际波动率
    正值表示优于有效前沿，负值表示劣于有效前沿
    """
    x_frontier = a * row["exret"]**2 + b * row["exret"] + c
    return x_frontier - row["exstd"]
```

## 参数配置

### 优化参数范围

| 参数名                 | 范围          | 说明         |
| ---------------------- | ------------- | ------------ |
| param_citic_limit      | (0, 0.5)      | 中信限制     |
| param_cmvg_limit       | (0, 0.5)      | CMVG 限制    |
| param_stock_hold_limit | (0.001, 0.02) | 股票持仓限制 |
| param_other_limit      | (0, 0.5)      | 其他限制     |
| param_stock_buy_ratio  | (0.001, 0.02) | 股票买入比例 |
| param_turnover_max     | (0.03, 0.2)   | 最大换手率   |
| param_memory_hold      | (0, 0.4)      | 记忆持仓     |

### 机器学习参数

```python
# 随机森林参数
n_estimators = 100
random_state = 42

# 差分进化参数
max_iters = 50
pop_size = 20
strategy = "rand1bin"
```

## 输出结果

### 1. 可视化图表

- **散点图**：显示所有参数组合的风险收益分布
- **上凸包图**：标识别有效边界点
- **有效前沿曲线**：二次函数拟合和优化后的有效边界
- **效率评分热力图**：显示参数组合的效率分布

### 2. 数据文件

- `diverse_efficient_parameters.json`：优化后的参数组合（JSON 格式）
- `diverse_efficient_parameters.csv`：优化后的参数组合（CSV 格式）

### 3. 模型文件

- 随机森林模型：用于预测参数组合的效率得分
- 有效前沿参数：二次函数系数 (a, b, c)
