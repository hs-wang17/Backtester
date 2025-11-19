import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 原始路径（请根据你的实际修改这几行）
DATA_PATH = r"/home/user0/project/backtester/data"
TEST_RESULT_PATH = r"/home/user0/project/backtester/results"
DAILY_DATA_PATH = r"/home/user0/data/data_frames"
SUPPORT_PATH = r"/home/user0/project/backtester/data/trade_support5"
SCORES_PATH = r"/home/user0/results/predictions/StockPredictor_20251119_043804_combined_predictions.csv"
STRATEGY_NAME = os.path.splitext(os.path.basename(SCORES_PATH))[0]


def update_from_args(args):
    """允许 run.py 注入参数覆盖默认配置"""
    global SCORES_PATH, STRATEGY_NAME

    if args.scores_path:
        SCORES_PATH = args.scores_path
        STRATEGY_NAME = os.path.splitext(os.path.basename(SCORES_PATH))[0]


# 指数参数
IDX_NAME = "zz1000"
if IDX_NAME == "zz1000":
    IDX_NAME2 = "中证1000"
    FUTURE_BASIS = 0.08
elif IDX_NAME == "zz500":
    IDX_NAME2 = "中证500"
    FUTURE_BASIS = 0.04
else:
    IDX_NAME2 = "沪深300"
    FUTURE_BASIS = 0.0

# 约束参数
INITIAL_MONEY = 2e8  # 初始资金
CITIC_LIMIT = 0.06  # CITIC限制
CMVG_LIMIT = 0.2  # CMVG限制
STK_HOLD_LIMIT = 0.0106  # 个股持仓限制
OTHER_LIMIT = 1.08  # 其他指标限制
STK_BUY_R = 0.0072  # 个股买入比例
TURN_MAX = 0.09  # 个股最大买入比例
MEM_HOLD = 0  # 个股持仓限制
