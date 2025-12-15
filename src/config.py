import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# data paths
DATA_PATH = r"/home/user0/project/backtester/data"
TEST_RESULT_PATH = r"/home/user0/project/backtester/results"
DAILY_DATA_PATH = r"/home/user0/data/data_frames"
SUPPORT5_PATH = r"/home/user0/project/backtester/data/trade_support5"
SUPPORT7_PATH = r"/home/user0/project/backtester/data/trade_support7"
SCORES_PATH = r"/home/user0/results/predictions/StockPredictor_20251119_043804_combined_predictions.csv"
HOLD_DF_PATH = r"/home/user0/results/backtests/"
STRATEGY_NAME = os.path.splitext(os.path.basename(SCORES_PATH))[0]
TRADE_SUPPORT = 5


def update_from_args(args):
    """Update configuration from command line arguments."""
    global SCORES_PATH, STRATEGY_NAME, TRADE_SUPPORT

    if args.scores_path:
        SCORES_PATH = args.scores_path
        STRATEGY_NAME = os.path.splitext(os.path.basename(SCORES_PATH))[0]
        TRADE_SUPPORT = args.trade_support


# index settings
IDX_NAME = "zz1000"
if IDX_NAME == "zz1000":
    IDX_NAME_CN = "中证1000"
    FUTURE_BASIS = 0.08
elif IDX_NAME == "zz500":
    IDX_NAME_CN = "中证500"
    FUTURE_BASIS = 0.04
else:
    IDX_NAME_CN = "沪深300"
    FUTURE_BASIS = 0.0

INITIAL_MONEY = 200000000.0  # 初始资金

# constraint parameters
CITIC_LIMIT = 0.06  # 行业限制
CMVG_LIMIT = 0.2  # 市值限制
STK_HOLD_LIMIT = 0.0106  # 个股持仓限制
OTHER_LIMIT = 1.08  # 其他指标限制
STK_BUY_R = 0.0072  # 个股买入比例
TURN_MAX = 0.09  # 个股最大买入比例
MEM_HOLD = 0.0  # 成员股持仓限制

# constraint parameters (trade_support5_optimal_params)
CITIC_LIMIT = 0.09522923178628084  # 行业限制 (范围0-2)
CMVG_LIMIT = 1.4773003074308027  # 市值限制 (范围0-2)
STK_HOLD_LIMIT = 0.005572496051203135  # 个股持仓限制 (范围0-0.02)
OTHER_LIMIT = 0.18291207860261288  # 其他指标限制 (范围0-2)
STK_BUY_R = 0.009569219620263202  # 个股买入比例 (范围0.001-0.02)
TURN_MAX = 0.09379183056054619  # 个股最大买入比例 (范围0.03-0.2)
MEM_HOLD = 0.38996638586551474  # 成员股持仓限制 (范围0-0.4)

# constraint parameters (trade_support7_optimal_params)
# CITIC_LIMIT = 0.0  # 行业限制 (范围0-0.5)
# CMVG_LIMIT = 0.5  # 市值限制 (范围0-0.5)
# STK_HOLD_LIMIT = 0.0093646643386971  # 个股持仓限制 (范围0-0.02)
# OTHER_LIMIT = 0.5  # 其他指标限制 (范围0-0.5)
# STK_BUY_R = 0.02  # 个股买入比例 (范围0.001-0.02)
# TURN_MAX = 0.06371569969647878  # 个股最大买入比例 (范围0.03-0.2)
# MEM_HOLD = 0.0  # 成员股持仓限制 (范围0-0.4)
