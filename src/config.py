import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 原始路径（请根据你的实际修改这几行）
DATA_PATH = r"/home/user0/project/backtester/data"
TEST_RESULT_PATH = r"/home/user0/project/backtester/results"
DAILY_DATA_PATH = r"/home/user0/data/data_frames"
SUPPORT_PATH = r"/home/user0/project/backtester/data/trade_support5"
SCORES_PATH = r"/home/user0/mydata/label.csv"
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
INITIAL_MONEY = 20000 * 10000
CITIC_LIMIT = 0.06
CMVG_LIMIT = 0.2
STK_HOLD_LIMIT = 0.0106
OTHER_LIMIT = 1.08
STK_BUY_R = 0.0072
TURN_MAX = 0.09
MEM_HOLD = 0
