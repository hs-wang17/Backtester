import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# data paths
DATA_PATH = r"/home/haris/data/trade_support_data"  # 回测数据输入
TEST_RESULT_PATH = r"/home/haris/project/backtester/results"  # 回测结果输出
DAILY_DATA_PATH = r"/home/haris/data/data_frames"  # 日频基础数据
SUPPORT5_PATH = r"/home/haris/data/trade_support_data/trade_support5"  # trade_support5特征
SUPPORT7_PATH = r"/home/haris/data/trade_support_data/trade_support7"  # trade_support7特征
SUPPORTBARRA_PATH = r"/home/haris/data/trade_support_data/trade_support_barra"  # trade_support_barra特征
SCORES_PATH = r"/home/haris/mymodel/predictions/StockPredictor_20251231_merged.csv"  # 早盘因子预测文件
NOON_SCORES_PATH = r"/home/haris/mymodel_noon/predictions/StockPredictor_20260302_merged.csv"  # 午盘因子预测文件
HOLD_DF_PATH = r"/home/haris/results/backtests/"  # 输出持仓文件

STRATEGY_NAME = os.path.splitext(os.path.basename(SCORES_PATH))[0]  # 打分名称
PLOT = True  # 是否绘制回测结果
AFTERNOON_START = True  # 是否为午盘交易模式
APM_MODE = False  # 是否为早午盘交易模式
CALL_START = False  # 是否为集合竞价模式
PARA_NAME = "20251231_trade_support7"  # 参数文件
SOLVER_METHOD = "basic"  # 组合优化方法(单阶段/两阶段)
STRATEGY = "solve"  # 选股策略(组合优化/topn)
TOT_HOLD_NUM = 200  # 持仓个股数
DAILY_SELL_NUM = 20  # 每日卖出个数
HOLD_INIT = "solve"  # 持仓初始化策略(组合优化/topn)
START_DATE_SHIFT = 0  # 开始日期偏移
LAMBDA_SPARSE = 0.0  # 稀疏化系数
N_CALLS = 200  # 高斯过程优化调用次数
N_RANDOM_STARTS = 100  # 高斯过程优化随机起始点次数
REMOVE_ABNORMAL = False  # 是否剔除异常数据

# index settings
IDX_NAME = "zz1000"
if IDX_NAME == "zz1000":
    IDX_NAME_CN = "中证1000"
    FUTURE_BASIS = 0.08
elif IDX_NAME == "zz500":
    IDX_NAME_CN = "中证500"
    FUTURE_BASIS = 0.04
elif IDX_NAME == "hs300":
    IDX_NAME_CN = "沪深300"
    FUTURE_BASIS = 0.0
elif IDX_NAME == "A500":
    IDX_NAME_CN = "中证A500"
    FUTURE_BASIS = 0.0

TRADE_SUPPORT = 5
INITIAL_MONEY = 200000000.0  # 初始资金
INITIAL_MONEY = 10010000.0  # 初始资金

# constraint parameters
CITIC_LIMIT = 0.06  # 行业(早盘)
CITIC_LIMIT_NOON = 0.06  # 行业(午盘)
CMVG_LIMIT = 0.2  # 市值
STK_HOLD_LIMIT = 0.0106  # 个股持仓
OTHER_LIMIT = 1.08  # 风格
STK_BUY_R = 0.0072  # 个股买入比例
TURN_MAX = 0.09  # 换手率(早盘)
TURN_MAX_NOON = 0.09  # 换手率(午盘)
MEM_HOLD = 0.2  # 成分股持仓


def update_from_args(args):
    """Update configuration from command line arguments."""
    global SCORES_PATH, NOON_SCORES_PATH, STRATEGY_NAME, TRADE_SUPPORT
    global CITIC_LIMIT, CMVG_LIMIT, CITIC_LIMIT_NOON, STK_HOLD_LIMIT, OTHER_LIMIT, STK_BUY_R, TURN_MAX, TURN_MAX_NOON, MEM_HOLD
    global PLOT, AFTERNOON_START, APM_MODE, CALL_START, PARA_NAME, SOLVER_METHOD, STRATEGY
    global TOT_HOLD_NUM, DAILY_SELL_NUM, HOLD_INIT, START_DATE_SHIFT, LAMBDA_SPARSE
    global N_CALLS, N_RANDOM_STARTS, REMOVE_ABNORMAL

    if hasattr(args, "scores_path") and args.scores_path:
        if "," in args.scores_path:
            SCORES_PATH = [p.strip() for p in args.scores_path.split(",") if p.strip()]
            NOON_SCORES_PATH = (
                [p.strip() for p in args.noon_scores_path.split(",") if p.strip()]
                if hasattr(args, "noon_scores_path") and args.noon_scores_path
                else []
            )
            STRATEGY_NAME = "+".join(os.path.splitext(os.path.basename(p))[0] for p in SCORES_PATH + NOON_SCORES_PATH)
        else:
            SCORES_PATH = [args.scores_path]
            if hasattr(args, "noon_scores_path") and args.noon_scores_path:
                NOON_SCORES_PATH = [args.noon_scores_path]
                STRATEGY_NAME = (
                    os.path.splitext(os.path.basename(args.scores_path))[0]
                    + "_am_"
                    + os.path.splitext(os.path.basename(args.noon_scores_path))[0]
                    + "_pm"
                )
            else:
                NOON_SCORES_PATH = []
                STRATEGY_NAME = os.path.splitext(os.path.basename(args.scores_path))[0]

    if hasattr(args, "trade_support") and args.trade_support is not None:
        TRADE_SUPPORT = args.trade_support

    if TRADE_SUPPORT == 5:
        # parameter 0
        CITIC_LIMIT = 0.09522923178628084  # 行业 (范围0-2)
        CITIC_LIMIT_NOON = 0.09522923178628084  # 行业(午盘) (范围0-2)
        CMVG_LIMIT = 1.4773003074308027  # 市值 (范围0-2)
        STK_HOLD_LIMIT = 0.005572496051203135  # 个股持仓 (范围0-0.02)
        OTHER_LIMIT = 0.18291207860261288  # 风格 (范围0-2)
        STK_BUY_R = 0.009569219620263202  # 个股买入比例 (范围0.001-0.02)
        TURN_MAX = 0.09379183056054619  # 换手率 (范围0.03-0.2)
        TURN_MAX_NOON = 0.09379183056054619  # 换手率(午盘) (范围0.03-0.2)
        MEM_HOLD = 0.38996638586551474  # 成分股持仓 (范围0-0.4)

        # parameter 1
        CITIC_LIMIT = 0.06  # 行业
        CITIC_LIMIT_NOON = 0.06  # 行业(午盘)
        CMVG_LIMIT = 0.2  # 市值
        STK_HOLD_LIMIT = 0.0106  # 个股持仓
        OTHER_LIMIT = 1.08  # 风格
        STK_BUY_R = 0.0072  # 个股买入比例
        TURN_MAX = 0.09  # 换手率
        TURN_MAX_NOON = 0.09  # 换手率(午盘)
        MEM_HOLD = 0.2  # 成分股持仓

    elif TRADE_SUPPORT == 7:
        if len(SCORES_PATH) > 1:
            # multiple score
            CITIC_LIMIT = 0.0  # 行业 (范围0-0.5)
            CITIC_LIMIT_NOON = 0.0  # 行业(午盘) (范围0-0.5)
            CMVG_LIMIT = 0.5  # 市值 (范围0-0.5)
            STK_HOLD_LIMIT = 0.0093646643386971 * 1.5  # 个股持仓 (范围0-0.02)
            OTHER_LIMIT = 0.5  # 风格 (范围0-0.5)
            STK_BUY_R = 0.02 * 1.5  # 个股买入比例 (范围0.001-0.02)
            TURN_MAX = 0.06371569969647878  # 换手率 (范围0.03-0.2)
            TURN_MAX_NOON = 0.06371569969647878  # 换手率(午盘) (范围0.03-0.2)
            MEM_HOLD = 0.0  # 成份股持仓 (范围0-0.4)
        elif len(SCORES_PATH) == 1:
            # single score
            CITIC_LIMIT = 0.0  # 行业 (范围0-0.5)
            CITIC_LIMIT_NOON = 0.0  # 行业(午盘) (范围0-0.5)
            CMVG_LIMIT = 0.5  # 市值 (范围0-0.5)
            STK_HOLD_LIMIT = 0.0093646643386971  # 个股持仓 (范围0-0.02)
            OTHER_LIMIT = 0.5  # 风格 (范围0-0.5)
            STK_BUY_R = 0.02  # 个股买入比例 (范围0.001-0.02)
            TURN_MAX = 0.06371569969647878  # 换手率 (范围0.03-0.2)
            TURN_MAX_NOON = 0.06371569969647878  # 换手率(午盘) (范围0.03-0.2)
            MEM_HOLD = 0.0  # 成份股持仓 (范围0-0.4)

    elif TRADE_SUPPORT == 8:
        CITIC_LIMIT = 0.0  # 行业 (范围0-0.5)
        CITIC_LIMIT_NOON = 0.0  # 行业(午盘) (范围0-0.5)
        CMVG_LIMIT = 0.24  # 市值 (范围0-0.5)
        STK_HOLD_LIMIT = 0.01  # 个股持仓 (范围0-0.02)
        OTHER_LIMIT = 0.4  # 风格 (范围0-0.5)
        STK_BUY_R = 0.015  # 个股买入比例 (范围0.001-0.02)
        TURN_MAX = 0.04  # 换手率 (范围0.03-0.2)
        TURN_MAX_NOON = 0.04  # 换手率(午盘) (范围0.03-0.2)
        MEM_HOLD = 0.1  # 成份股持仓 (范围0-0.4)

    if hasattr(args, "citic_limit") and args.citic_limit is not None:
        CITIC_LIMIT = args.citic_limit
    if hasattr(args, "citic_limit_noon") and args.citic_limit_noon is not None:
        CITIC_LIMIT_NOON = args.citic_limit_noon
    if hasattr(args, "cmvg_limit") and args.cmvg_limit is not None:
        CMVG_LIMIT = args.cmvg_limit
    if hasattr(args, "stk_hold_limit") and args.stk_hold_limit is not None:
        STK_HOLD_LIMIT = args.stk_hold_limit
    if hasattr(args, "other_limit") and args.other_limit is not None:
        OTHER_LIMIT = args.other_limit
    if hasattr(args, "stk_buy_r") and args.stk_buy_r is not None:
        STK_BUY_R = args.stk_buy_r
    if hasattr(args, "turn_max") and args.turn_max is not None:
        TURN_MAX = args.turn_max
    if hasattr(args, "turn_max_noon") and args.turn_max_noon is not None:
        TURN_MAX_NOON = args.turn_max_noon
    if hasattr(args, "mem_hold") and args.mem_hold is not None:
        MEM_HOLD = args.mem_hold

    if hasattr(args, "plot") and args.plot is not None:
        PLOT = args.plot
    if hasattr(args, "afternoon_start") and args.afternoon_start is not None:
        AFTERNOON_START = args.afternoon_start
    if hasattr(args, "apm_mode") and args.apm_mode is not None:
        APM_MODE = args.apm_mode
    if hasattr(args, "call_start") and args.call_start is not None:
        CALL_START = args.call_start
    if hasattr(args, "para_name") and args.para_name is not None:
        PARA_NAME = args.para_name
    if hasattr(args, "solver_method") and args.solver_method is not None:
        SOLVER_METHOD = args.solver_method
    if hasattr(args, "strategy") and args.strategy is not None:
        STRATEGY = args.strategy
    if hasattr(args, "tot_hold_num") and args.tot_hold_num is not None:
        TOT_HOLD_NUM = args.tot_hold_num
    if hasattr(args, "daily_sell_num") and args.daily_sell_num is not None:
        DAILY_SELL_NUM = args.daily_sell_num
    if hasattr(args, "hold_init") and args.hold_init is not None:
        HOLD_INIT = args.hold_init
    if hasattr(args, "start_date_shift") and args.start_date_shift is not None:
        START_DATE_SHIFT = args.start_date_shift
    if hasattr(args, "lambda_sparse") and args.lambda_sparse is not None:
        LAMBDA_SPARSE = args.lambda_sparse
    if hasattr(args, "n_calls") and args.n_calls is not None:
        N_CALLS = args.n_calls
    if hasattr(args, "n_random_starts") and args.n_random_starts is not None:
        N_RANDOM_STARTS = args.n_random_starts
    if hasattr(args, "remove_abnormal") and args.remove_abnormal is not None:
        REMOVE_ABNORMAL = args.remove_abnormal

    """Pretty-print all configuration parameters."""
    print("=" * 120)
    print("Backtest Configuration")
    print("=" * 120)
    for k, v in vars(args).items():
        if v is not None:
            print(f"{k:25s}: {v}")
    print("=" * 120)
