import argparse
from src.param_manager import ParamManager
from src.optimizer import HyperparameterOptimizer
from src import config
from src.backtest import run_backtest


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("true", "1", "yes", "y"):
        return True
    if v.lower() in ("false", "0", "no", "n"):
        return False
    raise argparse.ArgumentTypeError("Boolean value expected.")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--afternoon_start", type=str2bool, default=False, help="Afternoon start")
    parser.add_argument("--citic_limit", type=float, default=None, help="Citic limit")
    parser.add_argument("--cmvg_limit", type=float, default=None, help="Cmvg limit")
    parser.add_argument("--daily_sell_num", type=int, default=None, help="Daily sell number")
    parser.add_argument("--hold_init", type=str, default="solve", help="Hold initialization method (member or solve)")
    parser.add_argument("--lambda_sparse", type=float, default=None, help="Lambda sparse")
    parser.add_argument("--mem_hold", type=float, default=None, help="Member hold limit")
    parser.add_argument("--n_calls", type=int, default=None, help="Number of calls for GP optimizer")
    parser.add_argument("--n_random_starts", type=int, default=None, help="Number of random starts for GP optimizer")
    parser.add_argument("--other_limit", type=float, default=None, help="Other limit")
    parser.add_argument("--para_name", type=str, default=None, help="Parameter name")
    parser.add_argument("--plot", type=str2bool, default=False, help="Plot")
    parser.add_argument("--remove_abnormal", type=str2bool, default=True, help="Remove abnormal period data (20240130-20240219)")
    parser.add_argument("--scores_path", type=str, required=True, help="Path to prediction scores CSV")
    parser.add_argument("--solver_method", type=str, default=None, help="Solver method")
    parser.add_argument("--start_date_shift", type=int, default=None, help="Start date shift in days")
    parser.add_argument("--stk_buy_r", type=float, default=None, help="Stock buy ratio")
    parser.add_argument("--stk_hold_limit", type=float, default=None, help="Stock hold limit")
    parser.add_argument("--strategy", type=str, default="solve", help="Strategy type (solve or topn)")
    parser.add_argument("--tot_hold_num", type=int, default=None, help="Total hold number")
    parser.add_argument("--trade_support", type=int, required=True, help="Trade support type (5 or 7)")
    parser.add_argument("--turn_max", type=float, default=None, help="Turn max")
    return parser.parse_args()


def main():
    # create param manager
    param_manager = ParamManager()

    def create_backtest_wrapper():
        def wrapper():
            params = param_manager.get_param_dict()
            for key, value in params.items():
                if hasattr(config, key):
                    setattr(config, key, value)
            result = run_backtest()
            return result

        return wrapper

    print("超参数优化系统")
    print("=" * 120)

    # load saved info
    saved_info = param_manager.load_params()
    if saved_info:
        print(f"加载的基准信息比率: {saved_info.get('信息比率', 'N/A')}")

    # optimize
    backtest_wrapper = create_backtest_wrapper()
    optimizer = HyperparameterOptimizer(
        backtest_func=backtest_wrapper,
        param_manager=param_manager,
        n_calls=config.N_CALLS,
        n_random_starts=config.N_RANDOM_STARTS,
        random_state=42,
    )

    _ = optimizer.optimize()
    best_result = optimizer.get_best_result()
    param_manager.set_params(best_result["params"])
    param_manager.save_params(best_result["info"])


if __name__ == "__main__":
    args = parse_args()
    config.update_from_args(args)
    main()
