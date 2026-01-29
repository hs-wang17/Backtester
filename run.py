import argparse
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
    parser.add_argument("--daily_sell_num", type=int, default=20, help="Daily sell number")
    parser.add_argument("--hold_init", type=str, default="solve", help="Hold initialization method (member or solve)")
    parser.add_argument("--lambda_sparse", type=float, default=0.0, help="Lambda sparse")
    parser.add_argument("--mem_hold", type=float, default=None, help="Member hold limit")
    parser.add_argument("--other_limit", type=float, default=None, help="Other limit")
    parser.add_argument("--para_name", type=str, default=None, help="Parameter name")
    parser.add_argument("--plot", type=str2bool, default=True, help="Plot")
    parser.add_argument("--scores_path", type=str, required=True, help="Path to prediction scores CSV")
    parser.add_argument("--solver_method", type=str, default="basic", help="Solver method")
    parser.add_argument("--start_date_shift", type=int, default=0, help="Start date shift in days")
    parser.add_argument("--stk_buy_r", type=float, default=None, help="Stock buy ratio")
    parser.add_argument("--stk_hold_limit", type=float, default=None, help="Stock hold limit")
    parser.add_argument("--strategy", type=str, default="solve", help="Strategy type (solve or topn)")
    parser.add_argument("--tot_hold_num", type=int, default=200, help="Total hold number")
    parser.add_argument("--trade_support", type=int, required=True, help="Trade support type (5 or 7)")
    parser.add_argument("--turn_max", type=float, default=None, help="Turn max")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config.update_from_args(args)
    run_backtest()
