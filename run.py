import argparse
from src import config
from src.backtest import run_backtest


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scores_path", type=str, required=True, help="Path to prediction scores CSV")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config.update_from_args(args)
    run_backtest()
