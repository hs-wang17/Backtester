import argparse
from param_manager import ParamManager
from optimizer import HyperparameterOptimizer
import config as config
from backtest import run_backtest


def main():
    # create param manager
    param_manager = ParamManager()

    def create_backtest_wrapper():
        """create a backtest function wrapper that updates config with current params and runs backtest"""

        def wrapper():
            # get current params
            params = param_manager.get_param_dict()
            # update config
            for key, value in params.items():
                if hasattr(config, key):
                    setattr(config, key, value)
            # run backtest
            result = run_backtest()
            return result

        return wrapper

    print("=" * 60)
    print("超参数优化系统")
    print("=" * 60)

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
    result = optimizer.optimize()
    best_result = optimizer.get_best_result()
    print("\n" + "=" * 60)
    print("最终最佳结果:")
    print(f"信息比率: {best_result['score']:.4f}")
    print(f"超额年化收益: {best_result['info'].get('超额年化收益', 'N/A'):.4f}")
    print(f"超额年化波动: {best_result['info'].get('超额年化波动', 'N/A'):.4f}")
    param_manager.set_params(best_result["params"])
    param_manager.save_params(best_result["info"])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scores_path", type=str, required=True, help="Path to prediction scores CSV")
    parser.add_argument("--trade_support", type=int, required=True, help="Trade support type (5 or 7)")
    parser.add_argument("--n_calls", type=int, default=200, help="Number of calls to optimize (default: 200)")
    parser.add_argument("--n_random_starts", type=int, default=100, help="Number of random starts (default: 100)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config.update_from_args(args)
    main()
