import argparse
from param_manager import ParamManager
from optimizer import HyperparameterOptimizer
import config as config
from backtest import run_backtest

# 创建全局参数管理器实例
param_manager = ParamManager()


def create_backtest_wrapper():
    """创建回测函数包装器"""

    def wrapper():
        # 获取当前参数
        params = param_manager.get_param_dict()

        # 更新config中的参数
        for key, value in params.items():
            if hasattr(config, key):
                setattr(config, key, value)

        # 运行回测
        result = run_backtest()
        return result

    return wrapper


def main():
    """主函数"""
    print("=" * 60)
    print("超参数优化系统")
    print("=" * 60)

    # 加载现有最佳参数（如果存在）
    saved_info = param_manager.load_params()
    if saved_info:
        print(f"加载的基准信息比率: {saved_info.get('信息比率', 'N/A')}")

    # 创建回测函数包装器
    backtest_wrapper = create_backtest_wrapper()

    # 创建优化器
    optimizer = HyperparameterOptimizer(
        backtest_func=backtest_wrapper, param_manager=param_manager, n_calls=50, n_random_starts=10, random_state=42  # 可以根据需要调整  # 随机初始点数量
    )

    # 执行优化
    result = optimizer.optimize()

    # 输出最终结果
    best_result = optimizer.get_best_result()
    print("\n" + "=" * 60)
    print("最终最佳结果:")
    print(f"信息比率: {best_result['score']:.4f}")
    print(f"超额年化收益: {best_result['info'].get('超额年化收益', 'N/A'):.4f}")
    print(f"超额年化波动: {best_result['info'].get('超额年化波动', 'N/A'):.4f}")

    # 保存最终参数
    param_manager.set_params(best_result["params"])
    param_manager.save_params(best_result["info"])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scores_path", type=str, required=True, help="Path to prediction scores CSV")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config.update_from_args(args)
    main()
