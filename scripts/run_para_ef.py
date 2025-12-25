import json
import subprocess
import os

# 配置文件路径
JSON_PATH = "/home/haris/project/backtester/para_optimizer_ef/diverse_efficient_parameters_ratio_score.json"
PYTHON_EXE = "/home/haris/miniconda3/envs/myenv/bin/python"
RUN_SCRIPT = "/home/haris/project/backtester/run.py"
SCORES_PATH = "/home/haris/results/predictions/model_re_20251128.csv"


def run_backtests():
    if not os.path.exists(JSON_PATH):
        print(f"错误: 找不到文件 {JSON_PATH}")
        return

    with open(JSON_PATH, "r") as f:
        param_list = json.load(f)

    print(f"找到 {len(param_list)} 组参数，开始执行...")

    for i, params in enumerate(param_list):
        cmd = [
            PYTHON_EXE,
            RUN_SCRIPT,
            "--scores_path",
            SCORES_PATH,
            "--trade_support",
            "7",
            "--citic_limit",
            str(params["param_citic_limit"]),
            "--cmvg_limit",
            str(params["param_cmvg_limit"]),
            "--stk_hold_limit",
            str(params["param_stock_hold_limit"]),
            "--other_limit",
            str(params["param_other_limit"]),
            "--stk_buy_r",
            str(params["param_stock_buy_ratio"]),
            "--turn_max",
            str(params["param_turnover_max"]),
            "--mem_hold",
            str(params["param_memory_hold"]),
            "--plot",
            "False",
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"\n[错误] 第 {i+1} 组参数执行失败: {e}")
            continue


if __name__ == "__main__":
    run_backtests()
