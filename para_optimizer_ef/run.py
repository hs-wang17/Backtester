import json
import subprocess
import os
import argparse


PYTHON_EXE = "/home/haris/miniconda3/envs/myenv/bin/python"
RUN_SCRIPT = "/home/haris/project/backtester/run.py"
SCORES_PATH = "/home/haris/mymodel/predictions/StockPredictor_20251231_merged_all_stocks_until_20250630.csv"


def run_backtests(json_path):
    if not os.path.exists(json_path):
        print(f"错误: 找不到文件 {json_path}")
        return

    with open(json_path, "r") as f:
        param_list = json.load(f)

    print(f"参数文件: {json_path}")
    print(f"找到 {len(param_list)} 组参数，开始执行...")

    para_name = os.path.splitext(os.path.basename(json_path))[0]

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
            "--para_name",
            para_name,
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"\n[错误] 第 {i + 1} 组参数执行失败: {e}")
            continue


def parse_args():
    parser = argparse.ArgumentParser(description="批量回测参数执行脚本")
    parser.add_argument("--json_path", required=True, help="参数 JSON 文件路径")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_backtests(args.json_path)
    print("所有参数执行完成")
