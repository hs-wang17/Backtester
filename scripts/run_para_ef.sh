#!/bin/bash

JSON_DIR=/home/haris/project/backtester/para_optimizer_ef/parameters

# for json in ${JSON_DIR}/*7.json
# for json in /home/haris/project/backtester/para_optimizer_ef/parameters/diverse_efficient_parameters_ratio_score_trade_support7.json
for json in /home/haris/project/backtester/para_optimizer_ef/parameters/diverse_efficient_parameters_std_score_trade_support7.json
do
    echo "执行参数文件: $json"
    /home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/para_optimizer_ef/run.py --json_path "$json"
done