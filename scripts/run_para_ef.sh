#!/bin/bash

JSON_DIR=/home/haris/project/backtester/para_optimizer_ef/parameters

for json in ${JSON_DIR}/*5.json
do
    echo "执行参数文件: $json"
    /home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/para_optimizer_ef/run.py --json_path "$json"
done