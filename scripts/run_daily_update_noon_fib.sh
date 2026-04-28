#!/bin/bash

PREDICTIONS_DIR="/home/haris/mymodel_noon_fib/predictions/StockPredictor_20260412"
LATEST_FILE=$(ls "$PREDICTIONS_DIR"/predictions_period_*.csv 2>/dev/null | sort -t_ -k3 -r | head -1)

/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_FILE" --trade_support 5 --afternoon_start True --results_path "/home/haris/mymodel_noon_fib/backtests"
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_FILE" --trade_support 7 --afternoon_start True --results_path "/home/haris/mymodel_noon_fib/backtests"
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_FILE" --trade_support 8 --afternoon_start True --results_path "/home/haris/mymodel_noon_fib/backtests"
