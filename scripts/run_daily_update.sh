#!/bin/bash

PREDICTIONS_DIR="/home/haris/mymodel/predictions/StockPredictor_20251231"
LATEST_FILE=$(ls "$PREDICTIONS_DIR"/predictions_period_*.csv 2>/dev/null | sort -t_ -k3 -r | head -1)

/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_FILE" --trade_support 5 --results_path "/home/haris/mymodel/backtests"
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_FILE" --trade_support 7 --results_path "/home/haris/mymodel/backtests"
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_FILE" --trade_support 8 --results_path "/home/haris/mymodel/backtests"
