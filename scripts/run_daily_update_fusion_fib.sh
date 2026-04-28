#!/bin/bash

PREDICTIONS_MORNING_DIR="/home/haris/mymodel_fusion_fib/predictions_morning_fusion"
LATEST_MORNING_FILE=$(ls "$PREDICTIONS_MORNING_DIR"/predictions_period_*.csv 2>/dev/null | sort -t_ -k3 -r | head -1)
PREDICTIONS_NOON_DIR="/home/haris/mymodel_fusion_fib/predictions_noon_fusion"
LATEST_NOON_FILE=$(ls "$PREDICTIONS_NOON_DIR"/predictions_period_*.csv 2>/dev/null | sort -t_ -k3 -r | head -1)

/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_MORNING_FILE" --noon_scores_path "$LATEST_NOON_FILE" --apm_mode True --trade_support 5 --results_path "/home/haris/mymodel_fusion_fib/backtests"
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_MORNING_FILE" --noon_scores_path "$LATEST_NOON_FILE" --apm_mode True --trade_support 7 --results_path "/home/haris/mymodel_fusion_fib/backtests"
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path "$LATEST_MORNING_FILE" --noon_scores_path "$LATEST_NOON_FILE" --apm_mode True --trade_support 8 --results_path "/home/haris/mymodel_fusion_fib/backtests"

# /home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_fusion_fib/StockPredictor_20260430_morning_history_all_stocks.csv --noon_scores_path /home/haris/mymodel_fusion_fib/StockPredictor_20260430_noon_history_all_stocks.csv --apm_mode True --trade_support 5 --results_path "/home/haris/mymodel_fusion_fib/backtests"
# /home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_fusion_fib/StockPredictor_20260430_morning_history_all_stocks.csv --noon_scores_path /home/haris/mymodel_fusion_fib/StockPredictor_20260430_noon_history_all_stocks.csv --apm_mode True --trade_support 7 --results_path "/home/haris/mymodel_fusion_fib/backtests"
# /home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_fusion_fib/StockPredictor_20260430_morning_history_all_stocks.csv --noon_scores_path /home/haris/mymodel_fusion_fib/StockPredictor_20260430_noon_history_all_stocks.csv --apm_mode True --trade_support 8 --results_path "/home/haris/mymodel_fusion_fib/backtests"
