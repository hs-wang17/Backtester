#!/bin/bash

# 3-2) solve trade support 7 (continuous mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260409.csv --trade_support 7 --continuous_mode True
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260409.csv --trade_support 7 --continuous_mode True  --turn_max 0.031857849848239 --turn_max_second 0.031857849848239
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260409.csv --trade_support 7 --continuous_mode True  --turn_max 0.031857849848239 --turn_max_second 0.031857849848239 --twap_mode True
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260409.csv --trade_support 7 --twap_mode True

/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260412.csv --trade_support 7 --afternoon_start True --continuous_mode True
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260412.csv --trade_support 7 --afternoon_start True --continuous_mode True  --turn_max 0.031857849848239 --turn_max_second 0.031857849848239
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260412.csv --trade_support 7 --afternoon_start True --continuous_mode True  --turn_max 0.031857849848239 --turn_max_second 0.031857849848239 --twap_mode True
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260412.csv --trade_support 7 --afternoon_start True --twap_mode True

