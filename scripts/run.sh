#!/bin/bash

/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20251231.csv --trade_support 5 --hold_init solve
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20251231.csv,/home/haris/results/predictions/240_120_ret3_5_10_20_mse2.csv --trade_support 7 --hold_init solve
