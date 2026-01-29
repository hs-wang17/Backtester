#!/bin/bash

# single score
# 1) solve trade support 5
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_merged_all_stocks.csv --trade_support 5
# 2) solve trade support 7
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_merged_all_stocks.csv --trade_support 7
# 3) topn
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260122.csv --trade_support 7 --strategy topn

# multiple scores
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20251231.csv,/home/haris/results/predictions/240_120_ret3_5_10_20_mse2.csv --trade_support 7
