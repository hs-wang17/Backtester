#!/bin/bash

# single score
# 1-1) solve trade support 5 (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_history_all_stocks.csv --trade_support 5 --results_path "/home/haris/mymodel/backtests"
# 1-2) solve trade support 7 (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_history_all_stocks.csv --trade_support 7 --results_path "/home/haris/mymodel/backtests"
# 1-3) solve trade support barra (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_history_all_stocks.csv --trade_support 8 --results_path "/home/haris/mymodel/backtests"

# 1-1) solve trade support 5 (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_history_all_stocks.csv --trade_support 5 --results_path "/home/haris/mymodel_10/backtests"
# 1-2) solve trade support 7 (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_history_all_stocks.csv --trade_support 7 --results_path "/home/haris/mymodel_10/backtests"
# 1-3) solve trade support barra (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_history_all_stocks.csv --trade_support 8 --results_path "/home/haris/mymodel_10/backtests"

# 2-1) solve trade support 5 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon/predictions/StockPredictor_20260302_history_all_stocks.csv --trade_support 5 --afternoon_start True --results_path "/home/haris/mymodel_noon/backtests"
# 2-2) solve trade support 7 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon/predictions/StockPredictor_20260302_history_all_stocks.csv --trade_support 7 --afternoon_start True --results_path "/home/haris/mymodel_noon/backtests"
# 2-3) solve trade support barra (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon/predictions/StockPredictor_20260302_history_all_stocks.csv --trade_support 8 --afternoon_start True --results_path "/home/haris/mymodel_noon/backtests"

# 2-1) solve trade support 5 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon_10/predictions/StockPredictor_20260308_history_all_stocks.csv --trade_support 5 --afternoon_start True --results_path "/home/haris/mymodel_noon_10/backtests"
# 2-2) solve trade support 7 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon_10/predictions/StockPredictor_20260308_history_all_stocks.csv --trade_support 7 --afternoon_start True --results_path "/home/haris/mymodel_noon_10/backtests"
# 2-3) solve trade support barra (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon_10/predictions/StockPredictor_20260308_history_all_stocks.csv --trade_support 8 --afternoon_start True --results_path "/home/haris/mymodel_noon_10/backtests"