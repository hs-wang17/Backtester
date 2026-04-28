#!/bin/bash

# single score
# 1-1) solve trade support 5 (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260327.csv --trade_support 5
# 1-2) solve trade support 7 (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260409.csv --trade_support 7
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 7
# 1-3) solve trade support barra (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260327.csv --trade_support 8
# 1-4) topn (morning)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260327.csv --trade_support 7 --strategy topn

# 2-1) solve trade support 5 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260316_113125.csv --trade_support 5 --afternoon_start True
# 2-2) solve trade support 7 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260412.csv --trade_support 7 --afternoon_start True
# 2-3) solve trade support barra (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260302.csv --trade_support 8 --afternoon_start True

# 3-1) solve trade support 5 (continuous mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 5 --continuous_mode True
# 3-2) solve trade support 7 (continuous mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 7 --continuous_mode True  --turn_max 0.031857849848239 --turn_max_second 0.031857849848239 --twap_mode True
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon_10/predictions/StockPredictor_20260308_merged_all_stocks.csv --trade_support 7 --afternoon_start True --continuous_mode True  --turn_max 0.031857849848239 --turn_max_second 0.031857849848239 --twap_mode True
# 3-3) solve trade support barra (continuous mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 8 --continuous_mode True

# 4-1) solve trade support 5 (continuous general mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 5 --continuous_general_mode True
# 4-2) solve trade support 7 (continuous general mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 7 --continuous_general_mode True --turn_max 0.0106192832827463 --turn_max_second 0.0106192832827463
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon_10/predictions/StockPredictor_20260308_merged_all_stocks.csv --trade_support 7 --afternoon_start True --continuous_general_mode True  --turn_max 0.0106192832827463 --turn_max_second 0.0106192832827463
# 4-3) solve trade support barra (continuous general mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_10/predictions/StockPredictor_20260306_merged_all_stocks.csv --trade_support 8 --continuous_general_mode True

# 5-1) solve trade support 5 (APM mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260306.csv --noon_scores_path /home/haris/results/predictions/StockPredictor_20260308.csv --trade_support 5 --apm_mode True
# 5-2) solve trade support 7 (APM mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260306.csv --noon_scores_path /home/haris/results/predictions/StockPredictor_20260308.csv --trade_support 7 --apm_mode True
# 5-3) solve trade support barra (APM mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260306_StockPredictor_20260308_mix0.5.csv --noon_scores_path /home/haris/results/predictions/StockPredictor_20260308_StockPredictor_20260306_mix0.5.csv --trade_support 8 --apm_mode True

# 6-1) solve trade support 5 (call)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260312.csv --trade_support 5 --call_start True
# 6-2) solve trade support 7 (call)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260312.csv --trade_support 7 --call_start True
# 6-3) solve trade support barra (call)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260312.csv --trade_support 8 --call_start True

# multiple scores
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_merged.csv,/home/haris/mymodel_10/predictions/StockPredictor_20260306_merged.csv --trade_support 7 --mix_coefficient 0.0