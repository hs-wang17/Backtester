#!/bin/bash

# single score
# 1) solve trade support 5
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231/predictions_period_20260130.csv --trade_support 5
# 2) solve trade support 7
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_merged.csv --trade_support 7
# 3) topn
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260122.csv --trade_support 7 --strategy topn
# 4) solve trade support 5 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20260128.csv --trade_support 5 --afternoon_start True
# 5) solve trade support 7 (afternoon)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel_noon/predictions/StockPredictor_20260302_merged.csv --trade_support 7 --afternoon_start True
# 6) solve trade support 5 (APM mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_merged.csv --noon_scores_path /home/haris/mymodel_noon/predictions/StockPredictor_20260302_merged.csv --trade_support 5 --apm_mode True
# 7) solve trade support 7 (APM mode)
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/mymodel/predictions/StockPredictor_20251231_merged.csv --noon_scores_path /home/haris/mymodel_noon/predictions/StockPredictor_20260302_merged.csv --trade_support 7 --apm_mode True
# multiple scores
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/StockPredictor_20251231.csv,/home/haris/results/predictions/240_120_ret3_5_10_20_mse2.csv --trade_support 7
