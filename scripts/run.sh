#!/bin/bash

/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/model_re_20251128.csv --trade_support 5 --solver_method twostage
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/model_re_20251128.csv --trade_support 7 --solver_method twostage
/home/haris/miniconda3/envs/myenv/bin/python /home/haris/project/backtester/run.py --scores_path /home/haris/results/predictions/model_re_20251128_head.csv --trade_support 7
