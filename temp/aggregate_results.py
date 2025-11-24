import pandas as pd
import os

results_dirs = "/home/user0/results/predictions/StockPredictor_20251119/"
results_list = []
for d in sorted(os.listdir(results_dirs))[:3]:
    results_list.append(pd.read_csv(results_dirs + d).set_index("stock_code"))

results_df = pd.concat(results_list, join="outer", axis=1)
results_df.to_csv(results_dirs[:-1] + "_combined_predictions.csv")
