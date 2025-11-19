import os
import pandas as pd
import numpy as np
from src.config import *


def load_daily_data(name):
    return pd.read_feather(os.path.join(DAILY_DATA_PATH, f"{name}.feather"))


high_limit = load_daily_data("stk_ztprice").replace(0, np.nan).ffill()
low_limit = load_daily_data("stk_dtprice").replace(0, np.nan).ffill()
pre_close = load_daily_data("stk_preclose").replace(0, np.nan).ffill()
adj_factor = load_daily_data("stk_adjfactor").replace(0, np.nan).ffill()
close = load_daily_data("stk_close").replace(0, np.nan).ffill()
last_zt_df = (close == high_limit).shift(1).fillna(False).astype(int)
upper_price = pre_close + 0.9 * (high_limit - pre_close)
lower_price = pre_close + 0.9 * (low_limit - pre_close)
adj = adj_factor / adj_factor.shift(1)
zs_day = load_daily_data("idx_close")[IDX_NAME2].dropna()
vwap_df = pd.read_feather(os.path.join(DATA_PATH, "vwap.fea"))
scores = pd.read_csv(SCORES_PATH, index_col=0).T.sort_index().shift(1).dropna(how="all")
scores.columns = scores.columns.astype(str).str.zfill(6)
scores = scores[scores.columns[scores.columns.str[0].isin(["0", "3", "6"])]]
scores.index = scores.index.astype(str)
date_list = sorted(scores.index.tolist())
support_dates = sorted(os.listdir(SUPPORT_PATH))
