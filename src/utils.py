import os
import numpy as np
import pandas as pd
from src.config import *
from src.data_loader import support_dates

data_cache = {"daily_price": {}, "daily_support": {}}


def get_daily_price(date, vwap_df, close, pre_close, adj, scores, upper_price, lower_price, last_zt_df):
    if date not in data_cache["daily_price"]:
        data_cache["daily_price"][date] = (
            vwap_df.loc[date].dropna(),
            close.loc[date].dropna(),
            pre_close.loc[date].dropna(),
            adj.loc[date].replace(1, np.nan).dropna().to_dict(),
            scores.loc[date].dropna(),
            upper_price.loc[date].dropna(),
            lower_price.loc[date].dropna(),
            last_zt_df.loc[date].dropna(),
        )
    return data_cache["daily_price"][date]


def get_daily_support(str_date):
    if str_date not in data_cache["daily_support"]:
        os.chdir(SUPPORT_PATH)
        last_date = [x for x in support_dates if x < str_date[:8]][-1]
        df = pd.read_feather(last_date)
        sub_code = df.loc[(df["ipo_dates"] > 120) & (df["st"] == 0)].index.tolist()
        sub_code = [c for c in sub_code if c[0] in "036"]
        citic = df[[c for c in df.columns if "citic_b_" in c]].reindex(sub_code).fillna(0)
        cmvg = df[[c for c in df.columns if "cmvg_b_" in c]].reindex(sub_code).fillna(0)
        style = df[[c for c in df.columns if "style_b_" in c]].reindex(sub_code).fillna(0)
        mem = df[IDX_NAME + "_member"].reindex(sub_code).dropna()
        zz_citic = df.loc["idx_" + IDX_NAME, citic.columns]
        zz_cmvg = df.loc["idx_" + IDX_NAME, cmvg.columns]
        zz_style = df.loc["idx_" + IDX_NAME, style.columns]
        data_cache["daily_support"][str_date] = (citic, cmvg, mem, zz_citic, zz_cmvg, style, zz_style, sub_code)
    return data_cache["daily_support"][str_date]
