import os
import numpy as np
import pandas as pd
import config as config

data_cache = {"daily_price": {}, "daily_support": {}}


def get_daily_price(date, vwap_df, close, pre_close, adj, scores, upper_price, lower_price, last_zt_df):
    """
    Get the daily price data for a given date.

    Parameters
    ----------
    date : str
        The date for which to retrieve the daily price data.
    vwap_df : pandas.DataFrame
        The dataframe containing vwap data.
    close : pandas.DataFrame
        The dataframe containing close data.
    pre_close : pandas.DataFrame
        The dataframe containing pre-close data.
    adj : pandas.DataFrame
        The dataframe containing adj data.
    scores : pandas.DataFrame
        The dataframe containing scores data.
    upper_price : pandas.DataFrame
        The dataframe containing upper price data.
    lower_price : pandas.DataFrame
        The dataframe containing lower price data.
    last_zt_df : pandas.DataFrame
        The dataframe containing last_zt data.

    Returns
    -------
    tuple
        A tuple containing the daily price data for the given date.
    """
    if scores is None:  # only used when scores is not updated everyday
        data_cache["daily_price"][date] = (
            vwap_df.loc[date].dropna(),
            close.loc[date].dropna(),
            pre_close.loc[date].dropna(),
            adj.loc[date].replace(1, np.nan).dropna().to_dict(),
            None,
            upper_price.loc[date].dropna(),
            lower_price.loc[date].dropna(),
            last_zt_df.loc[date].dropna(),
        )
    elif isinstance(scores, pd.Series):  # if scores is Series
        data_cache["daily_price"][date] = (
            vwap_df.loc[date].dropna(),
            close.loc[date].dropna(),
            pre_close.loc[date].dropna(),
            adj.loc[date].replace(1, np.nan).dropna().to_dict(),
            scores.dropna(),
            upper_price.loc[date].dropna(),
            lower_price.loc[date].dropna(),
            last_zt_df.loc[date].dropna(),
        )
    else:  # if scores is DataFrame
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
    """
    Get the daily support data for a given date.

    Parameters
    ----------
    str_date : str
        The date for which to retrieve the daily support data.

    Returns
    -------
    tuple
        A tuple containing the daily support data for the given date.
    """
    if str_date not in data_cache["daily_support"]:
        os.chdir(config.SUPPORT_PATH)
        support_dates = sorted(os.listdir(config.SUPPORT_PATH))
        last_date = [x for x in support_dates if x < str_date[:8]][-1]
        df = pd.read_feather(last_date)
        # ignore the stocks listed within 120 days (ipo) and ST stocks
        sub_code = df.loc[(df["ipo_dates"] > 120) & (df["st"] == 0)].index.tolist()
        # ignore the stocks in 036 market (B stock, ST stock, etc.)
        sub_code = [c for c in sub_code if c[0] in "036"]
        citic = df[[c for c in df.columns if "citic_b_" in c]].reindex(sub_code).fillna(0)
        cmvg = df[[c for c in df.columns if "cmvg_b_" in c]].reindex(sub_code).fillna(0)
        style = df[[c for c in df.columns if "style_b_" in c]].reindex(sub_code).fillna(0)
        mem = df[config.IDX_NAME + "_member"].reindex(sub_code).dropna()
        zz_citic = df.loc["idx_" + config.IDX_NAME, citic.columns]
        zz_cmvg = df.loc["idx_" + config.IDX_NAME, cmvg.columns]
        zz_style = df.loc["idx_" + config.IDX_NAME, style.columns]
        data_cache["daily_support"][str_date] = (citic, cmvg, mem, zz_citic, zz_cmvg, style, zz_style, sub_code)
    return data_cache["daily_support"][str_date]
