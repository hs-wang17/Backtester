import os
import pandas as pd
import numpy as np
import datetime

# ==================== 路径配置 ====================
save_path = r"/home/haris/project/backtester/data/trade_support7"  # 每日特征输出路径
path_dailyData = r"/home/haris/data/data_frames"  # 日频基础数据
idx_weight_path = r"/home/haris/data/IndexWeightData"  # 指数权重文件


def load_daily_data(name):
    """读取日频feather数据"""
    return pd.read_feather(os.path.join(path_dailyData, f"{name}.feather"))


# ==================== 基础数据准备 ====================
citic1 = load_daily_data("stk_citic1_name").ffill()  # 中信一级行业分类
date_list = citic1.index.tolist()  # 所有交易日列表
ipo_dates = (load_daily_data("stk_adjclose").replace(0, np.nan).ffill() > 0).cumsum()  # 上市天数（首次有复权收盘价算第1天）

is_st = load_daily_data("stk_is_st_stock")  # ST标记
is_stop = load_daily_data("stk_is_stop_stock")  # 停牌标记
is_tuishi_ing = load_daily_data("stk_is_tuishi_ing")  # 退市整理标记
st_status = (is_st + is_stop + is_tuishi_ing).filLna(1)  # ST/停牌/退市整理标记（0=正常）, 有任一为1即视为不可交易

cmv = load_daily_data("stk_neg_market_value") / 1e8  # 流通市值（单位：亿）
cmv = cmv[[c for c in cmv.columns if c[0] in "036"]]  # 只保留A股
cmv_group_list = [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64]  # 累计百分位阈值
cmvr = cmv.rank(axis=1, pct=True, ascending=False)  # 大市值排前面
cmv_group = pd.DataFrame(0, index=cmv.index, columns=cmv.columns)
for g in cmv_group_list:
    cmv_group += (cmvr <= g).astype(int)  # 流通市值分组（1=最大，7=最小）

# ==================== 风格因子原始值 ====================
mv = load_daily_data("stk_neg_market_value").replace(0, np.nan).ffill()  # 总市值（用于size、turn）
pb = 1 / load_daily_data("stk_PB").ffill()  # PB倒数（价值）
pb = pb[[c for c in pb.columns if c[0] in "036"]]  # 只保留A股
pe = 1 / load_daily_data("stk_PE").ffill()  # PE倒数（价值）
pe = pe[[c for c in pe.columns if c[0] in "036"]]  # 只保留A股
price = load_daily_data("stk_adjclose").replace(0, np.nan).ffill()  # 复权收盘价
price0 = load_daily_data("stk_close").replace(0, np.nan).ffill()
zt_price = load_daily_data("stk_ztprice").replace(0, np.nan).ffill()
dt_price = load_daily_data("stk_dtprice").replace(0, np.nan).ffill()
zdt_info = (price0 == zt_price).astype(int) - (price0 == dt_price).astype(int)
dealnum = load_daily_data("stk_num").fillna(0)
amount = load_daily_data("stk_amount").fillna(0)
ret1 = price.pct_change()  # 1日收益
ret20 = price.pct_change(20)  # 20日收益
std = price.pct_change().rolling(20, min_periods=3).std()  # 20日收益波动率
turn = (load_daily_data("stk_amount") / mv).fillna(0).rolling(20).mean()  # 20日平均换手率
value_fac = pe.rank(ascending=False, pct=True, axis=1) + pb.rank(ascending=False, pct=True, axis=1)
amt_increase = (amount.rolling(5, min_periods=3).mean() / amount.rolling(20, min_periods=10).mean().shift(5)).replace([np.inf, -np.inf], np.nan)
amt_perdeal = (amount / dealnum).replace([np.inf, -np.inf], np.nan)


# ==================== 计算每日超额收益（行业/市值/风格） ====================
ind_ret_dict, mvg_ret_dict, sty_ret_dict, mkt_ret_dict = ({}, {}, {}, {})

for date in date_list[70:]:
    last_date = date_list[date_list.index(date) - 1]
    td_citic = citic1.loc[last_date].dropna()
    td_cmvg = cmv_group.loc[last_date].dropna()
    td_style = pd.concat(
        [
            mv.loc[last_date],
            value_fac.loc[last_date],
            ret20.shift(1).loc[last_date],
            ret1.loc[last_date],
            turn.loc[last_date],
            std.loc[last_date],
            price0.loc[last_date],
            amt_increase.loc[last_date],
            amt_perdeal.loc[last_date],
        ],
        axis=1,
        keys=["size", "value", "ret20", "ret1", "turn", "std", "price", "amtchange", "bigorder"],
    )
    td_ret = ret1.loc[date].dropna()

    ipo_num = ipo_dates.loc[date].fillna(0)
    ipo60_list = ipo_num[ipo_num > 60].index.tolist()
    td_st = st_status.loc[date]
    td_st_list = td_st[td_st == 0].index.tolist()
    td_code_list = [x for x in td_ret.index if ((x in ipo60_list) and (x in td_st_list) and (x[0] in ["0", "3", "6"]))]

    td_info = pd.concat([td_ret, td_citic, td_cmvg, np.sqrt(mv.loc[last_date])], axis=1, keys=["ret", "citic", "cmvg", "w"]).reindex(td_code_list)

    td_info["ret_w"] = td_info["ret"] * td_info["w"]
    mkt_ret = td_info["ret_w"].sum() / td_info["w"].sum()
    td_info2 = td_info.groupby("citic")[["ret_w", "w"]].sum()
    td_info3 = td_info.groupby("cmvg")[["ret_w", "w"]].sum()

    ind_ret_dict[date] = td_info2["ret_w"] / td_info2["w"] - mkt_ret
    mvg_ret_dict[date] = td_info3["ret_w"] / td_info3["w"] - mkt_ret

    td_style_rank = td_style.reindex(td_code_list).rank(pct=True).fillna(0.5)
    td_style_top = (td_style_rank > 0.75).astype(int)
    td_style_top_ret = td_info["ret_w"].dot(td_style_top) / td_info["w"].dot(td_style_top)
    td_style_tail = (td_style_rank < 0.25).astype(int)
    td_style_tail_ret = td_info["ret_w"].dot(td_style_tail) / td_info["w"].dot(td_style_tail)
    sty_ret_dict[date] = td_style_top_ret - td_style_tail_ret
    mkt_ret_dict[date] = mkt_ret

ind_ret_df = pd.DataFrame(ind_ret_dict).T
style_ret_df = pd.DataFrame(sty_ret_dict).T
cmvg_ret_df = pd.DataFrame(mvg_ret_dict).T
mkt_ret_s = pd.Series(mkt_ret_dict)

# 更新数据
os.makedirs(save_path, exist_ok=True)
ok_list = os.listdir(save_path)
for date in date_list[150:]:
    dt_str = date
    if date + ".fea" in ok_list:
        continue

    sub_ind_ret = ind_ret_df.loc[:date].iloc[-60:]
    sub_sty_ret = style_ret_df.loc[:date].iloc[-60:]
    sub_mkt_ret = mkt_ret_s.loc[:date].iloc[-60:]
    sub_cmvg_ret = cmvg_ret_df.loc[:date].iloc[-60:]
    sub_ret = ret1.loc[:date].iloc[-60:].dropna(axis=1, how="all").fillna(0)
    td_code_list = [x for x in sub_ret.columns if x[0] in ("0", "3", "6")]

    td_citic = pd.get_dummies(citic1.loc[date])
    td_cmvg = pd.get_dummies(cmv_group.loc[date])
    sub_ind_ret2 = sub_ind_ret.div((sub_ind_ret**2).mean(), axis=1)
    sub_cmvg_ret2 = sub_cmvg_ret.div((sub_cmvg_ret**2).mean(), axis=1)
    sub_sty_ret2 = sub_sty_ret.div((sub_sty_ret**2).mean(), axis=1)
    sub_ret2 = sub_ret.apply(lambda x: x - sub_mkt_ret)
    td_style = pd.concat(
        [
            mv.loc[date],
            value_fac.loc[date],
            ret20.shift(1).loc[date],
            ret1.loc[date],
            turn.loc[date],
            std.loc[date],
            price0.loc[date],
            amt_increase.loc[date],
            amt_perdeal.loc[date],
        ],
        axis=1,
        keys=["size", "value", "ret20", "ret1", "turn", "std", "price", "amtchange", "bigorder"],
    )
    td_style_rank = td_style.reindex(td_code_list).rank(pct=True).fillna(0.5)

    beta_ind = (sub_ret2.T.dot(sub_ind_ret2) / 60).clip(-4, 4)
    beta_cmvg = (sub_ret2.T.dot(sub_cmvg_ret2) / 60).clip(-10, 10)
    beta_style = (sub_ret2.T.dot(sub_sty_ret2) / 60).clip(-3, 3)

    td_citic.columns = td_citic.columns.map(lambda x: "citic_r_" + x)
    td_cmvg.columns = td_cmvg.columns.map(lambda x: "cmvg_r_" + str(int(x)))
    td_style_rank.columns = td_style_rank.columns.map(lambda x: "style_r_" + x)
    beta_ind.columns = beta_ind.columns.map(lambda x: "citic_b_" + x)
    beta_cmvg.columns = beta_cmvg.columns.map(lambda x: "cmvg_b_" + str(int(x)))
    beta_style.columns = beta_style.columns.map(lambda x: "style_b_" + x)

    td_ans = pd.concat([td_citic, td_cmvg, td_style_rank, beta_ind, beta_cmvg, beta_style], axis=1).reindex(td_code_list).astype(float)  # .reset_index()
    td_ans = td_ans.fillna(td_ans.median())

    ipo_num = ipo_dates.loc[date].fillna(0)
    td_st = st_status.loc[date]
    td_price = price0.loc[date]
    td_zdt_info = zdt_info.loc[date]

    td_ans["ipo_dates"] = ipo_num.reindex(td_code_list).fillna(0)
    td_ans["st"] = td_st.reindex(td_code_list).fillna(1)
    td_ans["close"] = td_price.reindex(td_code_list)
    td_ans["zdt"] = td_zdt_info.reindex(td_code_list)

    os.chdir(idx_weight_path)
    w_data = pd.read_feather(date + ".fea")
    for idx_name in ["zz500", "zz1000", "hs300"]:
        idx_name2 = idx_name.upper()

        sub_w = w_data.loc[w_data["index_name"] == idx_name2].set_index("stock_code")
        td_member2 = (sub_w["stock_weight"].reindex(td_code_list)).fillna(0)
        td_member2 = td_member2 / td_member2.sum()
        td_ans.loc["idx_" + idx_name] = td_ans.reindex(td_code_list).T.dot(td_member2)
        td_ans[idx_name + "_member"] = td_member2

    os.chdir(save_path)
    td_ans.fillna(0).to_feather(date + ".fea")

print("Done:", datetime.datetime.now())
