import os
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm

# ==================== 路径配置 ====================
save_path = r"/home/haris/project/backtester/data/trade_support5"  # 每日特征输出路径
path_dailyData = r"/home/haris/data/data_frames"  # 日频基础数据
idx_weight_path = r"/home/haris/data/IndexWeightData"  # 指数权重文件
min_path = r"/home/haris/data/min_data"  # 分钟线数据
backtest_path = r"/home/haris/project/backtester/data"  # VWAP输出路径


def load_daily_data(name):
    """读取日频feather数据"""
    return pd.read_feather(os.path.join(path_dailyData, f"{name}.feather"))


# ==================== 计算早盘30分钟VWAP ====================
# os.chdir(min_path)
# files = os.listdir()
# vwap_dict = {}

# for f in tqdm(files):
#     data = pd.read_feather(f)
#     vol = data.pivot(index="time", columns="code", values="volume")
#     amt = data.pivot(index="time", columns="code", values="amount")
#     # 早盘前30分钟成交额/成交量 = VWAP
#     vwap_dict[f[:8]] = amt.iloc[:30].sum() / (vol.iloc[:30].sum().replace(0, np.nan))

# vwap_df = pd.DataFrame(vwap_dict).T.sort_index()
# os.chdir(backtest_path)
# vwap_df.to_feather("vwap.fea")

# # ==================== 计算午盘30分钟VWAP ====================
os.chdir(min_path)
files = os.listdir()
vwap_noon_dict = {}

for f in tqdm(files):
    data = pd.read_feather(f)
    vol = data.pivot(index="time", columns="code", values="volume")
    amt = data.pivot(index="time", columns="code", values="amount")
    # 午盘前30分钟成交额/成交量 = VWAP
    vwap_noon_dict[f[:8]] = amt.iloc[121:151].sum() / (vol.iloc[121:151].sum().replace(0, np.nan))

vwap_noon_df = pd.DataFrame(vwap_noon_dict).T.sort_index()
os.chdir(backtest_path)
vwap_noon_df.to_feather("vwap_noon.fea")


# # ==================== 基础数据准备 ====================
# citic1 = load_daily_data("stk_citic1_name").ffill()  # 中信一级行业分类
# date_list = citic1.index.tolist()  # 所有交易日列表
# ipo_dates = (load_daily_data("stk_adjclose").replace(0, np.nan).ffill() > 0).cumsum()  # 上市天数（首次有复权收盘价算第1天）

# is_st = load_daily_data("stk_is_st_stock")  # ST标记
# is_stop = load_daily_data("stk_is_stop_stock")  # 停牌标记
# is_tuishi_ing = load_daily_data("stk_is_tuishi_ing")  # 退市整理标记
# st_status = (is_st + is_stop + is_tuishi_ing).fillna(1)  # ST/停牌/退市整理标记（0=正常）, 有任一为1即视为不可交易

# cmv = load_daily_data("stk_neg_market_value") / 1e8  # 流通市值（单位：亿）
# cmv = cmv[[c for c in cmv.columns if c[0] in "036"]]  # 只保留A股
# cmv_group_list = [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64]  # 累计百分位阈值
# cmvr = cmv.rank(axis=1, pct=True, ascending=False)  # 大市值排前面
# cmv_group = pd.DataFrame(0, index=cmv.index, columns=cmv.columns)
# for g in cmv_group_list:
#     cmv_group += (cmvr <= g).astype(int)  # 流通市值分组（1=最大，7=最小）


# # ==================== 风格因子原始值 ====================
# mv = load_daily_data("stk_neg_market_value").replace(0, np.nan).ffill()  # 总市值（用于size、turn）
# price = load_daily_data("stk_adjclose").replace(0, np.nan).ffill()  # 复权收盘价
# ret1 = price.pct_change()  # 1日收益
# ret20 = price.pct_change(20)  # 20日收益
# std = price.pct_change().rolling(20, min_periods=3).std()  # 20日收益波动率
# turn = (load_daily_data("stk_amount") / mv).fillna(0).rolling(20).mean()  # 20日平均换手率
# pb = 1 / load_daily_data("stk_PB").ffill()  # PB倒数（价值）
# pb = pb[[c for c in pb.columns if c[0] in "036"]]  # 只保留A股
# pe = 1 / load_daily_data("stk_PE").ffill()  # PE倒数（价值）
# pe = pe[[c for c in pe.columns if c[0] in "036"]]  # 只保留A股
# value_fac = pb.rank(ascending=False, pct=True, axis=1) + pe.rank(ascending=False, pct=True, axis=1)  # 综合价值因子


# # ==================== 计算每日超额收益（行业/市值/风格） ====================
# ind_ret_dict, mvg_ret_dict, sty_ret_dict, mkt_ret_dict = ({}, {}, {}, {})

# for date in date_list[70:]:  # 前70天用于滚动计算
#     last_date = date_list[date_list.index(date) - 1]  # 因子排序使用前一天数据
#     td_ret = ret1.loc[date].dropna()  # 当日个股收益率

#     # 前一日因子值
#     td_citic = citic1.loc[last_date].dropna()
#     td_cmvg = cmv_group.loc[last_date].dropna()
#     td_style = pd.concat(
#         [
#             mv.loc[last_date],
#             value_fac.loc[last_date],
#             ret20.shift(1).loc[last_date],
#             ret1.loc[last_date],
#             turn.loc[last_date],
#             std.loc[last_date],
#             load_daily_data("stk_close").replace(0, np.nan).ffill().loc[last_date],
#         ],
#         axis=1,
#         keys=["size", "value", "ret20", "ret1", "turn", "std", "price"],
#     )

#     w_sqrt = np.sqrt(mv.loc[last_date])  # 权重 = sqrt(流通市值)

#     # 入池条件：上市>60天、非ST、A股
#     ipo60 = ipo_dates.loc[date][ipo_dates.loc[date] > 60].index
#     no_st = st_status.loc[date][st_status.loc[date] == 0].index
#     codes = [c for c in td_ret.index if c in ipo60 and c in no_st and c[0] in "036"]

#     # 合并当日收益 + 分组 + 权重
#     td_info = pd.concat([td_ret, td_citic, td_cmvg, w_sqrt], axis=1, keys=["ret", "citic", "cmvg", "w"]).reindex(codes)
#     td_info["ret_w"] = td_info["ret"] * td_info["w"]

#     # 市场加权收益
#     mkt_ret = td_info["ret_w"].sum() / td_info["w"].sum()
#     mkt_ret_dict[date] = mkt_ret

#     # 行业超额收益（等权后减市场）
#     ind_ret = td_info.groupby("citic")[["ret_w", "w"]].sum()
#     ind_ret_dict[date] = ind_ret["ret_w"] / ind_ret["w"] - mkt_ret

#     # 市值组超额收益
#     mvg_ret = td_info.groupby("cmvg")[["ret_w", "w"]].sum()
#     mvg_ret_dict[date] = mvg_ret["ret_w"] / mvg_ret["w"] - mkt_ret

#     # 风格超额收益：前25% - 后25%
#     style_rank = td_style.reindex(codes).rank(pct=True).fillna(0.5)
#     top25 = (style_rank > 0.75).astype(int)
#     bot25 = (style_rank < 0.25).astype(int)
#     ret_top = td_info["ret_w"].dot(top25) / td_info["w"].dot(top25)
#     ret_bot = td_info["ret_w"].dot(bot25) / td_info["w"].dot(bot25)
#     sty_ret_dict[date] = ret_top - ret_bot

# # 转为DataFrame
# mkt_ret_s = pd.Series(mkt_ret_dict)
# ind_ret_df = pd.DataFrame(ind_ret_dict).T
# style_ret_df = pd.DataFrame(sty_ret_dict).T
# cmvg_ret_df = pd.DataFrame(mvg_ret_dict).T

# # ==================== 生成每日特征文件 ====================
# os.makedirs(save_path, exist_ok=True)
# os.chdir(save_path)
# exist_files = os.listdir()

# for date in date_list[150:]:  # 前150天用于60日回归窗口
#     # if f"{date}.fea" in exist_files:
#     #     continue

#     # 过去60日数据
#     sub_ind = ind_ret_df.loc[:date].iloc[-60:]  # 行业超额
#     sub_sty = style_ret_df.loc[:date].iloc[-60:]  # 风格超额
#     sub_mkt = mkt_ret_s.loc[:date].iloc[-60:]  # 市场收益
#     sub_cmvg = cmvg_ret_df.loc[:date].iloc[-60:]  # 市值组超额
#     sub_ret = ret1.loc[:date].iloc[-60:].dropna(axis=1, how="all").fillna(0)

#     # 当日哑变量
#     td_citic = pd.get_dummies(citic1.loc[date])  # 行业one-hot
#     td_cmvg = pd.get_dummies(cmv_group.loc[date])  # 市值组one-hot

#     # 去市场 + z-score标准化（回归用）
#     sub_ret_adj = sub_ret.sub(sub_mkt, axis=0)  # 个股超额收益
#     sub_ind_z = sub_ind.sub(sub_ind.mean()).div(sub_ind.std() ** 2)
#     sub_cmvg_z = sub_cmvg.sub(sub_cmvg.mean()).div(sub_cmvg.std() ** 2)
#     sub_sty_z = sub_sty.sub(sub_sty.mean()).div(sub_sty.std() ** 2)
#     sub_ret_z = sub_ret_adj.sub(sub_ret_adj.mean())

#     # 回归beta（近似协方差/方差）
#     beta_ind = (sub_ret_z.T @ sub_ind_z / 60).clip(-2, 4)
#     beta_cmvg = (sub_ret_z.T @ sub_cmvg_z / 60).clip(-2, 4)
#     beta_style = (sub_ret_z.T @ sub_sty_z / 60).clip(-2, 4)

#     # 重命名列
#     td_citic.columns = "citic_r_" + td_citic.columns
#     td_cmvg.columns = "cmvg_r_" + td_cmvg.columns.astype(str)
#     beta_ind.columns = "citic_b_" + beta_ind.columns
#     beta_cmvg.columns = "cmvg_b_" + beta_cmvg.columns.astype(str)
#     beta_style.columns = "style_b_" + beta_style.columns

#     # 合并所有特征
#     codes = [c for c in sub_ret.columns if c[0] in "036"]
#     feat = pd.concat([td_citic, td_cmvg, beta_ind, beta_cmvg, beta_style], axis=1)
#     feat = feat.reindex(codes).astype(float)
#     feat = feat.fillna(feat.median())  # 中位数填补

#     # 风控标签
#     feat["ipo_dates"] = ipo_dates.loc[date].reindex(codes).fillna(0)
#     feat["st"] = st_status.loc[date].reindex(codes).fillna(1)

#     # 指数组合因子值 + 成份股权重
#     os.chdir(idx_weight_path)
#     w_data = pd.read_feather(f"{date}.fea")
#     for idx in ["zz500", "zz1000", "hs300", "A500"]:
#         sub_w = w_data[w_data["index_name"] == idx.upper()].set_index("stock_code")
#         weight = sub_w["stock_weight"].reindex(codes).fillna(0)
#         weight /= weight.sum() or 1  # 防止除0
#         feat.loc[f"idx_{idx}"] = feat.reindex(codes).T @ weight  # 指数组合因子
#         feat[f"{idx}_member"] = weight  # 个股权重

#     # 保存
#     os.chdir(save_path)
#     feat.fillna(0).to_feather(f"{date}.fea")
#     print(f"Saved {date}.fea")
#     # feat.fillna(0).to_feather(f"/home/haris/share/haris/backtester/data/trade_support5/{date}.fea")

# print("Done:", datetime.datetime.now())
