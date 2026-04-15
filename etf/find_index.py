"""
ETF轮动策略指数权重查询脚本

功能说明：
1. 从多个Excel文件中读取ETF数据
2. 获取ETF对应的指数基准信息
3. 查询数据库获取指数成分股权重数据
4. 根据每日ETF持仓构建对应的股票组合
5. 根据每日股票组合构造ETF的打分
6. 根据ETF打分设置不同的权重
"""

import tushare as ts
from tqdm import tqdm
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine

# ===== 初始化设置 =====

# 初始化Tushare专业版API
ts.set_token("4288b62ce66c47333d99e85b7c4987c63038341074a47a5392dab2b8")
pro = ts.pro_api()

# 设置数据文件路径
data_path = r"/home/haris/project/etf/data"
os.chdir(data_path)
data_type = "GF"  # 目前只有广发可以正常读取


# ===== 第一部分：读取ETF数据文件 =====
def load_etf_data(data_type):
    data_dir = {"HB": "ETF轮动策略底层1-HB.xlsx", "YFD": "ETF轮动策略底层2-YFD.xlsx", "GF": "ETF轮动策略底层3-GF.xlsx"}[data_type]  # 华宝/银河/广发

    data = pd.read_excel(data_dir, index_col=0).sort_index()
    if data_type == "HB":
        tot_etfs = sorted(set([item for sublist in data.apply(lambda x: x.dropna().tolist(), axis=1) for item in sublist]))
    if data_type == "YFD":
        tot_etfs = sorted(set(data.iloc[:, 0].tolist()))
    if data_type == "GF":
        tot_etfs = sorted(set(data.code.tolist()))
    data["weighted_score"] = 0.0
    data["s1"] = 0.0
    data["s2"] = 0.0
    return data, tot_etfs


def save_etf_data(data, data_type):
    data_dir = {"HB": "ETF轮动策略底层1-HB.xlsx", "YFD": "ETF轮动策略底层2-YFD.xlsx", "GF": "ETF轮动策略底层3-GF.xlsx"}[data_type]
    base = os.path.splitext(data_dir)[0]
    new_path = f"{base}_with_score.xlsx"
    data.to_excel(new_path, index=True)


# 从Excel文件中读取ETF数据，这些文件包含ETF轮动策略的底层ETF代码
data, tot_etfs = load_etf_data(data_type=data_type)

# ===== 第二部分：处理ETF基本信息（以广发证券的ETF数据为例） =====

# 查询ETF对应标的
etf_info = pd.read_csv("etf.csv", index_col=0)

# 根据tot_etfs重新索引ETF信息，只保留我们关心的ETF
etf_info = etf_info.reindex(tot_etfs)

# 提取ETF的关键信息列
etf_bench = etf_info[["name", "management", "fund_type", "benchmark", "invest_type", "type", "market"]].copy()

# 清理基准指数名称，移除常见的后缀词汇，便于后续匹配
etf_bench["bench"] = (
    etf_bench["benchmark"]
    .str.replace("指数", "")  # 移除"指数"
    .str.replace("收益率", "")  # 移除"收益率"
    .str.replace("等权重", "等权")  # 简化"等权重"为"等权"
    .str.replace("行业", "")  # 移除"行业"
)

# 重置索引，便于后续处理
etf_bench = etf_bench.reset_index()

# ===== 第三部分：连接数据库获取指数信息 =====


# 获取指数基础数据
def tonglian_engine(user="readonly_user", passwd="readonly_user", ip="119.253.67.3", port=3306, db="mydb"):
    """
    创建数据库连接引擎

    参数：
        user: 数据库用户名
        passwd: 数据库密码
        ip: 数据库服务器IP
        port: 数据库端口
        db: 数据库名称

    返回：
        SQLAlchemy数据库引擎对象
    """
    # 构建数据库连接URL，使用mysql+pymysql驱动
    engine = create_engine(url="mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (user, passwd, ip, port, db), echo=False)  # 不输出SQL语句日志
    return engine


# 创建数据库连接引擎
engine = tonglian_engine()

# 从数据库查询所有指数的基础信息（资产类别为"idx"的证券）
index_info = pd.read_sql('select * from md_security where ASSET_CLASS="idx"', engine)

# 清理指数简称，移除常见的后缀词汇，便于与ETF基准名称匹配
index_info["SEC_SHORT_NAME"] = (
    index_info["SEC_FULL_NAME"]
    .str.replace("指数", "")  # 移除"指数"
    .str.replace("收益率", "")  # 移除"收益率"
    .str.replace("等权重", "等权")  # 简化"等权重"为"等权"
    .str.replace("行业", "")  # 移除"行业"
    .str.replace("全收益", "")  # 移除"全收益"
)

# 筛选出与ETF基准名称匹配的指数
index_selected = index_info[index_info.SEC_SHORT_NAME.isin(etf_bench["bench"].tolist())]

# ===== 第四部分：查找指数权重数据表 =====

# 所有有成分数据的指数代码列表：定义所有可能包含指数权重数据的数据库表名列表
idx_wt_tables = [
    "idx_weight",  # 主要指数权重表
    "hsi_idxm_weight",  # 恒生指数权重表
    "cni_idxm_weight",  # 中证指数权重表
    "sw_idxm_weight",  # 申万指数权重表
    "csi_idxm_wt_ashare",  # 中证指数A股权重表
]

# 查询每个权重表中包含的指数代码列表
idx_wt_list_dict = {}
for table in idx_wt_tables:
    # 从每个权重表中查询不重复的SECURITY_ID
    idx_wt_list_dict[table] = pd.read_sql(f"select distinct SECURITY_ID from {table}", engine)

# 为每个ETF基准查找对应的权重数据表
index_wt_check_list = []
for i in etf_bench["bench"]:
    # 查找与当前基准名称匹配的指数信息，筛选出6位数字的代码
    security_ids = index_selected[(index_selected.SEC_SHORT_NAME == i) & (index_selected["TICKER_SYMBOL"].apply(lambda x: len(x) == 6))]["SECURITY_ID"].tolist()
    ticker_sbs = index_selected[(index_selected.SEC_SHORT_NAME == i) & (index_selected["TICKER_SYMBOL"].apply(lambda x: len(x) == 6))]["TICKER_SYMBOL"].tolist()

    # 为每个匹配的指数查找对应的权重数据表
    for s, t in zip(security_ids, ticker_sbs):
        for table in idx_wt_tables:
            if s in idx_wt_list_dict[table]["SECURITY_ID"].tolist():
                # 找到匹配的权重表，记录信息并跳出内层循环
                index_wt_check_list.append([i, s, t, table])
                break

# 将查找结果转换为DataFrame
idx_table_ans_temp = pd.DataFrame(index_wt_check_list, columns=["bench", "security_id", "ticker_symbol", "指数权重数据表"])

# 对结果进行去重处理：每个基准名称只保留第一个匹配的记录
idx_table_ans = (
    idx_table_ans_temp.sort_values(by=["bench", "ticker_symbol"])  # 按基准名称和代码排序
    .drop_duplicates(subset=["bench"], keep="first")  # 去重，保留第一个
    .set_index("bench")  # 设置基准名称为索引
)

# 将查找结果合并回ETF基准数据中
etf_bench["index_security_id"] = idx_table_ans["security_id"].reindex(etf_bench["bench"]).reset_index(drop=True)
etf_bench["index_ticker_symbol"] = idx_table_ans["ticker_symbol"].reindex(etf_bench["bench"]).reset_index(drop=True)
etf_bench["index_table"] = idx_table_ans["指数权重数据表"].reindex(etf_bench["bench"]).reset_index(drop=True)
etf_bench = etf_bench.set_index("ts_code")

# ===== 第五部分：查询股票基础信息 =====

# 查找所有股票的 security_id 与 ticker：从数据库查询所有股票的基础信息
# 条件：资产类别为"E"（股票），交易所为上交所、深交所、北交所
stock_security_ids = pd.read_sql(f'select * from md_security where ASSET_CLASS = "E" and EXCHANGE_CD IN ("XSHG","XSHE","XBEI")', engine).set_index(
    "SECURITY_ID"
)

# ===== 第六部分：构建股票持仓组合 =====

# 接下来，根据每个月的etf列表，查找到最近的指数权重，构建股票组合
# 获取所有需要处理的日期列表（去重并排序）
date_list = sorted(set(data.index.tolist()))
scores = pd.read_csv("/home/haris/results/predictions/model_re_20251128.csv", index_col=0)
scores.index = scores.index.astype(str).str.zfill(6)

# 遍历每个交易日，构建对应的股票持仓组合
for date in tqdm(date_list, desc="处理日期进度"):
    # 获取当日的ETF代码列表
    td_etfs = data.loc[date]["code"].tolist()
    score = scores[str(date)]

    # 根据ETF代码获取对应的指数信息，删除空值行
    td_idxs = etf_bench.reindex(td_etfs).dropna(how="all")

    # 遍历每个指数，获取其成分股权重
    for td_idx in td_idxs.index:
        # 获取当前指数的SECURITY_ID
        bench_id = td_idxs.loc[td_idx]["index_security_id"]

        # 获取当前指数对应的权重数据表名
        bench_tb = td_idxs.loc[td_idx]["index_table"]

        # 查询该指数的所有有效日期（eff_date）
        month_dates = pd.read_sql(f"select distinct eff_date from {bench_tb} where security_id = '{bench_id}'", engine)["eff_date"].sort_values()

        # 找到小于等于当前日期的最近一个有效日期
        last_month = month_dates[month_dates <= datetime.strptime(str(date), "%Y%m%d").date()].iloc[-1].strftime("%Y-%m-%d")

        # 查询该指数在最近有效日期的成分股权重数据
        weight = pd.read_sql(f"select * from {bench_tb} where eff_date = '{last_month}' and security_id = '{bench_id}'", engine)

        # 将成分股代码（CONS_ID）映射为股票代码（TICKER_SYMBOL）
        weight["stk_code"] = stock_security_ids["TICKER_SYMBOL"].reindex(weight["CONS_ID"]).reset_index(drop=True)

        # 将ETF的权重和股票打分（/home/haris/temp/model_re_20251128.csv）进行内积计算，得到ETF的打分
        w = weight.set_index("stk_code")["WEIGHT"]
        valid_stks = w.index.intersection(score.index)
        if len(valid_stks) == 0:
            weighted_score = 0
        else:
            w2 = w.loc[valid_stks]
            w_norm = w2 / w2.sum()
            weighted_score = score.loc[valid_stks].fillna(0.0) @ w_norm
        data.loc[(data.index == date) & (data["code"] == td_idx), "weighted_score"] = weighted_score

    # 根据ETF的打分调整ETF权重（归一化打分，或4:3:2:1:0分配）
    df_day = data.loc[date]
    scores_day = df_day["weighted_score"].fillna(0)

    # ---------- s1：按得分归一化 ----------
    if scores_day.max() > scores_day.min():
        s1_raw = (scores_day - scores_day.min()) / (scores_day.max() - scores_day.min())
    else:
        s1_raw = scores_day * 0.0
    total = s1_raw.sum()
    if total > 0:
        s1 = s1_raw / total
    else:
        s1 = s1_raw

    # ---------- s2：按得分排序分配 ----------
    s2_raw = s1.rank() - 1.0
    total = s2_raw.sum()
    if total > 0:
        s2 = s2_raw / total
    else:
        s2 = s2_raw

    data.loc[date, "s1"] = s1.values
    data.loc[date, "s2"] = s2.values

save_etf_data(data=data, data_type=data_type)
