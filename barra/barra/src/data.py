import logging
from typing import Optional, Dict, List, Tuple, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from datetime import date
import pandas as pd
import os
import polars as pl
from config import MYSQL_URI

__global_logger = logging.getLogger(__name__)

load_dotenv()  # 从.env文件中加载环境变量
user = os.getenv("TL_USER")
passwd = os.getenv("TL_PASSWD")
ip = os.getenv("TL_IP")
port = os.getenv("TL_PORT")
db = os.getenv("TL_DB")

# 全局变量配置
level2_data_path = os.getenv("TL_LEVEL2_PATH")  # Level2原始数据路径


# -----------------------------------------------------
# read data from mysql
# -----------------------------------------------------
def tonglian_engine(
    user: Optional[str] = user,
    passwd: Optional[str] = passwd,
    ip: Optional[str] = ip,
    port: Optional[str] = port,
    db: Optional[str] = db,
) -> Engine:
    engine = create_engine(
        url="mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"
        % (user, passwd, ip, port, db),
        echo=False,
    )
    return engine


# load data from mysql
def get_trade_days(
    engine: Engine, begin_year: str = "2000", end_year: Optional[str] = None
) -> Tuple[List[Any], List[Any]]:
    """通过engine获取指定年代区间内的交易日列表和自然日列表"""
    if end_year is None:
        trade_calendar = pd.read_sql(
            f'select calendar_date, is_open from md_trade_cal where exchange_cd="XSHG" and calendar_date>="{begin_year}-01-01"',
            engine,
        )
    else:
        trade_calendar = pd.read_sql(
            f'select calendar_date, is_open from md_trade_cal where exchange_cd="XSHG" and calendar_date>="{begin_year}-01-01" and calendar_date<="{end_year}-12-31"',
            engine,
        )

    # 确保日期按时间顺序从远到近排列
    trade_calendar = trade_calendar.sort_values("calendar_date")

    natural_days = trade_calendar["calendar_date"].tolist()
    trade_days = trade_calendar.loc[
        trade_calendar["is_open"] == 1, "calendar_date"
    ].tolist()

    return trade_days, natural_days


def get_trade_days_pl(
    begin_year: str = "2000", end_year: Optional[str] = None
) -> Tuple[List[date], List[date]]:
    """通过engine获取指定年代区间内的交易日列表和自然日列表，返回polars格式"""
    if end_year is None:
        trade_calendar = pl.read_database_uri(
            f'select calendar_date, is_open from md_trade_cal where exchange_cd="XSHG" and calendar_date>="{begin_year}-01-01"',
            uri=MYSQL_URI,
            engine="connectorx",
        )
    else:
        trade_calendar = pl.read_database_uri(
            f'select calendar_date, is_open from md_trade_cal where exchange_cd="XSHG" and calendar_date>="{begin_year}-01-01" and calendar_date<="{end_year}-12-31"',
            uri=MYSQL_URI,
            engine="connectorx",
            schema_overrides={"calendar_date": pl.Date, "is_open": pl.Int8},
        )

    # 确保日期按时间顺序从远到近排列
    trade_calendar = trade_calendar.sort("calendar_date")

    natural_days = trade_calendar["calendar_date"].to_list()
    trade_days = trade_calendar.filter(pl.col("is_open") == 1)[
        "calendar_date"
    ].to_list()

    return trade_days, natural_days


def get_ohlc_data(
    engine: Engine, begin_date: str = "2010-01-01", end_date: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    通过engine获取指定日期区间内的昨收、高、开、低、收、量、额、成交笔数、换手率、市盈率TTM、市净率、
    流通市值、总市值、涨跌幅等数据
    """
    if end_date is None:
        ohlc_data = pd.read_sql(
            f'select * from mkt_equd  where TRADE_DATE>="{begin_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
            engine,
        )
    else:
        ohlc_data = pd.read_sql(
            f'select * from mkt_equd  where TRADE_DATE>="{begin_date}" and TRADE_DATE<="{end_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
            engine,
        )
    ohlc_dict = {}
    for col in ohlc_data.columns:
        if col not in [
            "ID",
            "SECURITY_ID",
            "TICKER_SYMBOL",
            "EXCHANGE_CD",
            "TRADE_DATE",
            "ACT_PRE_CLOSE_PRICE",
            "PE1",
            "UPDATE_TIME",
            "RANGE_PCT",
            "ETL_CLOSE_PRICE",
            "QA_ACTIVE_FLG",
            "CREATE_TIME",
        ]:
            ohlc_dict[col] = (
                ohlc_data.pivot(index="TRADE_DATE", columns="TICKER_SYMBOL", values=col)
                .sort_index()
                .sort_index(axis=1)
            )
    return ohlc_dict


def get_ohlc_data_adj(
    engine: Engine, begin_date: str = "2010-01-01", end_date: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    获取指定日期区间内的后复权的昨收、高、开、低、收、量等数据
    """
    if end_date is None:
        ohlc_data = pd.read_sql(
            f'select TICKER_SYMBOL,TRADE_DATE,PRE_CLOSE_PRICE_2,OPEN_PRICE_2,HIGHEST_PRICE_2,\
        LOWEST_PRICE_2,CLOSE_PRICE_2,TURNOVER_VOL,ACCUM_ADJ_FACTOR_2 from mkt_equd_adj_af  where TRADE_DATE>="{begin_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
            engine,
        )
    else:
        ohlc_data = pd.read_sql(
            f'select TICKER_SYMBOL,TRADE_DATE,PRE_CLOSE_PRICE_2,OPEN_PRICE_2,HIGHEST_PRICE_2,\
        LOWEST_PRICE_2,CLOSE_PRICE_2,TURNOVER_VOL,ACCUM_ADJ_FACTOR_2 from mkt_equd_adj_af  where TRADE_DATE>="{begin_date}" and TRADE_DATE<="{end_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
            engine,
        )
    ohlc_dict = {}
    for col in ohlc_data.columns:
        if col not in ["TICKER_SYMBOL", "TRADE_DATE"]:
            ohlc_dict[col] = (
                ohlc_data.pivot(index="TRADE_DATE", columns="TICKER_SYMBOL", values=col)
                .sort_index()
                .sort_index(axis=1)
            )
    return ohlc_dict


def get_ipo_info(engine: Engine) -> pd.DataFrame:
    stk_info = pd.read_sql(
        'select TICKER_SYMBOL,SEC_SHORT_NAME,EXCHANGE_CD,INTO_DATE,OUT_DATE from md_sec_type where TYPE_NAME="全部A股" order by TICKER_SYMBOL',
        engine,
    )
    return stk_info


def get_trade_status(
    engine: Engine, begin_date: str = "2010-01-01", end_date: Optional[str] = None
) -> pd.DataFrame:
    """获取指定日期区间内的股票交易状态"""
    if end_date is None:
        status_data = pd.read_sql(
            f'select TICKER_SYMBOL,TRADE_DATE,LIST_STATUS_CD from equ_retud where TRADE_DATE>="{begin_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
            engine,
        )
    else:
        status_data = pd.read_sql(
            f'select TICKER_SYMBOL,TRADE_DATE,LIST_STATUS_CD from equ_retud where TRADE_DATE>="{begin_date}" and \
        TRADE_DATE<="{end_date}" and LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
            engine,
        )
    return status_data.pivot(
        index="TRADE_DATE", columns="TICKER_SYMBOL", values="LIST_STATUS_CD"
    )


def get_st_status(engine: Engine) -> pd.DataFrame:
    """获取指定日期区间内的股票 ST 状态"""
    st_raw = pd.read_sql(
        'select TICKER_SYMBOL, PARTY_STATE, EFF_DATE from equ_inst_sstate where PARTY_STATE<3 and \
    LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9"',
        engine,
    )
    st_raw["PARTY_STATE"] = st_raw["PARTY_STATE"].replace(1, 0)
    st_raw["PARTY_STATE"] = st_raw["PARTY_STATE"].replace(2, 1)
    st_a = (
        st_raw.pivot(index="EFF_DATE", columns="TICKER_SYMBOL", values="PARTY_STATE")
        .ffill()
        .fillna(0)
    )
    return st_a


def get_citic_industry(
    engine: Engine, natural_days: List[Any], trade_days: List[Any]
) -> pd.DataFrame:
    """
    从engine获取指定区间内的中信一级行业名称
    """
    sql_query = """SELECT distinct
            a.PARTY_ID,/*机构内部ID*/
            b.TICKER_SYMBOL,/*股票代码*/
            b.SEC_SHORT_NAME,/*证券简称*/
            c.TYPE_NAME,/*行业分类*/
            a.INTO_DATE,/* 纳入日期*/
            a.OUT_DATE,/*剔除日期*/
            a.IS_NEW/*是否最新*/
        FROM
                md_inst_type a
        LEFT JOIN md_security b ON a.PARTY_ID = b.PARTY_ID
        LEFT JOIN md_type c ON left(a.TYPE_ID,8) = c.TYPE_ID /*通过TYPE_ID实现行业成分与行业分类标准关联，8位为1级，10位为2级，12位为3级*/
        WHERE
                LEFT (a.TYPE_ID, 6) IN ('010317')  /*中信行业*/
        AND DY_USE_FLG = 1
        AND EXCHANGE_CD IN ('XSHE', 'XSHG') /*限定上交所、深交所*/
        AND ASSET_CLASS = 'E' /*限定上市公司*/
        and LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9" """
    industry_raw = pd.read_sql(sql_query, engine)
    industry_raw.sort_values(["TICKER_SYMBOL", "INTO_DATE"])
    industr_df = (
        industry_raw.pivot(
            index="INTO_DATE", columns="TICKER_SYMBOL", values="TYPE_NAME"
        )
        .ffill()
        .reindex(natural_days)
        .ffill()
        .reindex(trade_days)
        .dropna(how="all")
    )
    return industr_df


def get_limit_data(
    engine: Engine, begin_date: str = "2010-01-01", end_date: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    通过engine获取指定日期区间内的涨停价、跌停价等数据
    """
    if end_date is None:
        ans_data = pd.read_sql(
            f'select TICKER_SYMBOL,TRADE_DATE,LIMIT_UP_PRICE,LIMIT_DOWN_PRICE from mkt_limit  where TRADE_DATE>="{begin_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9" and LEFT(TICKER_SYMBOL,1) !="1" and LEFT(TICKER_SYMBOL,1) !="5"',
            engine,
        )
    else:
        ans_data = pd.read_sql(
            f'select TICKER_SYMBOL,TRADE_DATE,LIMIT_UP_PRICE,LIMIT_DOWN_PRICE from mkt_limit  where TRADE_DATE>="{begin_date}" and TRADE_DATE<="{end_date}" and \
        LEFT(TICKER_SYMBOL,1) !="2" and LEFT(TICKER_SYMBOL,1) !="9" and LEFT(TICKER_SYMBOL,1) !="1" and LEFT(TICKER_SYMBOL,1) !="5"',
            engine,
        )
    ans_dict = {}
    for col in ["LIMIT_UP_PRICE", "LIMIT_DOWN_PRICE"]:
        ans_dict[col] = (
            ans_data.pivot(index="TRADE_DATE", columns="TICKER_SYMBOL", values=col)
            .sort_index()
            .sort_index(axis=1)
        )
    return ans_dict


# 筛选出A股代码，确保在因子/训练/交易环节中不会出现北交所/B股/可转债等奇怪的代码，污染数据
def check_a_stock(stock: str) -> bool:
    res = True if stock[:2] in ["00", "30", "60", "68"] else False
    return res


# -----------------------------------------------------
# process data
# -----------------------------------------------------


# Tick数据
def get_stock_total_tick_data(date: str) -> Optional[pd.DataFrame]:
    """
    获取指定日期的Tick数据，包括价格、成交量、10档行情等
    返回处理后的DataFrame，而不是保存到文件
    """

    def get_valid_tick_data(tick_data: pd.DataFrame) -> pd.DataFrame:
        """
        由于Tick数据存在一些不需要的行情，这里进行一些剔除和过滤
        """
        invalid1 = (
            (tick_data["time"] >= 92500000)
            & (tick_data["time"] < 93000000)
            & (tick_data["volume"] == 0)
            & (tick_data["InstruStatus"] == "TRADE")
        )  # 926-930选择性剔除
        invalid2 = (
            (tick_data["time"] > 113000000)
            & (tick_data["time"] < 130000000)
            & (tick_data["volume"] == 0)
        )  # 1130-1300且volume为0全部剔除
        invalid3 = (tick_data["time"] < 91500000) | (
            tick_data["time"] > 150100000
        )  # 0915和1501之后全部剔除全部剔除
        invalid4 = ~tick_data["InstruStatus"].isin(["OCALL", "CCALL", "TRADE"])
        invalid = invalid1 | invalid2 | invalid3 | invalid4
        tick_data_valid = tick_data[~invalid]
        # 保留收盘的那一笔
        tick_data_close = tick_data.loc[
            tick_data["InstruStatus"] == "CLOSE"
        ].drop_duplicates(["code"], keep="first")
        tick_data_valid = pd.concat([tick_data_valid, tick_data_close])
        return tick_data_valid

    # 读取tonglian的level2数据
    tick_data_sh = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_4_4_0.csv", index_col=False
    )
    if len(tick_data_sh) != tick_data_sh["SeqNo"].max():
        __global_logger.warning("mdl_4_4_0数据丢失: date=%s", date)
        return None
    tick_data_sh = tick_data_sh.loc[
        tick_data_sh["SecurityID"].map(lambda x: str(x)[:2] in ["60", "68"])
    ]

    # SH需要的字段
    local_data_sh = pd.DataFrame(index=tick_data_sh.index)
    local_data_sh["date"] = date
    local_data_sh["code"] = tick_data_sh["SecurityID"].map(lambda x: str(x).zfill(6))
    local_data_sh["time"] = tick_data_sh["UpdateTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_data_sh["preclose"] = round(
        tick_data_sh["PreCloPrice"] + 1e-8, 2
    )  # 防止浮点数问题
    local_data_sh["open"] = round(tick_data_sh["OpenPrice"] + 1e-8, 2)
    local_data_sh["high"] = round(tick_data_sh["HighPrice"] + 1e-8, 2)
    local_data_sh["low"] = round(tick_data_sh["LowPrice"] + 1e-8, 2)
    local_data_sh["close"] = round(tick_data_sh["LastPrice"] + 1e-8, 2)
    local_data_sh["cjbs"] = (
        tick_data_sh.groupby("SecurityID")["TradNumber"].diff().fillna(0)
    )
    local_data_sh["volume"] = (
        tick_data_sh.groupby("SecurityID")["TradVolume"].diff().fillna(0).astype("int")
    )
    local_data_sh["amount"] = (
        tick_data_sh.groupby("SecurityID")["Turnover"].diff().fillna(0)
    )
    local_data_sh["InstruStatus"] = tick_data_sh["InstruStatus"]
    # 处理10档行情
    level_num = 10
    for level in range(1, level_num + 1):
        local_data_sh[f"bp{level}"] = round(tick_data_sh[f"BidPrice{level}"] + 1e-8, 2)
        local_data_sh[f"sp{level}"] = round(tick_data_sh[f"AskPrice{level}"] + 1e-8, 2)
        local_data_sh[f"bv{level}"] = (
            tick_data_sh[f"BidVolume{level}"].fillna(0).astype("int")
        )
        local_data_sh[f"sv{level}"] = (
            tick_data_sh[f"AskVolume{level}"].fillna(0).astype("int")
        )
    del tick_data_sh

    tick_data_sz = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_6_28_0.csv", index_col=False
    )
    if len(tick_data_sz) != tick_data_sz["SeqNo"].max():
        __global_logger.warning("mdl_6_28_0数据丢失: date=%s", date)
        return None
    tick_data_sz = tick_data_sz.loc[
        tick_data_sz["SecurityID"].map(lambda x: str(x).zfill(6)[:2] in ["00", "30"])
    ]
    # SZ需要的字段
    local_data_sz = pd.DataFrame(index=tick_data_sz.index)
    local_data_sz["date"] = date
    local_data_sz["code"] = tick_data_sz["SecurityID"].map(lambda x: str(x).zfill(6))
    local_data_sz["time"] = tick_data_sz["UpdateTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_data_sz["preclose"] = round(
        tick_data_sz["PreCloPrice"] + 1e-8, 2
    )  # 防止浮点数问题
    local_data_sz["open"] = round(tick_data_sz["OpenPrice"] + 1e-8, 2)
    local_data_sz["high"] = round(tick_data_sz["HighPrice"] + 1e-8, 2)
    local_data_sz["low"] = round(tick_data_sz["LowPrice"] + 1e-8, 2)
    local_data_sz["close"] = round(tick_data_sz["LastPrice"] + 1e-8, 2)
    local_data_sz["cjbs"] = (
        tick_data_sz.groupby("SecurityID")["TurnNum"].diff().fillna(0)
    )
    local_data_sz["volume"] = (
        tick_data_sz.groupby("SecurityID")["Volume"].diff().fillna(0).astype("int")
    )
    local_data_sz["amount"] = (
        tick_data_sz.groupby("SecurityID")["Turnover"].diff().fillna(0)
    )
    local_data_sz["InstruStatus"] = (
        tick_data_sz["TradingPhaseCode"]
        .map(str.strip)
        .map(
            {"O0": "OCALL", "B0": "TRADE", "T0": "TRADE", "C0": "CCALL", "E0": "CLOSE"}
        )
    )
    # 处理10档行情
    level_num = 10
    for level in range(1, level_num + 1):
        local_data_sz[f"bp{level}"] = round(tick_data_sz[f"BidPrice{level}"] + 1e-8, 2)
        local_data_sz[f"sp{level}"] = round(tick_data_sz[f"AskPrice{level}"] + 1e-8, 2)
        local_data_sz[f"bv{level}"] = (
            tick_data_sz[f"BidVolume{level}"].fillna(0).astype("int")
        )
        local_data_sz[f"sv{level}"] = (
            tick_data_sz[f"AskVolume{level}"].fillna(0).astype("int")
        )
    del tick_data_sz

    # 将沪深股票拼接在一起
    local_data = pd.concat([local_data_sz, local_data_sh])

    # 再执行一下行情过滤
    local_data = get_valid_tick_data(local_data)
    local_data = local_data.sort_values(["code", "time"])

    # 返回处理后的数据，而不是保存到文件
    return local_data.reset_index(drop=True)


# Order数据
def get_stock_total_order_data(date: str) -> Optional[pd.DataFrame]:
    """
    获取指定日期的订单数据，包括委托和撤单
    返回处理后的DataFrame，而不是保存到文件
    """
    trans_data_sh = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_4_24_0.csv", index_col=False
    )
    if len(trans_data_sh) != trans_data_sh["SeqNo"].max():
        __global_logger.warning("mdl_4_24_0数据丢失: date=%s", date)
        return None
    trans_data_sh = trans_data_sh.loc[
        trans_data_sh["SecurityID"].map(lambda x: str(x)[:2] in ["60", "68"])
    ]
    trans_data_sh["orderId"] = trans_data_sh[["BuyOrderNO", "SellOrderNO"]].max(axis=1)
    cancel_data_sh = trans_data_sh.loc[trans_data_sh["Type"].isin(["D"])]
    order_data_sh = trans_data_sh.loc[trans_data_sh["Type"].isin(["A", "T"])]
    order_data_sh = order_data_sh[
        ~(
            (order_data_sh["Type"] == "T")
            & (
                (order_data_sh["TickTime"] < "09:29:00.000")
                | (order_data_sh["TickTime"] > "14:59:00.000")
            )
        )
    ]

    order_data_sh["Pricemax"] = order_data_sh["Price"]
    order_data_sh["Pricemin"] = order_data_sh["Price"]
    order_data_sh = (
        order_data_sh.groupby(["SecurityID", "orderId"])
        .agg(
            {
                "TickTime": "first",
                "TickBSFlag": "first",
                "Pricemax": "max",
                "Pricemin": "min",
                "BizIndex": "min",
                "Qty": "sum",
            }
        )
        .reset_index()
    )
    # 订单价格如果是买单，就取价格最大，卖单就取价格最小
    order_data_sh["Price"] = (order_data_sh["TickBSFlag"] == "B") * order_data_sh[
        "Pricemax"
    ] + (order_data_sh["TickBSFlag"] == "S") * order_data_sh["Pricemin"]

    # SH需要的字段
    local_data_sh = pd.DataFrame(index=order_data_sh.index)
    local_data_sh["date"] = date
    local_data_sh["code"] = order_data_sh["SecurityID"].map(lambda x: str(x).zfill(6))
    local_data_sh["time"] = order_data_sh["TickTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_data_sh["orderId"] = order_data_sh["BizIndex"].astype("int")
    local_data_sh["orderType"] = "A"
    local_data_sh["orderSide"] = order_data_sh["TickBSFlag"]
    local_data_sh["orderPrice"] = round(order_data_sh["Price"] + 1e-8, 2)
    local_data_sh["orderVolume"] = order_data_sh["Qty"].astype("int")
    local_data_sh["orderIdOrigin"] = order_data_sh["orderId"].astype("int")

    local_cancel_sh = pd.DataFrame(index=cancel_data_sh.index)
    local_cancel_sh["date"] = date
    local_cancel_sh["code"] = cancel_data_sh["SecurityID"].map(
        lambda x: str(x).zfill(6)
    )
    local_cancel_sh["time"] = cancel_data_sh["TickTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_cancel_sh["orderId"] = cancel_data_sh["BizIndex"].astype("int")
    local_cancel_sh["orderType"] = "D"
    local_cancel_sh["orderSide"] = cancel_data_sh["TickBSFlag"]
    local_cancel_sh["orderPrice"] = round(cancel_data_sh["Price"] + 1e-8, 2)
    local_cancel_sh["orderVolume"] = -cancel_data_sh["Qty"].astype("int")
    local_cancel_sh["orderIdOrigin"] = cancel_data_sh["orderId"].astype("int")

    local_data_sh = pd.concat([local_data_sh, local_cancel_sh])
    local_data_sh["orderAmount"] = round(
        local_data_sh["orderVolume"] * local_data_sh["orderPrice"] + 1e-8, 2
    )
    del trans_data_sh, order_data_sh, cancel_data_sh

    trans_data_sz = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_6_36_0.csv", index_col=False
    )
    if len(trans_data_sz) != trans_data_sz["SeqNo"].max():
        __global_logger.warning("mdl_6_36_0数据丢失: date=%s", date)
        return None
    trans_data_sz = trans_data_sz.loc[
        trans_data_sz["SecurityID"].map(lambda x: str(x).zfill(6)[:2] in ["00", "30"])
    ]
    cancel_data_sz = trans_data_sz.loc[
        trans_data_sz["ExecType"] == 52
    ]  # sz数据在这里只用到了成交数据
    order_data_sz = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_6_33_0.csv", index_col=False
    )
    if len(order_data_sz) != order_data_sz["SeqNo"].max():
        __global_logger.warning("mdl_6_33_0数据丢失: date=%s", date)
        return None
    order_data_sz = order_data_sz.loc[
        order_data_sz["SecurityID"].map(lambda x: str(x).zfill(6)[:2] in ["00", "30"])
    ]
    # SZ需要的字段
    local_data_sz = pd.DataFrame(index=order_data_sz.index)
    local_data_sz["date"] = date
    local_data_sz["code"] = order_data_sz["SecurityID"].map(lambda x: str(x).zfill(6))
    local_data_sz["time"] = order_data_sz["TransactTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_data_sz["orderId"] = order_data_sz["ApplSeqNum"].astype("int")
    local_data_sz["orderType"] = order_data_sz["OrdType"].map(
        {49: "0", 50: "1", 85: "2"}
    )
    local_data_sz["orderSide"] = order_data_sz["Side"].map({49: "B", 50: "S"})
    local_data_sz["orderPrice"] = round(order_data_sz["Price"] + 1e-8, 2)
    local_data_sz["orderVolume"] = order_data_sz["OrderQty"].astype("int")
    local_data_sz["orderIdOrigin"] = local_data_sz["orderId"].astype("int")

    local_cancel_sz = pd.DataFrame(index=cancel_data_sz.index)
    local_cancel_sz["date"] = date
    local_cancel_sz["code"] = cancel_data_sz["SecurityID"].map(
        lambda x: str(x).zfill(6)
    )
    local_cancel_sz["time"] = cancel_data_sz["TransactTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_cancel_sz["orderId"] = cancel_data_sz["ApplSeqNum"].astype("int")
    local_cancel_sz["orderType"] = "C"
    local_cancel_sz["orderVolume"] = -cancel_data_sz["LastQty"].astype("int")
    local_cancel_sz["orderIdOrigin"] = (
        cancel_data_sz["BidApplSeqNum"] + cancel_data_sz["OfferApplSeqNum"]
    ).astype("int")
    local_cancel_sz = local_cancel_sz.merge(
        local_data_sz[["orderIdOrigin", "code", "orderPrice", "orderSide"]],
        on=["code", "orderIdOrigin"],
        how="left",
    )
    local_cancel_sz = local_cancel_sz[
        local_cancel_sz["orderSide"].notna()
    ]  # 只保留能找到原始委托的撤单委托

    local_data_sz = pd.concat([local_data_sz, local_cancel_sz])
    local_data_sz["orderAmount"] = round(
        local_data_sz["orderVolume"] * local_data_sz["orderPrice"] + 1e-8, 2
    )
    del trans_data_sz, order_data_sz, cancel_data_sz
    local_data = pd.concat([local_data_sh, local_data_sz])

    local_data = local_data.sort_values(["code", "time", "orderId"])

    # 返回处理后的数据，而不是保存到文件
    return local_data.reset_index(drop=True)


# Trans数据
def get_stock_total_trans_data(date: str) -> Optional[pd.DataFrame]:
    """
    获取指定日期的成交数据
    返回处理后的DataFrame，而不是保存到文件
    """
    # 读取tonglian的level2数据
    trans_data_sh = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_4_24_0.csv", index_col=False
    )
    if len(trans_data_sh) != trans_data_sh["SeqNo"].max():
        __global_logger.warning("mdl_4_24_0数据丢失: date=%s", date)
        return None
    trans_data_sh = trans_data_sh.loc[
        trans_data_sh["SecurityID"].map(lambda x: str(x)[:2] in ["60", "68"])
    ]
    trans_data_sh = trans_data_sh.loc[
        trans_data_sh["Type"] == "T"
    ]  # sh数据在这里只用到了成交数据
    # SH需要的字段
    local_data_sh = pd.DataFrame(index=trans_data_sh.index)
    local_data_sh["date"] = date
    local_data_sh["code"] = trans_data_sh["SecurityID"].map(lambda x: str(x).zfill(6))
    local_data_sh["time"] = trans_data_sh["TickTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_data_sh["transPrice"] = round(trans_data_sh["Price"] + 1e-8, 2)
    local_data_sh["transVolume"] = trans_data_sh["Qty"].astype("int")
    local_data_sh["transAmount"] = round(
        local_data_sh["transVolume"] * local_data_sh["transPrice"] + 1e-8, 2
    )
    local_data_sh["askId"] = trans_data_sh["SellOrderNO"].astype("int")
    local_data_sh["bidId"] = trans_data_sh["BuyOrderNO"].astype("int")
    local_data_sh["transSide"] = (local_data_sh["bidId"] > local_data_sh["askId"]).map(
        {True: "B", False: "S"}
    )
    local_data_sh["transId"] = trans_data_sh["BizIndex"].astype("int")
    del trans_data_sh

    trans_data_sz = pd.read_csv(
        rf"{level2_data_path}/{date}/mdl_6_36_0.csv", index_col=False
    )
    if len(trans_data_sz) != trans_data_sz["SeqNo"].max():
        __global_logger.warning("mdl_6_36_0数据丢失: date=%s", date)
        return None
    trans_data_sz = trans_data_sz.loc[
        trans_data_sz["SecurityID"].map(lambda x: str(x).zfill(6)[:2] in ["00", "30"])
    ]
    trans_data_sz = trans_data_sz.loc[
        trans_data_sz["ExecType"] == 70
    ]  # sz数据在这里只用到了成交数据

    # SZ需要的字段
    local_data_sz = pd.DataFrame(index=trans_data_sz.index)
    local_data_sz["date"] = date
    local_data_sz["code"] = trans_data_sz["SecurityID"].map(lambda x: str(x).zfill(6))
    local_data_sz["time"] = trans_data_sz["TransactTime"].map(
        lambda x: int(x.replace(":", "").replace(".", ""))
    )
    local_data_sz["transPrice"] = round(trans_data_sz["LastPx"] + 1e-8, 2)
    local_data_sz["transVolume"] = trans_data_sz["LastQty"].astype("int")
    local_data_sz["transAmount"] = round(
        local_data_sz["transVolume"] * local_data_sz["transPrice"] + 1e-8, 2
    )
    local_data_sz["askId"] = trans_data_sz["OfferApplSeqNum"].astype("int")
    local_data_sz["bidId"] = trans_data_sz["BidApplSeqNum"].astype("int")
    local_data_sz["transSide"] = (local_data_sz["bidId"] > local_data_sz["askId"]).map(
        {True: "B", False: "S"}
    )
    local_data_sz["transId"] = trans_data_sz["ApplSeqNum"].astype("int")
    del trans_data_sz
    # 将沪深股票拼接在一起
    local_data = pd.concat([local_data_sz, local_data_sh])

    # 再执行一下行情过滤
    local_data = local_data.sort_values(["code", "time", "transId"])

    # 返回处理后的数据，而不是保存到文件
    return local_data.reset_index(drop=True)
