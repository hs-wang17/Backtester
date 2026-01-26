from datetime import date
import polars as pl
from config import ID_DICT, MYSQL_URI, load_id_dict
import logging

__fin_logger = logging.getLogger(__name__)


def prepare_stk_revenue() -> pl.LazyFrame:
    lf = pl.read_database_uri(
        "SELECT PARTY_ID,PUBLISH_DATE,END_DATE,REVENUE FROM fdmt_is_n_ttmp WHERE PUBLISH_DATE >= '2018-01-01' AND MERGED_FLAG=1",
        "mysql://readonly_user:readonly_user@119.253.67.3:3306/mydb",
        engine="connectorx",
    ).lazy()
    lf = lf.rename({col: col.lower() for col in lf.collect_schema().names()})
    lf = lf.group_by("party_id", "end_date", "publish_date").max().sort("end_date")

    return lf


def get_fdmt_data_from_mysql(
    table_name: str, colnames: list[str], start_date: date, end_date: date
) -> pl.LazyFrame:
    """
    Helper function to get fundamental data from fdmt tables
    """
    if start_date > end_date:
        raise ValueError("start_date must be less than or equal to end_date")

    __fin_logger.info(f"Getting {table_name} data from {start_date} to {end_date}")

    ori_df = None

    def _db_select_cols(cols: list[str]) -> list[str]:
        # `colnames` are canonical (post-normalization) names used by factor code.
        # Translate to DB columns used in SQL SELECT.
        out: list[str] = []
        for c in cols:
            if c == "sec_id":
                continue
            out.append(c.upper())
        return out

    try:
        if table_name == "mkt_div_yield":
            db_cols = ["TICKER_SYMBOL", "TRADE_DATE"]
            if colnames:
                db_cols.extend(_db_select_cols(colnames))
            else:
                db_cols.append("DIV_RATE_L12M")
            db_cols = sorted(set(db_cols))
            query = f"SELECT {', '.join(db_cols)} FROM {table_name} WHERE TRADE_DATE BETWEEN '{start_date}' AND '{end_date}'"
            ori_df = pl.read_database_uri(query, MYSQL_URI, engine="connectorx")
            lf = (
                ori_df.lazy()
                .rename({col: col.lower() for col in ori_df.columns})
                .rename({"ticker_symbol": "sec_id"})
                .filter(pl.col("sec_id").is_not_null())
                .sort(["sec_id", "trade_date"])
            )
        elif table_name == "mkt_equd":
            db_cols = ["TICKER_SYMBOL", "TRADE_DATE"]
            if colnames:
                db_cols.extend(_db_select_cols(colnames))
            else:
                db_cols.append("PE")
            db_cols = sorted(set(db_cols))
            query = f"SELECT {', '.join(db_cols)} FROM {table_name} WHERE TRADE_DATE BETWEEN '{start_date}' AND '{end_date}'"
            ori_df = pl.read_database_uri(query, MYSQL_URI, engine="connectorx")
            lf = (
                ori_df.lazy()
                .rename({col: col.lower() for col in ori_df.columns})
                .rename({"ticker_symbol": "sec_id"})
                .filter(pl.col("sec_id").is_not_null())
                .sort(["sec_id", "trade_date"])
            )
        elif table_name in [
            "con_sec_coredata",
            "con_sec_coredata_2",
            "con_sec_corederi",
            "con_sec_corederi_2",
        ]:
            db_cols = ["SEC_CODE", "REP_FORE_DATE", "FORE_YEAR"]
            if colnames:
                db_cols.extend(_db_select_cols(colnames))
            else:
                db_cols.append("*")
            if "*" in db_cols:
                select_sql = "*"
            else:
                select_sql = ", ".join(sorted(set(db_cols)))
            query = f"SELECT {select_sql} FROM {table_name} WHERE REP_FORE_DATE BETWEEN '{start_date}' AND '{end_date}'"
            ori_df = pl.read_database_uri(query, MYSQL_URI, engine="connectorx")
            lf = (
                ori_df.lazy()
                .rename({col: col.lower() for col in ori_df.columns})
                .rename({"sec_code": "sec_id"})
                .filter(pl.col("sec_id").is_not_null())
                .sort(["sec_id", "fore_year", "rep_fore_date"])
            )
        elif table_name == "equ_free_shares":
            db_cols = ["TICKER_SYMBOL", "PUBLISH_DATE"]
            if colnames:
                db_cols.extend(_db_select_cols(colnames))
            else:
                db_cols.append("*")
            if "*" in db_cols:
                select_sql = "*"
            else:
                select_sql = ", ".join(sorted(set(db_cols)))
            query = f"SELECT {select_sql} FROM {table_name} WHERE PUBLISH_DATE BETWEEN '{start_date}' AND '{end_date}'"
            ori_df = pl.read_database_uri(query, MYSQL_URI, engine="connectorx")
            lf = (
                ori_df.lazy()
                .rename({col: col.lower() for col in ori_df.columns})
                .rename({"ticker_symbol": "sec_id"})
                .filter(pl.col("sec_id").is_not_null())
                .sort(["sec_id", "publish_date"])
            )
        else:
            id_dict = ID_DICT or {}
            if not id_dict:
                # Lazy-load mapping only when actually needed, to keep imports offline-friendly.
                id_dict = load_id_dict()
            # Most fdmt tables use PARTY_ID + PUBLISH_DATE (+ END_DATE) as keys.
            db_cols = ["PARTY_ID", "PUBLISH_DATE", "END_DATE"]
            if colnames:
                db_cols.extend(_db_select_cols(colnames))
            else:
                db_cols.append("*")
            if "*" in db_cols:
                select_sql = "*"
            else:
                select_sql = ", ".join(sorted(set(db_cols)))
            query = f"SELECT {select_sql} FROM {table_name} WHERE PUBLISH_DATE BETWEEN '{start_date}' AND '{end_date}'"
            ori_df = pl.read_database_uri(query, MYSQL_URI, engine="connectorx")
            lf = (
                ori_df.lazy()
                .rename({col: col.lower() for col in ori_df.columns})
                .with_columns(
                    pl.col("party_id")
                    .replace_strict(id_dict, default=None)
                    .alias("sec_id"),
                )
                .filter(pl.col("sec_id").is_not_null())
                .sort(["sec_id", "end_date", "publish_date"])
            )
    except Exception as e:
        __fin_logger.error(f"Error getting {table_name} data: {e}")
        raise e

    return lf
