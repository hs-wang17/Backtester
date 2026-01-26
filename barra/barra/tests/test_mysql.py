import os
import polars as pl
import pytest

PATH_DAILY_DATA = r"/mnt/raid0/user_data/data_frames"
# mysql client
MYSQL_URI = "mysql://readonly_user:readonly_user@119.253.67.3:3306/mydb"

query = "SELECT TICKER_SYMBOL,TRADE_DATE,PER_CASH_DIV_TTM FROM mkt_div_yield where `TRADE_DATE` > '2025-01-01' LIMIT 1000000"


def test_mysql_connection():
    if os.getenv("BARRA_RUN_MYSQL_TESTS", "0") != "1":
        pytest.skip("mysql integration test (set BARRA_RUN_MYSQL_TESTS=1 to enable)")
    # Connect to MySQL and execute query
    df = pl.read_database_uri(
        query,
        uri=MYSQL_URI,
        engine="connectorx",  # partition_on="ID", partition_num=4
    )

    # LazyFrame does not support 'pivot' (or unstack) directly because the output schema
    # (column names) depends on the data values, which are unknown until execution.
    # We must collect() first.

    wide_df = df.pivot(
        on="TICKER_SYMBOL", index="TRADE_DATE", values="PER_CASH_DIV_TTM"
    ).rename({"TRADE_DATE": "date"})

    print("Wide DataFrame (from LazyFrame):")
    print(wide_df)

    wide_df.write_ipc(
        os.path.join(PATH_DAILY_DATA, "stk_per_cash_div_ttm.feather"),
        compression="zstd",
    )

    # print(df)
    assert wide_df.height > 0  # Ensure we got some data


if __name__ == "__main__":
    test_mysql_connection()
