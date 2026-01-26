from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import polars as pl
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from config import FEATHER_INDEX_NAME, PATH_DAILY_DATA  # noqa: E402
from data import get_trade_days_pl  # noqa: E402
from main import compute_barra_factors_from_base, process_single_day  # noqa: E402


def _parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def _ymd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def _scan_daily_table(table: str) -> pl.LazyFrame:
    lf = pl.scan_ipc(os.path.join(PATH_DAILY_DATA, f"{table}.feather"), memory_map=True)
    schema = lf.collect_schema()
    if FEATHER_INDEX_NAME in schema.names():
        lf = lf.rename({FEATHER_INDEX_NAME: "date"})
    return lf.with_columns(pl.col("date").str.to_date("%Y%m%d"))


def _load_daily_panels() -> dict[str, pl.LazyFrame]:
    feather_files = [
        f
        for f in os.listdir(PATH_DAILY_DATA)
        if f.endswith(".feather") and (f.startswith("stk_") or f.startswith("idx_"))
    ]

    df_dict: dict[str, pl.LazyFrame] = {}
    for f in feather_files:
        df_name = f.split(".")[0]
        lf = pl.scan_ipc(os.path.join(PATH_DAILY_DATA, f), memory_map=True)
        schema = lf.collect_schema()
        if FEATHER_INDEX_NAME in schema.names():
            lf = lf.rename({FEATHER_INDEX_NAME: "date"})
        df_dict[df_name] = lf.with_columns(pl.col("date").str.to_date("%Y%m%d"))

    if "stk_adjclose" in df_dict and "idx_close" in df_dict:
        df_dict["stk_ret"] = (
            df_dict["stk_adjclose"]
            .with_columns(pl.all().exclude("date").pct_change())
            .slice(1)
        )
        df_dict["idx_ret"] = (
            df_dict["idx_close"]
            .with_columns(pl.all().exclude("date").pct_change())
            .slice(1)
        )
    return df_dict


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Integration script: compute base factors and then barra factors for a "
            "date range (typically one month). Requires local daily data mounts; "
            "some factors may require MySQL/cache."
        )
    )
    parser.add_argument("--start", required=True, help="YYYYMMDD")
    parser.add_argument("--end", required=True, help="YYYYMMDD")
    parser.add_argument("--out-base", default="tmp_one_month/base")
    parser.add_argument("--out-barra", default="tmp_one_month/barra")
    parser.add_argument("--industry-table", default="stk_citic1_code")
    parser.add_argument("--mv-table", default="stk_neg_market_value")
    parser.add_argument("--winsor-p", type=float, default=0.01)
    parser.add_argument("--no-orthogonalize", action="store_true")
    parser.add_argument("--no-corr", action="store_true")
    parser.add_argument("--no-check", action="store_true")
    args = parser.parse_args()

    start = _parse_yyyymmdd(args.start)
    end = _parse_yyyymmdd(args.end)
    if start > end:
        raise SystemExit("--start must be <= --end")

    trd_days, _ = get_trade_days_pl(begin_year=str(start.year), end_year=str(end.year))
    trd_days = [d for d in trd_days if start <= d <= end]
    trd_days.sort()
    if not trd_days:
        raise SystemExit("No trading days in range")

    os.makedirs(args.out_base, exist_ok=True)
    os.makedirs(args.out_barra, exist_ok=True)

    df_dict = _load_daily_panels()
    industry_lf = _scan_daily_table(args.industry_table)
    mv_lf = _scan_daily_table(args.mv_table)

    for trd_day in tqdm(trd_days, desc="One-month factors"):
        base_lf = process_single_day(trd_day, df_dict)
        base_df = base_lf.collect(engine="streaming")
        base_path = os.path.join(args.out_base, f"{_ymd(trd_day)}.feather")
        if base_df.height > 0:
            base_df.write_ipc(base_path, compression="zstd")

        barra_df = compute_barra_factors_from_base(
            trd_day,
            base_df,
            industry_lf=industry_lf,
            mv_lf=mv_lf,
            industry_value_name=args.industry_table,
            mv_value_name=args.mv_table,
            winsor_p=args.winsor_p,
            do_orthogonalize=not args.no_orthogonalize,
            print_corr=not args.no_corr,
            do_check=not args.no_check,
        )
        barra_path = os.path.join(args.out_barra, f"{_ymd(trd_day)}.feather")
        if barra_df.height > 0:
            barra_df.write_ipc(barra_path, compression="zstd")


if __name__ == "__main__":
    main()
