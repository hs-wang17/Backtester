from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime

_cpu_cnt = os.cpu_count() or 8
# Avoid nested parallelism (Polars/NumExpr/BLAS) exploding CPU+memory when we also parallelize by day.
os.environ.setdefault("NUMEXPR_MAX_THREADS", str(min(128, _cpu_cnt)))
os.environ.setdefault("NUMEXPR_NUM_THREADS", str(min(128, _cpu_cnt)))
os.environ.setdefault("OMP_NUM_THREADS", str(min(128, _cpu_cnt)))
os.environ.setdefault("MKL_NUM_THREADS", str(min(128, _cpu_cnt)))
os.environ.setdefault("POLARS_MAX_THREADS", str(min(128, _cpu_cnt)))

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from time import perf_counter
from typing import Callable

import numpy as np
import polars as pl
from tqdm import tqdm

from config import (
    FEATHER_INDEX_NAME,
    FIN_TABLE_REQUIRED_COLS,
    PATH_DAILY_DATA,
    PATH_FAC_ROOT,
    PATH_FD_DATA,
    TABLES,
)
from .data import get_trade_days_pl
from .prepare_fin_data import get_fdmt_data_from_mysql

pl.Config.set_engine_affinity(engine="streaming")
pl.Config.set_verbose(False)
pl.Config.set_tbl_width_chars(100)  # 限制表格宽度


# Set log file path to project root
log_file_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "polars_errors.log"
)

_log_level = getattr(
    logging, os.getenv("BARRA_LOG_LEVEL", "INFO").upper(), logging.INFO
)

# Log to both file and stdout so progress/errors are visible in terminal runs.
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode="w", encoding="utf-8"),
        # logging.StreamHandler(),
    ],
    force=True,
)
__global_logger = logging.getLogger(__name__)

# 优化：添加内存监控和垃圾回收
import gc

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None


def log_memory_usage(stage: str):
    """记录内存使用情况"""
    if psutil is None:
        return
    process = psutil.Process()
    memory_info = process.memory_info()
    __global_logger.info(
        f"{stage} - Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB"
    )


def _prev_month_bounds(d: date) -> tuple[date, date]:
    """Return (start, end) of the previous calendar month for date d."""
    first_of_month = date(d.year, d.month, 1)
    prev_month_end = first_of_month - timedelta(days=1)
    prev_month_start = date(prev_month_end.year, prev_month_end.month, 1)
    return prev_month_start, prev_month_end


def _wls_alpha_beta_single_regressor(
    y: np.ndarray, x: np.ndarray, w: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """
    Closed-form WLS for y ~ alpha + beta * x (single regressor), vectorized over columns.

    - y: (T, N) with possible NaNs
    - x: (T,)
    - w: (T,) non-negative weights
    Returns (alpha, beta) each shape (N,).
    """
    if y.ndim != 2:
        raise ValueError("y must be 2D (T, N)")
    if x.ndim != 1 or w.ndim != 1:
        raise ValueError("x and w must be 1D (T,)")
    if y.shape[0] != x.shape[0] or x.shape[0] != w.shape[0]:
        raise ValueError("y, x, w must share the same T dimension")

    mask = np.isfinite(y)
    y0 = np.where(mask, y, 0.0)

    w2 = w[:, None]
    w_mask = w2 * mask
    wsum = w_mask.sum(axis=0)  # (N,)

    x2 = x[:, None]
    xsum = (w_mask * x2).sum(axis=0)
    ysum = (w2 * y0).sum(axis=0)

    with np.errstate(divide="ignore", invalid="ignore"):
        xbar = xsum / wsum
        ybar = ysum / wsum

        dx = x2 - xbar
        dy = y0 - ybar
        cov = (w_mask * dx * dy).sum(axis=0)
        var = (w_mask * dx * dx).sum(axis=0)

        beta = cov / var
        alpha = ybar - beta * xbar

    beta = np.where((wsum > 0) & np.isfinite(beta), beta, np.nan)
    alpha = np.where((wsum > 0) & np.isfinite(alpha), alpha, np.nan)
    return alpha, beta


def _wls_resid_std_single_regressor(
    y: np.ndarray, x: np.ndarray, w: np.ndarray
) -> np.ndarray:
    """Weighted residual std for y ~ alpha + beta * x, vectorized over columns."""
    alpha, beta = _wls_alpha_beta_single_regressor(y, x, w)
    mask = np.isfinite(y)
    y0 = np.where(mask, y, 0.0)
    w2 = w[:, None]
    wsum = (w2 * mask).sum(axis=0)

    x2 = x[:, None]
    resid = y0 - alpha - beta * x2
    rss = (w2 * mask * resid * resid).sum(axis=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        sigma = np.sqrt(rss / wsum)
    return np.where((wsum > 0) & np.isfinite(sigma), sigma, np.nan)


def _weighted_mean_std(y: np.ndarray, w: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Weighted mean/std for y over axis=0, ignoring NaNs per-column.

    - y: (T, N)
    - w: (T,)
    Returns (mean, std) each (N,).
    """
    if y.ndim != 2:
        raise ValueError("y must be 2D (T, N)")
    if w.ndim != 1 or w.shape[0] != y.shape[0]:
        raise ValueError("w must be 1D (T,) matching y")

    mask = np.isfinite(y)
    y0 = np.where(mask, y, 0.0)
    w2 = w[:, None]
    w_mask = w2 * mask
    wsum = w_mask.sum(axis=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        mean = (w2 * y0).sum(axis=0) / wsum
        var = (w_mask * (y0 - mean) ** 2).sum(axis=0) / wsum
        std = np.sqrt(var)
    mean = np.where((wsum > 0) & np.isfinite(mean), mean, np.nan)
    std = np.where((wsum > 0) & np.isfinite(std), std, np.nan)
    return mean, std


def _ols_alpha_beta_single_regressor_pairwise(
    y: np.ndarray, x: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """
    OLS for y ~ alpha + beta*x with pairwise-complete rows per-column.
    y: (T, N) with NaNs; x: (T,)
    Returns (alpha, beta) each (N,).
    """
    if y.ndim != 2:
        raise ValueError("y must be 2D (T, N)")
    if x.ndim != 1 or x.shape[0] != y.shape[0]:
        raise ValueError("x must be 1D (T,) matching y")

    x2 = x[:, None]
    mask = np.isfinite(y) & np.isfinite(x2)
    y0 = np.where(mask, y, 0.0)

    cnt = mask.sum(axis=0)
    xsum = (mask * x2).sum(axis=0)
    ysum = y0.sum(axis=0)

    with np.errstate(divide="ignore", invalid="ignore"):
        xbar = xsum / cnt
        ybar = ysum / cnt
        dx = x2 - xbar
        dy = y0 - ybar
        cov = (mask * dx * dy).sum(axis=0) / cnt
        var = (mask * dx * dx).sum(axis=0) / cnt
        beta = cov / var
        alpha = ybar - beta * xbar

    beta = np.where((cnt > 1) & np.isfinite(beta), beta, np.nan)
    alpha = np.where((cnt > 1) & np.isfinite(alpha), alpha, np.nan)
    return alpha, beta


def _ewm_mean_adjust(y: np.ndarray, half_life: float) -> np.ndarray:
    """
    Exponentially-weighted moving mean with adjust=True and ignore_nulls=True semantics.

    - y: (T, N) with NaNs for missing
    Returns: (T, N)
    """
    if y.ndim != 2:
        raise ValueError("y must be 2D (T, N)")
    if half_life <= 0:
        raise ValueError("half_life must be > 0")

    T, N = y.shape
    # (1 - alpha) ** half_life = 0.5
    decay = float(np.exp(np.log(0.5) / half_life))

    num = np.zeros(N, dtype=float)
    den = np.zeros(N, dtype=float)
    out = np.empty((T, N), dtype=float)

    for t in range(T):
        xt = y[t]
        m = np.isfinite(xt)
        xt0 = np.where(m, xt, 0.0)
        num = xt0 + decay * num
        den = m.astype(float) + decay * den
        out[t] = np.where(den > 0, num / den, np.nan)

    return out


# sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))


# query = "SELECT TICKER_SYMBOL,TRADE_DATE,DIV_RATE_TTM FROM mkt_div_yield where `TRADE_DATE` > '2025-01-01' LIMIT 1000000"


class BaseFactor:
    def __init__(
        self,
        name: str,
        data: dict[str, pl.LazyFrame],
        calc: Callable[[date, dict[str, pl.LazyFrame]], pl.LazyFrame],
    ):
        self.name = name
        self.data = data
        self.calc = calc

    def cal(self, trd_date: date) -> pl.LazyFrame:
        return self.calc(trd_date, self.data)


@dataclass(frozen=True, slots=True)
class BarraFactorSpec:
    name: str
    base_weights: tuple[tuple[str, float], ...]


def _get_barra_factor_specs() -> list[BarraFactorSpec]:
    return [
        BarraFactorSpec("BETA", (("HBETA", 1.0),)),
        BarraFactorSpec("BOOK-TO-PRICE", (("BTOP", 1.0),)),
        BarraFactorSpec("DIVIDEND-YIELD", (("DTOP", 0.5), ("DTOPF", 0.5))),
        BarraFactorSpec("EARNINGS-QUALITY", (("ABS", 0.5), ("ACF", 0.5))),
        BarraFactorSpec(
            "EARNINGS-VARIABILITY",
            (("VSAL", 0.3), ("VERN", 0.3), ("VFLO", 0.3), ("ETOPF_STD", 0.1)),
        ),
        BarraFactorSpec(
            "EARNINGS-YIELD",
            (("ETOPF", 0.25), ("CETOP", 0.25), ("ETOP", 0.25), ("EM", 0.25)),
        ),
        BarraFactorSpec("GROWTH", (("EGRLF", 1 / 3), ("EGRO", 1 / 3), ("SGRO", 1 / 3))),
        BarraFactorSpec(
            "INVESTMENT-QUALITY", (("AGRO", 1 / 3), ("CXGRO", 1 / 3), ("IGRO", 1 / 3))
        ),
        BarraFactorSpec(
            "LEVERAGE", (("MLEV", 1 / 3), ("DTOA", 1 / 3), ("BLEV", 1 / 3))
        ),
        BarraFactorSpec(
            "LIQUIDITY",
            (("STOM", 0.25), ("STOQ", 0.25), ("STOA", 0.25), ("ATVR", 0.25)),
        ),
        BarraFactorSpec("LONG-TERM-REVERSAL", (("LTRSTR", 0.5), ("LTHALPHA", 0.5))),
        BarraFactorSpec("MID-CAP", (("MID-CAP", 1.0),)),
        BarraFactorSpec("MOMENTUM", (("RSTR", 0.5), ("HALPHA", 0.5))),
        BarraFactorSpec("SIZE", (("SIZE", 1.0),)),
        BarraFactorSpec(
            "PROFITABILITY", (("ATO", 0.25), ("GP", 0.25), ("GPM", 0.25), ("ROA", 0.25))
        ),
        BarraFactorSpec(
            "RESIDUAL-VOLATILITY",
            (("DASTD", 1 / 3), ("CMRA", 1 / 3), ("HSIGMA", 1 / 3)),
        ),
    ]


def _ymd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def _wide_last_row_to_long(
    lf: pl.LazyFrame, trd_date: date, value_name: str
) -> pl.DataFrame:
    df = (
        lf.filter(pl.col("date") <= trd_date)
        .sort("date")
        .last()
        .select(pl.all().exclude("date"))
        .unpivot(variable_name="sec_id", value_name=value_name)
        .collect(engine="streaming")
    )
    return df


def _ensure_fin_tables_from_cache(df_dict: dict[str, pl.LazyFrame]) -> None:
    """
    Ensure MySQL-backed financial tables exist in df_dict.

    This is mainly for ad-hoc calls to `process_single_day()` (e.g. test scripts)
    where the caller didn't pre-load `TABLES`. We only scan from local cache to
    avoid unexpected network/DB access.
    """
    if os.getenv("BARRA_AUTO_LOAD_FIN_FROM_CACHE", "1") != "1":
        return

    cache_dir = Path(PATH_FD_DATA) / "barra_fd_cache"
    if not cache_dir.is_dir():
        return

    by_table: dict[str, list[Path]] = {}
    for p in cache_dir.iterdir():
        if not p.is_file() or p.suffix != ".feather":
            continue
        parts = p.stem.rsplit("_", 2)
        if len(parts) != 3:
            continue
        table, _start, _end = parts
        by_table.setdefault(table, []).append(p)

    for table in TABLES:
        if table in df_dict:
            continue
        candidates = by_table.get(table, [])
        if not candidates:
            __global_logger.warning(f"Fin cache missing for table: {table}")
            continue
        cache_file = max(candidates, key=lambda x: x.stat().st_mtime)
        try:
            df_dict[table] = pl.scan_ipc(str(cache_file), memory_map=True)
            __global_logger.info(
                f"Loaded fin table from cache: {table} -> {cache_file.name}"
            )
        except Exception as e:
            __global_logger.error(f"Load fin table cache failed ({table}): {e}")


def _winsorize_center(df: pl.DataFrame, col: str, p: float) -> pl.Series:
    # Treat NaN as missing, otherwise mean becomes NaN and centering turns the
    # whole column into all-NaN.
    s = df.get_column(col).fill_nan(None)
    if s.null_count() == s.len():
        return s
    out = s
    if p > 0:
        q_low = out.quantile(p, interpolation="nearest")
        q_high = out.quantile(1 - p, interpolation="nearest")
        if q_low is not None and q_high is not None:
            out = out.clip(q_low, q_high)
    mean = out.mean()
    if mean is None:
        return out
    return (out - mean).fill_nan(None)


def _wls_orthogonalize_resid(
    df: pl.DataFrame,
    y_col: str,
    size_col: str,
    industry_col: str,
    weight_col: str,
) -> pl.Series:
    import statsmodels.api as sm

    y = df.get_column(y_col).fill_nan(None).to_numpy()
    size = df.get_column(size_col).fill_nan(None).to_numpy()
    industry = df.get_column(industry_col).fill_nan(None).to_numpy()
    mv = df.get_column(weight_col).fill_nan(None).to_numpy()

    valid = (
        np.isfinite(y)
        & np.isfinite(size)
        & np.isfinite(industry)
        & np.isfinite(mv)
        & (mv > 0)
    )

    out = np.full(df.height, np.nan, dtype="float64")
    if valid.sum() < 10:
        return pl.Series(y_col, out).fill_nan(None)

    yv = y[valid].astype("float64", copy=False)
    sizev = size[valid].astype("float64", copy=False)
    mvv = mv[valid].astype("float64", copy=False)
    indv = industry[valid]

    uniq = np.unique(indv)
    if uniq.size <= 1:
        x = np.column_stack([np.ones_like(sizev), sizev])
    else:
        dummies = np.column_stack([(indv == u).astype("float64") for u in uniq[1:]])
        x = np.column_stack([np.ones_like(sizev), sizev, dummies])

    try:
        w = np.power(mvv, 0.5)
        res = sm.WLS(yv, x, weights=w).fit()
    except Exception:
        return pl.Series(y_col, out).fill_nan(None)

    resid = np.asarray(res.resid, dtype="float64")
    out[valid] = resid
    return pl.Series(y_col, out).fill_nan(None)


def _corr_matrix_complete_cases(df: pl.DataFrame, cols: list[str]) -> np.ndarray | None:
    if len(cols) < 2:
        return None
    sub = df.select([pl.col(c).fill_nan(None).alias(c) for c in cols]).drop_nulls()
    if sub.height < 3:
        return None
    arr = sub.to_numpy()
    if arr.ndim != 2 or arr.shape[0] < 3:
        return None
    arr = arr.astype("float64", copy=False)
    arr = arr[np.all(np.isfinite(arr), axis=1)]
    if arr.shape[0] < 3:
        return None
    with np.errstate(invalid="ignore"):
        corr = np.corrcoef(arr, rowvar=False)
    if np.all(np.isnan(corr)):
        return None
    return corr


def compute_barra_factors_from_base(
    trd_day: date,
    base_df: pl.DataFrame,
    industry_lf: pl.LazyFrame,
    mv_lf: pl.LazyFrame,
    *,
    industry_value_name: str = "stk_citic1_code",
    mv_value_name: str = "stk_neg_market_value",
    winsor_p: float = 0.01,
    do_orthogonalize: bool = True,
    print_corr: bool = True,
    do_check: bool = True,
) -> pl.DataFrame:
    specs = _get_barra_factor_specs()
    needed_base = sorted({b for s in specs for (b, _w) in s.base_weights})

    if "sec_id" not in base_df.columns:
        raise ValueError("base_df must contain 'sec_id'")

    missing_cols = [c for c in needed_base if c not in base_df.columns]
    if missing_cols:
        __global_logger.warning(f"{trd_day}: missing base columns: {missing_cols}")

    # Exposures for orthogonalization.
    industry_df = _wide_last_row_to_long(
        industry_lf, trd_day, industry_value_name
    ).with_columns(pl.col(industry_value_name).cast(pl.UInt32, strict=False))
    mv_df = _wide_last_row_to_long(mv_lf, trd_day, mv_value_name)

    df = (
        base_df.select(["sec_id"] + [c for c in needed_base if c in base_df.columns])
        .join(industry_df, on="sec_id", how="left")
        .join(mv_df, on="sec_id", how="left")
    )

    # Winsorize + center each base factor.
    processed_cols: list[str] = []
    for c in needed_base:
        if c not in df.columns:
            continue
        df = df.with_columns(_winsorize_center(df, c, winsor_p).rename(c))
        processed_cols.append(c)

    # Orthogonalize against SIZE + industry dummies (WLS weights = market value).
    if do_orthogonalize:
        if "SIZE" not in df.columns:
            __global_logger.warning(
                f"{trd_day}: SIZE not found; skip orthogonalization"
            )
        else:
            for c in processed_cols:
                if c == "SIZE":
                    continue
                try:
                    resid = _wls_orthogonalize_resid(
                        df,
                        y_col=c,
                        size_col="SIZE",
                        industry_col=industry_value_name,
                        weight_col=mv_value_name,
                    )
                    df = df.with_columns(resid.rename(c))
                except Exception as e:
                    __global_logger.error(f"{trd_day}: orthogonalize {c} failed: {e}")

    # Compose barra factors with per-row available-weight normalization.
    barra_exprs: list[pl.Expr] = []
    for spec in specs:
        cols_weights = [(c, w) for (c, w) in spec.base_weights if c in df.columns]
        if not cols_weights:
            continue

        numerator = pl.sum_horizontal(
            [pl.col(c).fill_null(0.0) * w for (c, w) in cols_weights]
        )
        denom = pl.sum_horizontal(
            [
                pl.when(pl.col(c).is_not_null()).then(pl.lit(w)).otherwise(pl.lit(0.0))
                for (c, w) in cols_weights
            ]
        )
        barra_exprs.append(
            pl.when(denom > 0).then(numerator / denom).otherwise(None).alias(spec.name)
        )

        if print_corr and len(cols_weights) >= 2:
            corr = _corr_matrix_complete_cases(df, [c for (c, _w) in cols_weights])
            if corr is not None:
                __global_logger.info(
                    "%s %s corr_matrix (processed base):\n%s",
                    trd_day,
                    spec.name,
                    np.array2string(corr, max_line_width=200),
                )

    out = df.select(["sec_id"] + barra_exprs)

    if do_check and out.width > 1:
        total = out.height
        for name in out.columns:
            if name == "sec_id":
                continue
            s = out.get_column(name)
            non_null = total - s.null_count()
            coverage = non_null / total if total else 0.0
            stats = out.select(
                pl.col(name).min().alias("min"),
                pl.col(name).max().alias("max"),
                pl.col(name).mean().alias("mean"),
                pl.col(name).std().alias("std"),
            ).row(0)
            __global_logger.info(
                "%s %s: coverage=%.2f%%, min=%s, max=%s, mean=%s, std=%s",
                trd_day,
                name,
                coverage * 100.0,
                stats[0],
                stats[1],
                stats[2],
                stats[3],
            )

            # Corr sign sanity for composites: warn when negative correlation appears.
            spec = next((s for s in specs if s.name == name), None)
            if spec is not None and len(spec.base_weights) >= 2:
                cols = [c for (c, _w) in spec.base_weights if c in df.columns]
                corr = _corr_matrix_complete_cases(df, cols)
                if corr is not None and np.any(
                    corr[np.triu_indices_from(corr, k=1)] < 0
                ):
                    __global_logger.warning(f"{trd_day} {name}: negative corr detected")
                    if not print_corr:
                        __global_logger.info(
                            "%s %s corr_matrix (processed base):\n%s",
                            trd_day,
                            name,
                            np.array2string(corr, max_line_width=200),
                        )

    return out


# Barra Factor composed of BaseFactors
class BarraFactor:
    def __init__(
        self,
        name: str,
        base_factors: list[tuple[BaseFactor, float]],
    ):
        self.name = name
        self.base_factors = base_factors

    def cal(self, trd_date: date) -> pl.LazyFrame:
        if not self.base_factors:
            return pl.LazyFrame()

        # Collect all base factors with their weights
        base_lfs_with_weights = []
        for base_factor, weight in self.base_factors:
            lf = base_factor.cal(trd_date)
            # Ensure all factors have sec_id column
            schema = lf.collect_schema()
            if "sec_id" not in schema.names():
                # Find the ID column (non-factor column) and rename to sec_id
                id_col = next(
                    (col for col in schema.names() if col != base_factor.name), None
                )
                if id_col:
                    lf = lf.rename({id_col: "sec_id"})
                else:
                    raise ValueError(f"Factor {base_factor.name} missing ID column")
            base_lfs_with_weights.append((lf, base_factor, weight))

        # Start with the first factor
        combined, first_factor, _ = base_lfs_with_weights[0]

        # Join all remaining factors on sec_id
        for lf, base_factor, _ in base_lfs_with_weights[1:]:
            combined = combined.join(lf, on="sec_id", how="full", coalesce=True)

        # Calculate weighted sum with null handling
        weighted_sum_expr = pl.sum_horizontal(
            [
                pl.col(base_factor.name).fill_null(0) * weight
                for base_factor, weight in self.base_factors
            ]
        )
        return combined.select("sec_id", weighted_sum_expr.alias(self.name))


def cal_hbeta(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Beta

    HBETA beta
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        stk_ret_arr = data.get("_stk_ret_arr")
        idx_ret_arr = data.get("_idx_ret_arr")
        stock_cols = data.get("_stk_cols")
        if (
            isinstance(stk_ret_arr, np.ndarray)
            and isinstance(idx_ret_arr, np.ndarray)
            and isinstance(stock_cols, list)
            and stk_ret_arr.shape[0] == idx_ret_arr.shape[0]
            and stk_ret_arr.ndim == 2
        ):
            y = stk_ret_arr[-252:, :]
            x = idx_ret_arr[-252:]
            _alpha, beta = _ols_alpha_beta_single_regressor_pairwise(y, x)
            return pl.DataFrame({"sec_id": stock_cols, "HBETA": beta}).lazy()

    # Calculate index returns
    # 1. Parse date, 2. Sort by date, 3. Calculate pct_change
    idx_ret = (
        data["idx_ret"].select(["date", "国证A指"]).rename({"国证A指": "idx_return"})
    )

    # Calculate stock returns
    # 1. Parse date, 2. Sort by date, 3. Calculate pct_change for all stocks
    stk_ret = data["stk_ret"]

    # Merge index returns into the wide stock returns table
    merged = (
        stk_ret.join(idx_ret, on="date", how="inner")
        .filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(252)
    )

    # Calculate Beta for each stock column: Cov(stk_col, idx_return) / Var(idx_return)
    beta_wide = merged.select(
        pl.cov(pl.all().exclude("date", "idx_return"), pl.col("idx_return"))
        / pl.var("idx_return")
    )

    # Transpose to get stock_id and HBETA columns
    # Since beta_wide is a 1-row DataFrame, we can unpivot it to get the desired format
    beta_df = beta_wide.unpivot(variable_name="sec_id", value_name="HBETA")

    return beta_df.lazy()


def cal_btop(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    # Implement the calculation of BTOP using the data provided
    btop_df = (
        data["stk_PB"]
        .filter(pl.col("date") <= trd_date)
        .with_columns(pl.all().exclude("date").fill_nan(None).forward_fill())
        .sort("date")
        .last()
        .select(pl.all().exclude("date"))
        .unpivot(variable_name="sec_id", value_name="BTOP")
    )
    return btop_df


def cal_dtop(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    # Critical perf fix: filter early to avoid sorting/ffill over full history.
    # Semantics preserved by ffill over all rows up to previous-month end,
    # then selecting last observation within the previous month.
    pm_start, pm_end = _prev_month_bounds(trd_date)
    dtop_df = (
        data["mkt_div_yield"]
        .select(["sec_id", "trade_date", "div_rate_l12m"])
        .with_columns(pl.col("trade_date").cast(pl.Date))
        .filter(pl.col("trade_date") <= pm_end)
        .sort(["sec_id", "trade_date"])
        .with_columns(
            pl.col("div_rate_l12m").fill_nan(None).forward_fill().over("sec_id")
        )
        .filter(pl.col("trade_date") >= pm_start)
        .group_by("sec_id")
        .agg(pl.col("div_rate_l12m").last().alias("DTOP"))
        .select(["sec_id", "DTOP"])
    )
    return dtop_df


def cal_dtopf(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    # Filter first to reduce sort footprint.
    dtopf_df = (
        data["con_sec_corederi_2"]
        .select(["sec_id", "rep_fore_date", "fore_year", "con_dyr"])
        .with_columns(pl.col("rep_fore_date").cast(pl.Date))
        .filter(
            (pl.col("rep_fore_date") <= trd_date)
            & (pl.col("fore_year") == trd_date.year)
        )
        .sort(["sec_id", "rep_fore_date"])
        .with_columns(pl.col("con_dyr").fill_nan(None).forward_fill().over("sec_id"))
        .group_by("sec_id")
        .agg(pl.col("con_dyr").last().alias("DTOPF"))
        .select(["sec_id", "DTOPF"])
    )
    return dtopf_df


def cal_abs(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """资产负债表应计项目

    INT_DEBT 带息债务
    """
    td = (
        data["fdmt_main_data_pit"]
        .select(["sec_id", "publish_date", "end_date", "int_debt", "da"])
        .with_columns(
            pl.col("publish_date").cast(pl.Date),
            pl.col("end_date").cast(pl.Date),
        )
        .filter(pl.col("publish_date") <= trd_date)
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(
            pl.col("int_debt").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("da").fill_nan(None).forward_fill().over("sec_id"),
        )
        .group_by("sec_id")
        .agg(
            [
                pl.col("int_debt").last(),
                pl.col("int_debt")
                .filter(
                    (
                        pl.col("end_date").dt.year()
                        <= (pl.col("end_date").max().dt.year() - 1)
                    )
                    & (
                        pl.col("end_date").dt.month()
                        <= (pl.col("end_date").max().dt.month())
                    )
                )
                .last()
                .alias("int_debt_prev"),
                pl.col("da").last(),
            ]
        )
    )

    bs = (
        data["fdmt_bs_n_qa_pit"]
        .select(
            [
                "sec_id",
                "publish_date",
                "end_date",
                "t_assets",
                "t_liab",
                "cash_c_equiv",
            ]
        )
        .filter(
            pl.col("publish_date") <= trd_date,
            pl.col("end_date").dt.year() >= trd_date.year - 2,
        )
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(
            pl.col("t_assets").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("t_liab").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("cash_c_equiv").fill_nan(None).forward_fill().over("sec_id"),
        )
        .group_by("sec_id")
        .agg(
            [
                pl.col("t_assets").last(),
                pl.col("t_liab").last(),
                pl.col("cash_c_equiv").last(),
                pl.col("t_assets")
                .filter(
                    (
                        pl.col("end_date").dt.year()
                        <= (pl.col("end_date").max().dt.year() - 1)
                    )
                    & (
                        pl.col("end_date").dt.month()
                        <= (pl.col("end_date").max().dt.month())
                    )
                )
                .last()
                .alias("t_assets_prev"),
                pl.col("t_liab")
                .filter(
                    (
                        pl.col("end_date").dt.year()
                        <= (pl.col("end_date").max().dt.year() - 1)
                    )
                    & (
                        pl.col("end_date").dt.month()
                        <= (pl.col("end_date").max().dt.month())
                    )
                )
                .last()
                .alias("t_liab_prev"),
                pl.col("cash_c_equiv")
                .filter(
                    (
                        pl.col("end_date").dt.year()
                        <= (pl.col("end_date").max().dt.year() - 1)
                    )
                    & (
                        pl.col("end_date").dt.month()
                        <= (pl.col("end_date").max().dt.month())
                    )
                )
                .last()
                .alias("cash_c_equiv_prev"),
            ]
        )
    )

    abs_df = (
        bs.join(td, on="sec_id", how="inner")
        .with_columns(
            (
                (pl.col("t_assets") - pl.col("cash_c_equiv"))
                - (pl.col("t_liab") - pl.col("int_debt"))
            ).alias("noa"),
            (
                (pl.col("t_assets_prev") - pl.col("cash_c_equiv_prev"))
                - (pl.col("t_liab_prev") - pl.col("int_debt_prev"))
            ).alias("noa_prev"),
        )
        .with_columns(
            (pl.col("noa") - pl.col("noa_prev") - pl.col("da")).alias("accr_bs")
        )
        .select(
            pl.col("sec_id"),
            pl.when(pl.col("t_assets") != 0)
            .then(-pl.col("accr_bs") / pl.col("t_assets"))
            .otherwise(None)
            .alias("ABS"),
        )
    )

    return abs_df


def cal_acf(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    应计项目
    应计项目 = 当期净利润 - (经营活动现金流量 + 投资活动现金流量) + 折旧和摊销之和
    """
    _is = (
        data["fdmt_is_n_ttmp"]
        .select(["sec_id", "publish_date", "end_date", "n_income"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("n_income").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort("sec_id", "end_date", "publish_date")
        .unique("sec_id", keep="last")
        .select(["sec_id", "n_income"])
    )
    _cf = (
        data["fdmt_cf_n_ttmp"]
        .select(
            ["sec_id", "publish_date", "end_date", "n_cf_operate_a", "n_cf_fr_invest_a"]
        )
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(
            pl.col("n_cf_operate_a").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("n_cf_fr_invest_a").fill_nan(None).forward_fill().over("sec_id"),
        )
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort("sec_id", "end_date", "publish_date")
        .unique("sec_id", keep="last")
        .select(["sec_id", "n_cf_operate_a", "n_cf_fr_invest_a"])
    )
    _da = (
        data["fdmt_main_data_pit"]
        .select(["sec_id", "publish_date", "end_date", "da"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("da").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort("sec_id", "end_date", "publish_date")
        .unique("sec_id", keep="last")
        .select(["sec_id", "da"])
    ).select(["sec_id", "da"])

    acf_long = (
        _is.join(_cf, on="sec_id")
        .join(_da, on="sec_id")
        .select(
            [
                "sec_id",
                (
                    pl.col("n_income")
                    - (pl.col("n_cf_operate_a") + pl.col("n_cf_fr_invest_a"))
                    + pl.col("da")
                ).alias("ACF"),
            ]
        )
    )
    return acf_long


def cal_vsal(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    营业额的波动性
    （过去五年）年度营业收入标准差/年度营业收入均值。
    """
    field_def = {
        "table": "fdmt_is_n_ttmp",
        "cols": ["symbol", "publish_date", "end_date", "revenue"],
    }
    vsal_long = (
        data[field_def["table"]]
        .select(["sec_id", "publish_date", "end_date", "revenue"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("revenue").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
            pl.col("end_date").dt.year() >= trd_date.year - 5,
            pl.col("end_date").dt.month() == 12,
        )
        .unique(["sec_id", "end_date"], keep="last")
        .group_by("sec_id")
        .agg(
            [
                pl.when(pl.col("revenue").mean() != 0)
                .then(pl.col("revenue").std() / pl.col("revenue").mean())
                .otherwise(None)
                .alias("VSAL")
            ]
        )
    )
    return vsal_long


def cal_vern(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    盈利的波动性
    （过去五年）年度盈利的标准差/年度盈利均值。
    """
    field_def = {
        "table": "fdmt_is_n_ttmp",
        "cols": ["symbol", "publish_date", "end_date", "n_income"],
    }

    vern_long = (
        data[field_def["table"]]
        .select(["sec_id", "publish_date", "end_date", "n_income"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("n_income").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
            pl.col("end_date").dt.year() >= trd_date.year - 5,
            pl.col("end_date").dt.month() == 12,
        )
        .unique(["sec_id", "end_date"], keep="last")
        .group_by("sec_id")
        .agg(
            [
                pl.when(pl.col("n_income").mean() != 0)
                .then(pl.col("n_income").std() / pl.col("n_income").mean())
                .otherwise(None)
                .alias("VERN")
            ]
        )
    )

    return vern_long


def cal_vflo(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
     现金流量的波动性
    （过去五年）年度现金流量净额的标准差/年度现金流量净额均值。
    """
    field_def = {
        "table": "fdmt_cf_n_ttmp",
        "cols": [
            "symbol",
            "publish_date",
            "end_date",
            "n_cf_operate_a",
            "n_cf_fr_invest_a",
            "n_cf_fr_finan_a",
        ],
    }

    vflo_df = (
        data[field_def["table"]]
        .select(
            [
                "sec_id",
                "publish_date",
                "end_date",
                "n_cf_operate_a",
                "n_cf_fr_invest_a",
                "n_cf_fr_finan_a",
            ]
        )
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(
            pl.col("n_cf_operate_a").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("n_cf_fr_invest_a").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("n_cf_fr_finan_a").fill_nan(None).forward_fill().over("sec_id"),
        )
        .filter(
            pl.col("publish_date") < trd_date,
            pl.col("end_date").dt.year() >= trd_date.year - 5,
            pl.col("end_date").dt.month() == 12,
        )
        .unique(["sec_id", "end_date"], keep="last")
        .with_columns(
            pl.sum_horizontal(
                pl.col("n_cf_operate_a"),
                pl.col("n_cf_fr_invest_a"),
                pl.col("n_cf_fr_finan_a"),
            ).alias("n_cf_a")
        )
        .group_by("sec_id")
        .agg(
            [
                pl.when(pl.col("n_cf_a").mean() != 0)
                .then(pl.col("n_cf_a").std() / pl.col("n_cf_a").mean())
                .otherwise(None)
                .alias("VFLO")
            ]
        )
    )

    return vflo_df


def cal_etopf_std(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    预测收益市盈率的波动性
    预测 12 月的每股收益（EPS）标准差/当前股价。
    """
    fin_field_def = {
        "table": "con_sec_coredata",
        "cols": ["sec_id", "rep_fore_date", "fore_year", "con_eps"],
    }
    etopd_std_df = (
        data[fin_field_def["table"]]
        .select(fin_field_def["cols"])
        .sort(["sec_id", "fore_year", "rep_fore_date"])
        .with_columns(pl.col("con_eps").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("rep_fore_date") <= trd_date,
            pl.col("fore_year") >= trd_date.year - 5,
        )
        .unique(["sec_id", "fore_year"], keep="last")
        .group_by("sec_id")
        .agg(pl.col("con_eps").std().alias("eps_std"))
    )
    stk_close = data.get("_stk_close_today_long")
    if stk_close is None:
        stk_close = (
            data["stk_close"]
            .filter(pl.col("date") == trd_date)
            .select(pl.all().exclude("date"))
            .unpivot(variable_name="sec_id", value_name="close")
        )
    etopf_std_df = etopd_std_df.join(stk_close, on="sec_id", how="left").select(
        pl.col("sec_id"),
        pl.when(pl.col("close") != 0)
        .then(pl.col("eps_std") / pl.col("close"))
        .otherwise(None)
        .alias("ETOPF_STD"),
    )
    return etopf_std_df


def cal_etopf(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    预测市盈率
    未来 12 个月的预测盈利除以每日股价。
    """
    eps = (
        data["con_sec_coredata"]
        .select(["sec_id", "rep_fore_date", "fore_year", "con_eps"])
        .sort(["sec_id", "fore_year", "rep_fore_date"])
        .with_columns(pl.col("con_eps").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("rep_fore_date") <= trd_date, pl.col("fore_year") == trd_date.year
        )
        .unique(["sec_id"], keep="last")
        .select(pl.col("sec_id"), pl.col("con_eps").alias("eps_forecast"))
    )

    stk_close = data.get("_stk_close_today_long")
    if stk_close is None:
        stk_close = (
            data["stk_close"]
            .filter(pl.col("date") == trd_date)
            .select(pl.all().exclude("date"))
            .unpivot(variable_name="sec_id", value_name="close")
        )

    df = eps.join(stk_close, on="sec_id", how="left")
    etopf_df = df.select(
        pl.col("sec_id"),
        pl.when(pl.col("close") != 0)
        .then(pl.col("eps_forecast") / pl.col("close"))
        .otherwise(None)
        .alias("ETOPF"),
    )
    return etopf_df


def cal_cetop(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    现金市盈率
    过去 12 个月的现金盈利除以当前市值。
    """
    cetop_df = (
        data["fdmt_md_n_ttmp"]
        .select(["sec_id", "ebitda", "publish_date", "end_date"])
        .sort(["sec_id", "publish_date", "end_date"])
        .with_columns(pl.col("ebitda").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
        .select("sec_id", "ebitda")
    )

    stk_mv = data.get("_stk_mv_last_long")
    if stk_mv is None:
        stk_mv = (
            data["stk_market_value"]
            .filter(pl.col("date") <= trd_date)
            .sort("date")
            .last()
            .select(pl.all().exclude("date"))
            .unpivot(variable_name="sec_id", value_name="mv")
        )
    cetop_df = cetop_df.join(stk_mv, on="sec_id", how="left").select(
        pl.col("sec_id"),
        pl.when(pl.col("mv") != 0)
        .then(pl.col("ebitda") / pl.col("mv"))
        .otherwise(None)
        .alias("CETOP"),
    )

    return cetop_df


def cal_etop(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    滞后市盈率
    过去 12 个月的盈利除以当前市值。
    """
    etop_df = (
        data["mkt_equd"]
        .select(["sec_id", "trade_date", "pe"])
        .filter(
            pl.col("trade_date") <= trd_date,
        )
        .sort(["sec_id", "trade_date"])
        .unique("sec_id", keep="last")
        .select("sec_id", pl.col("pe").alias("ETOP"))
    )
    return etop_df


def cal_em(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    企业乘数
    上一年的息税前利润（𝐸𝐵𝐼𝑇）除以当前企业价值（𝐸𝑉）。
    """
    ebit = (
        data["fdmt_md_n_ttmp"]
        .select(["sec_id", "end_date", "publish_date", "ebit"])
        .filter(
            pl.col("end_date") <= trd_date,
        )
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("ebit").fill_nan(None).forward_fill().over("sec_id"))
        .unique(["sec_id"], keep="last")
        .select(["sec_id", "ebit"])
    )
    stk_mv = data.get("_stk_mv_last_long")
    if stk_mv is None:
        stk_mv = (
            data["stk_market_value"]
            .filter(pl.col("date") <= trd_date)
            .sort("date")
            .last()
            .select(pl.all().exclude("date"))
            .unpivot(variable_name="sec_id", value_name="mv")
        )
    em_df = ebit.join(stk_mv, on="sec_id", how="left").select(
        pl.col("sec_id"),
        pl.when(pl.col("mv") != 0)
        .then(pl.col("ebit") / pl.col("mv"))
        .otherwise(None)
        .alias("EM"),
    )
    return em_df


def cal_egrlf(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    分析师预测长期盈利增长率
    分析师预测的长期盈利增长率。
    """
    egrlf_df = (
        data["con_sec_corederi"]
        .select(["sec_id", "rep_fore_date", "fore_year", "con_profit_cgr2y"])
        .sort(["sec_id", "fore_year", "rep_fore_date"])
        .with_columns(
            pl.col("con_profit_cgr2y").fill_nan(None).forward_fill().over("sec_id")
        )
        .filter(
            pl.col("rep_fore_date") <= trd_date,
        )
        .unique(["sec_id"], keep="last")
        .select(["sec_id", pl.col("con_profit_cgr2y").alias("EGRLF")])
    )

    return egrlf_df


def cal_egro(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    每股收益增长率
    将过去五个财年的年度每股收益与时间进行回归，再用回归系数除以年度每股收益的均值。
    """
    stk_eps = (
        data["fdmt_indi_ps_ttm_pit"]
        .select(["sec_id", "end_date", "publish_date", "eps"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("eps").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            [
                pl.col("publish_date") <= trd_date,
                pl.col("end_date").dt.month() == 12,
                pl.col("end_date").dt.year() >= trd_date.year - 5,
            ]
        )
        .unique(
            ["sec_id", "end_date"],
            keep="last",
        )
    )

    # Use group_by to perform regression for each stock
    # Calculate: slope / mean where slope = corr * (std_y / std_x)
    egro_df = (
        stk_eps.with_columns(
            pl.col("end_date").rank("ordinal").over("sec_id").alias("time_idx")
        )
        .group_by("sec_id")
        .agg(
            (
                pl.when(pl.col("time_idx").cast(pl.Float64).std() > 1e-10)
                .then(
                    pl.corr("eps", "time_idx")
                    * (pl.col("eps").std() / pl.col("time_idx").cast(pl.Float64).std())
                    / pl.col("eps").mean()
                )
                .otherwise(None)
                .alias("EGRO")
            )
        )
    )

    # 优化：移除调试打印，减少不必要的collect操作
    return egro_df


def cal_sgro(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    销售增长率
    将过去五个财年的年度销售收入与时间进行回归，再用回归系数除以年度销售收入的均值。
    """
    stk_sgro = (
        data["fdmt_indi_ps_ttm_pit"]
        .select(["sec_id", "end_date", "publish_date", "rev_ps"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("rev_ps").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            [
                pl.col("publish_date") <= trd_date,
                pl.col("end_date").dt.month() == 12,
                pl.col("end_date").dt.year() >= trd_date.year - 5,
            ]
        )
        .unique(
            ["sec_id", "end_date"],
            keep="last",
        )
    )

    sgro_df = (
        stk_sgro.with_columns(
            pl.col("end_date").rank("ordinal").over("sec_id").alias("time_idx")
        )
        .group_by("sec_id")
        .agg(
            (
                pl.corr("rev_ps", "time_idx")
                * (pl.col("rev_ps").std() / pl.col("time_idx").cast(pl.Float64).std())
                / pl.col("rev_ps").mean()
            ).alias("SGRO")
        )
    )

    return sgro_df


def cal_agro(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    总资产增长率
    最近三个财年的总资产对时间回归的斜率，除以平均总资产再取相反数。
    """
    stk_ta = (
        data["fdmt_bs_n_qa_pit"]
        .select(["sec_id", "end_date", "publish_date", "t_assets"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("t_assets").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            [
                pl.col("publish_date") <= trd_date,
                pl.col("end_date").dt.month() == 12,
                pl.col("end_date").dt.year() >= trd_date.year - 3,
            ]
        )
        .unique(
            ["sec_id", "end_date"],
            keep="last",
        )
    )

    agro_df = (
        stk_ta.with_columns(
            pl.col("end_date").rank("ordinal").over("sec_id").alias("time_idx")
        )
        .group_by("sec_id")
        .agg(
            (
                -pl.corr("t_assets", "time_idx")
                * (pl.col("t_assets").std() / pl.col("time_idx").cast(pl.Float64).std())
                / pl.col("t_assets").mean()
            ).alias("AGRO")
        )
    )

    return agro_df


def cal_cxgro(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Investment Quality

    资本支出增长率

    最近三个财年的资本支出对时间回归的斜率，除以平均资本支出，再取相反数。
    """
    stk_capex = (
        data["fdmt_md_n_ttmp"]
        .select(["sec_id", "end_date", "publish_date", "cp_exp"])
        .sort(["sec_id", "end_date", "publish_date"])
        .with_columns(pl.col("cp_exp").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            [
                pl.col("publish_date") <= trd_date,
                pl.col("end_date").dt.month() == 12,
                pl.col("end_date").dt.year() >= trd_date.year - 3,
            ]
        )
        .unique(
            ["sec_id", "end_date"],
            keep="last",
        )
    )

    cxgro_df = (
        stk_capex.with_columns(
            pl.col("end_date").rank("ordinal").over("sec_id").alias("time_idx")
        )
        .group_by("sec_id")
        .agg(
            (
                -pl.corr("cp_exp", "time_idx")
                * (pl.col("cp_exp").std() / pl.col("time_idx").cast(pl.Float64).std())
                / pl.col("cp_exp").mean()
            ).alias("CXGRO")
        )
    )

    return cxgro_df


def cal_igro(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Investment Quality

    发行量增长率

    最近三个财年的流通股本对时间回归的斜率，除以平均流通股本，再取相反数。
    """
    stk_igro = (
        data["equ_free_shares"]
        .select(["sec_id", "publish_date", "free_shares"])
        .sort(["sec_id", "publish_date"])
        .with_columns(
            pl.col("free_shares").fill_nan(None).forward_fill().over("sec_id")
        )
        .filter(
            [
                pl.col("publish_date") <= trd_date,
                pl.col("publish_date").dt.year() >= (trd_date.year - 3),
            ]
        )
        .group_by(["sec_id", pl.col("publish_date").dt.year().alias("end_year")])
        .agg(pl.col("free_shares").last())
    )

    igro_df = (
        stk_igro.with_columns(
            pl.col("end_year").rank("ordinal").over("sec_id").alias("time_idx")
        )
        .sort(["sec_id", "time_idx"])
        .unique(["sec_id", "time_idx"], keep="last")
        .group_by("sec_id")
        .agg(
            (
                -pl.corr("free_shares", "time_idx")
                * (
                    pl.col("free_shares").std()
                    / pl.col("time_idx").cast(pl.Float64).std()
                )
                / pl.col("free_shares").mean()
            ).alias("IGRO")
        )
    )

    return igro_df


def cal_mlev(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Leverage

    市场杠杆率

    MLEV=(ME+LD)/ME

    注：ME是上个交易日的市值，LD是最近报告的长期负债。
    """
    stk_mv = data.get("_stk_mv_last_long")
    if stk_mv is None:
        stk_mv = (
            data["stk_market_value"]
            .filter(pl.col("date") <= trd_date)
            .sort("date")
            .last()
            .select(pl.all().exclude("date"))
            .unpivot(variable_name="sec_id", value_name="mv")
        )
    stk_ld = (
        data["fdmt_bs_n_qa_pit"]
        .select("sec_id", "publish_date", "t_ncl")
        .sort(["sec_id", "publish_date"])
        .with_columns(pl.col("t_ncl").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort(["sec_id", "publish_date"])
        .unique("sec_id", keep="last")
    )

    mlev_df = stk_mv.join(stk_ld, on="sec_id", how="left").select(
        pl.col("sec_id"),
        pl.when(pl.col("mv") != 0)
        .then((pl.col("mv") + pl.col("t_ncl")) / pl.col("mv"))
        .otherwise(None)
        .alias("MLEV"),
    )

    return mlev_df


def cal_dtoa(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Leverage

    资产负债比

    DTOA=TL/TA

    注：TL总负债（包括长期负债和流动负债）；TA：最近报告的总资产账面价值
    """
    stk_ta = (
        data["fdmt_bs_n_qa_pit"]
        .select("publish_date", "sec_id", "t_assets")
        .sort(["sec_id", "publish_date"])
        .with_columns(pl.col("t_assets").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
    )
    stk_tl = (
        data["fdmt_bs_n_qa_pit"]
        .select("publish_date", "sec_id", "t_liab")
        .sort(["sec_id", "publish_date"])
        .with_columns(pl.col("t_liab").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
    )

    dtoa_df = stk_ta.join(stk_tl, on=["sec_id"], how="left").select(
        pl.col("sec_id"),
        pl.when(pl.col("t_assets") != 0)
        .then(pl.col("t_liab") / pl.col("t_assets"))
        .otherwise(None)
        .alias("DTOA"),
    )

    return dtoa_df


def cal_blev(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Leverage

    账面杠杆率

    BLEV=(BE+LA)/BE

    注：BE最近报告的普通股账面价值；LD最近报告的长期负债
    """
    stk_be = (
        data["fdmt_bs_n_qa_pit"]
        .select("publish_date", "sec_id", "t_sh_equity")
        .sort(["sec_id", "publish_date"])
        .with_columns(
            pl.col("t_sh_equity").fill_nan(None).forward_fill().over("sec_id")
        )
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
    )
    stk_la = (
        data["fdmt_bs_n_qa_pit"]
        .select("publish_date", "sec_id", "t_ncl")
        .sort(["sec_id", "publish_date"])
        .with_columns(pl.col("t_ncl").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
    )
    blev_df = stk_be.join(stk_la, on=["sec_id"], how="left").select(
        pl.col("sec_id"),
        pl.when(pl.col("t_sh_equity") != 0)
        .then((pl.col("t_sh_equity") + pl.col("t_ncl")) / pl.col("t_sh_equity"))
        .otherwise(None)
        .alias("BLEV"),
    )
    return blev_df


def cal_stom(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Liquidity

    月度换手率

    过去21个交易日的每日换手率之和的对数。
    """
    stom_df = (
        data["stk_turn"]
        .filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(21)
        .select(
            pl.when(pl.all().exclude("date").sum() > 0)
            .then(pl.all().exclude("date").sum().log())
            .otherwise(None)
        )
        .unpivot(variable_name="sec_id", value_name="STOM")
    )
    return stom_df


def cal_stoq(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Liquidity

    季度换手率

    过去3个月的月均换手率的对数。
    """
    stoq_df = (
        data["stk_turn"]
        .filter((pl.col("date") <= trd_date))
        .sort("date")
        .tail(63)
        .select(
            pl.when((pl.all().exclude("date").mean() * 21) > 0)
            .then((pl.all().exclude("date").mean() * 21).log())
            .otherwise(None)
        )
        .unpivot(variable_name="sec_id", value_name="STOQ")
    )
    return stoq_df


def cal_stoa(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Liquidity

    年度换手率

    过去12个月的月均换手率的对数。
    """
    stoa_df = (
        data["stk_turn"]
        .filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(252)
        .select(
            pl.when((pl.all().exclude("date").mean() * 21) > 0)
            .then((pl.all().exclude("date").mean() * 21).log())
            .otherwise(None)
        )
        .unpivot(variable_name="sec_id", value_name="STOA")
    )
    return stoa_df


def cal_atvr(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Liquidity

    ATVR 年化交易量比率

    通过对过去 252 天内每日交易的股票百分比进行加权计算得出，该加权使用 63 天的半衰期进行指数加权。
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        turn_arr = data.get("_stk_turn_arr")
        turn_cols = data.get("_stk_turn_cols")
        if (
            isinstance(turn_arr, np.ndarray)
            and isinstance(turn_cols, list)
            and turn_arr.ndim == 2
        ):
            y = turn_arr  # already last 252 days
            ewm = _ewm_mean_adjust(y, half_life=63)
            last = ewm[-1, :]
            return pl.DataFrame({"sec_id": turn_cols, "ATVR": last}).lazy()

    atvr_df = (
        data["stk_turn"]
        .filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(252)
        .select(pl.all().exclude("date").ewm_mean(half_life=63).last())
        .unpivot(variable_name="sec_id", value_name="ATVR")
    )
    return atvr_df


def cal_ltrstr(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Long-Term Reversal

    LTRSTR 长期相对强度

    1. 计算非滞后的长期相对强度：股票对数收益率的加权和，时间窗口为 756 个交易日，半衰期为 195 个交易日。
    2. 向后推 273 个交易日，在该时点求近 11 个交易日的均值，再取相反数。
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        stk_ret_arr = data.get("_stk_ret_arr")
        stock_cols = data.get("_stk_cols")
        if (
            isinstance(stk_ret_arr, np.ndarray)
            and isinstance(stock_cols, list)
            and stk_ret_arr.ndim == 2
        ):
            y = stk_ret_arr[-1040:, :]
            y = np.where((1.0 + y) > 0, np.log(1.0 + y), np.nan)
            ewm = _ewm_mean_adjust(y, half_life=195)
            start = ewm.shape[0] - 273 - 11
            end = start + 11
            out = -np.nanmean(ewm[start:end, :], axis=0)
            return pl.DataFrame({"sec_id": stock_cols, "LTRSTR": out}).lazy()

    ltrstr_df = (
        data["stk_ret"]
        .filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(1040)
        .with_columns((pl.all().exclude("date") + 1).log().ewm_mean(half_life=195))
        .slice(-273 - 11, 11)
        .select(-pl.all().exclude("date").mean())
        .unpivot(variable_name="sec_id", value_name="LTRSTR")
    )
    return ltrstr_df


def cal_lthalpha(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Long-Term Reversal

    LTHALPHA 长期历史alpha

    1. 与之前计算BETA 的模型相同，但使用 756 个交易日的时间窗口和 195 个交易日的半衰期计算出ALPHA。
    2. 向后推 273 个交易日，在该时点求近 11 个交易日的均值，再取相反数。

    使用WLS (Weighted Least Squares) 进行rolling beta和alpha计算
    使用向量化numpy实现
    """
    stk_ret = data["stk_ret"]
    idx_ret = (
        data["idx_ret"].select(["date", "国证A指"]).rename({"国证A指": "idx_return"})
    )

    # Filter and prepare data with 756 trading days window
    merged = (
        stk_ret.join(idx_ret, on="date", how="inner")
        .filter(pl.col("date") <= trd_date)
        .sort("date", descending=False)
        .tail(1040)  # 756 + 273 + 11 buffer for shifting
    )

    # Get stock columns
    stock_cols = [
        col
        for col in merged.select(pl.all().exclude("date", "idx_return"))
        .collect_schema()
        .names()
    ]

    # Convert to numpy arrays for vectorized processing
    merged_collected = merged.collect()
    stk_ret_arr = merged_collected.select(stock_cols).to_numpy().astype(float)
    idx_ret_arr = (
        merged_collected.select("idx_return").to_numpy().astype(float).flatten()
    )

    # Call vectorized WLS calculation
    lthalpha_values = _calc_rolling_wls_alpha_vectorized(
        stk_ret_arr, idx_ret_arr, half_life=195, window=756, shift=273, avg_days=11
    )

    # Create result DataFrame
    lthalpha_df = pl.DataFrame(
        {"sec_id": stock_cols, "LTHALPHA": lthalpha_values}
    ).lazy()

    return lthalpha_df


def _calc_rolling_wls_alpha_vectorized(
    stk_ret_arr: np.ndarray,
    idx_ret_arr: np.ndarray,
    half_life: int = 195,
    window: int = 756,
    shift: int = 273,
    avg_days: int = 11,
) -> np.ndarray:
    """
    Optimized calculation of rolling WLS alpha for specific days only

    Args:
        stk_ret_arr: Array of stock returns, shape (T, N) where T=time, N=stocks
        idx_ret_arr: Array of index returns, shape (T,)
        half_life: Half-life for exponential weights (default 195)
        window: Rolling window size (default 756)
        shift: Number of days to shift back (default 273)
        avg_days: Number of days to average (default 11)

    Returns:
        Array of LTHALPHA values for each stock, shape (N,)

    Process:
    1. Only calculate WLS alpha for the specific windows needed for averaging
    2. Shift back 273 days, calculate alpha for last 11 days only
    3. Take mean of these 11 days and negate the result
    """
    T, N = stk_ret_arr.shape

    # Calculate number of rolling windows
    n_windows = T - window + 1
    if n_windows < shift + avg_days:
        # Not enough data
        return np.full(N, np.nan)

    # Pre-calculate exponential weights for the window
    # days_back: 755, 754, ..., 1, 0 (0 = most recent in window)
    days_back = np.arange(window - 1, -1, -1)
    weights = np.exp(-np.log(2) * days_back / half_life)

    # 关键优化：只计算需要平均的11个窗口的alpha值
    # 这些窗口对应于 (n_windows - shift - avg_days) 到 (n_windows - shift - 1)
    target_start = n_windows - shift - avg_days
    target_end = n_windows - shift

    if target_start < 0:
        return np.full(N, np.nan)

    # Prepare array to store alpha for the target windows only
    alpha_target = np.full((avg_days, N), np.nan)

    # 只计算需要的窗口，大幅减少计算量（从~285个窗口减少到11个）
    for i in range(target_start, target_end):
        window_start = i
        window_end = i + window

        y_window = stk_ret_arr[window_start:window_end, :]  # (window, N)
        x_window_raw = idx_ret_arr[window_start:window_end].astype(float)  # (window,)

        if np.all(np.isnan(y_window)) or np.all(np.isnan(x_window_raw)):
            continue

        # Drop non-finite x by zeroing its weight (equivalent to row-wise missing).
        x_ok = np.isfinite(x_window_raw)
        w_eff = weights * x_ok
        x_window = np.where(x_ok, x_window_raw, 0.0)

        alpha, _beta = _wls_alpha_beta_single_regressor(y_window, x_window, w_eff)
        alpha_target[i - target_start, :] = alpha

    # Take mean over the avg_days and negate
    lthalpha = -np.nanmean(alpha_target, axis=0)  # (N,)

    return lthalpha


def cal_mid_cap(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Mid Capitalization

    MID-CAP 中盘/非线性市值

    公司总市值（Size）的立方。
    """
    stk_mv_long = data.get("_stk_mv_last_long")
    if stk_mv_long is not None:
        return stk_mv_long.select(
            pl.col("sec_id"),
            (pl.col("log_mv") ** 3).alias("MID-CAP"),
        )

    stk_mv = data["stk_market_value"]

    # Data is in wide format (date rows × stk_id columns)
    # Get latest row
    mid_cap_df = (
        stk_mv.filter(pl.col("date") <= trd_date)
        .sort("date")
        .last()
        .select(pl.all().exclude("date").log())
        .unpivot(variable_name="sec_id", value_name="market_value")
        .select(
            pl.col("sec_id"),
            (pl.col("market_value") ** 3).alias("MID-CAP"),
        )
    )
    return mid_cap_df


def cal_rstr(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Momentum

    RSTR 非滞后的相对强度

    1. 个股相对市场的对数超额收益在过去252个交易日的时间窗口内的指数衰减加权和，权重的半衰期为 126 个交易日。
    2. 向后推 11 个交易日，在该时点求近 11 个交易日的均值。
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        stk_ret_arr = data.get("_stk_ret_arr")
        idx_ret_arr = data.get("_idx_ret_arr")
        stock_cols = data.get("_stk_cols")
        if (
            isinstance(stk_ret_arr, np.ndarray)
            and isinstance(idx_ret_arr, np.ndarray)
            and isinstance(stock_cols, list)
            and stk_ret_arr.ndim == 2
            and idx_ret_arr.ndim == 1
            and stk_ret_arr.shape[0] == idx_ret_arr.shape[0]
        ):
            y = stk_ret_arr[-274:, :]
            x = idx_ret_arr[-274:]
            y_log = np.where((1.0 + y) > 0, np.log(1.0 + y), np.nan)
            x_log = np.where((1.0 + x) > 0, np.log(1.0 + x), np.nan)
            ex = y_log - x_log[:, None]
            ewm = _ewm_mean_adjust(ex, half_life=126)
            start = ewm.shape[0] - 11 - 11
            end = start + 11
            out = np.nanmean(ewm[start:end, :], axis=0)
            return pl.DataFrame({"sec_id": stock_cols, "RSTR": out}).lazy()

    stk_ret = data["stk_ret"]
    idx_ret = data["idx_ret"].select(["date", "国证A指"])
    merged = (
        stk_ret.join(idx_ret, on="date", how="inner")
        .filter(pl.col("date") <= trd_date)
        .sort("date", descending=False)
        .tail(274)
    )
    rstr_df = (
        merged.with_columns(
            (
                (pl.all().exclude(["date", "国证A指"]) + 1).log()
                - (pl.col("国证A指") + 1).log()
            ).ewm_mean(half_life=126)
        )
        .slice(-11 - 11, 11)
        .select(pl.all().exclude(["date", "国证A指"]).mean())
        .unpivot(variable_name="sec_id", value_name="RSTR")
    )
    return rstr_df


def cal_halpha(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Momentum

    HALPHA alpha

    计算BETA的模型时的截距项。
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        stk_ret_arr = data.get("_stk_ret_arr")
        idx_ret_arr = data.get("_idx_ret_arr")
        stock_cols = data.get("_stk_cols")
        if (
            isinstance(stk_ret_arr, np.ndarray)
            and isinstance(idx_ret_arr, np.ndarray)
            and isinstance(stock_cols, list)
            and stk_ret_arr.shape[0] == idx_ret_arr.shape[0]
            and stk_ret_arr.ndim == 2
        ):
            y = stk_ret_arr[-252:, :]
            x = idx_ret_arr[-252:]
            alpha, _beta = _ols_alpha_beta_single_regressor_pairwise(y, x)
            return pl.DataFrame({"sec_id": stock_cols, "HALPHA": alpha}).lazy()

    stk_ret = data["stk_ret"]
    idx_ret = data["idx_ret"].select(["date", "国证A指"])
    merged = stk_ret.join(idx_ret, on="date", how="inner")

    # Simplified implementation - calculate alpha directly
    # For each stock, calculate: alpha = mean(ret) - beta * mean(idx_return)
    # where beta = cov(ret, idx_return) / var(idx_return)
    beta = pl.cov(pl.all().exclude(["date", "国证A指"]), "国证A指") / pl.var("国证A指")
    alpha = pl.all().exclude(["date", "国证A指"]).mean() - beta * pl.mean("国证A指")

    halpha_df = (
        merged.filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(252)
        .select(alpha)
        .unpivot(variable_name="sec_id", value_name="HALPHA")
    )
    return halpha_df


def cal_size(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Size

    SIZE 市值

    公司总市值的对数。
    """
    stk_mv_long = data.get("_stk_mv_last_long")
    if stk_mv_long is not None:
        return stk_mv_long.select(pl.col("sec_id"), pl.col("log_mv").alias("SIZE"))

    stk_mv = data["stk_market_value"]

    # Data is in wide format (date rows × stk_id columns)
    # Get latest row
    log_size_lf = (
        stk_mv.filter(pl.col("date") <= trd_date)
        .sort("date")
        .last()
        .select(pl.all().exclude("date").log())
        .unpivot(variable_name="sec_id", value_name="SIZE")
    )

    return log_size_lf


def cal_ato(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Profitability

    ATO 资产周转率

    ATO=Sales/TA

    Sales：过去 12 个月的营业总收入
    TA：总资产
    """
    ato = (
        data["fdmt_indi_trnovr_ttm_pit"]
        .select(["sec_id", "publish_date", "ta_turnover"])
        .sort(["sec_id", "publish_date"])
        .with_columns(
            pl.col("ta_turnover").fill_nan(None).forward_fill().over("sec_id")
        )
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
        .select(
            pl.col("sec_id"),
            pl.col("ta_turnover").alias("ATO"),
        )
    )

    return ato


def cal_gp(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Profitability

    GP 资产毛利率

    GP=(Sales-COGS)/TA

    COGS：营业成本
    """
    ta_latest = (
        data["fdmt_bs_n_qa_pit"]
        .select("sec_id", "publish_date", "t_assets")
        .sort(["sec_id", "publish_date"])
        .with_columns(pl.col("t_assets").fill_nan(None).forward_fill().over("sec_id"))
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .unique("sec_id", keep="last")
        .select(pl.col("sec_id"), pl.col("t_assets"))
    )
    gp_latest = (
        data["fdmt_is_n_ttmp"]
        .select("sec_id", "publish_date", "revenue", "cogs")
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort("sec_id", "publish_date", descending=False)
        .with_columns(
            pl.col("revenue").fill_nan(None).forward_fill().over("sec_id"),
            pl.col("cogs").fill_nan(None).forward_fill().over("sec_id"),
        )
        .unique("sec_id", keep="last")
        .select(
            pl.col("sec_id"),
            (pl.col("revenue") - pl.col("cogs")).alias("gp"),
        )
    )
    merge = gp_latest.join(ta_latest, on="sec_id", how="inner")
    gp = merge.select(
        pl.col("sec_id"),
        pl.when(pl.col("t_assets") != 0)
        .then(pl.col("gp") / pl.col("t_assets"))
        .otherwise(None)
        .alias("GP"),
    )

    return gp


def cal_gpm(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Profitability

    GPM 销售毛利率

    GPM=(Sales-COGS)/Sales
    """
    gpm = (
        data["fdmt_indi_rtn_ttmpit"]
        .select(["sec_id", "publish_date", "gross_margin"])
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort(["sec_id", "publish_date"])
        .with_columns(
            pl.col("gross_margin").fill_nan(None).forward_fill().over("sec_id")
        )
        .unique("sec_id", keep="last")
        .select(
            pl.col("sec_id"),
            pl.col("gross_margin").alias("GPM"),
        )
    )
    return gpm


def cal_roa(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Profitability

    ROA 资产回报率
    """

    roa = (
        data["fdmt_indi_rtn_ttmpit"]
        .select(["sec_id", "publish_date", "roa"])
        .filter(
            pl.col("publish_date") <= trd_date,
        )
        .sort(["sec_id", "publish_date"])
        .with_columns(pl.col("roa").fill_nan(None).forward_fill().over("sec_id"))
        .unique("sec_id", keep="last")
        .select(
            pl.col("sec_id"),
            pl.col("roa").alias("ROA"),
        )
    )
    return roa


def cal_dastd(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Residual Volatility

    DASTD 日波动率

    过去 252 个交易日的日超额收益率的波动率，半衰期为42个交易日。
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        stk_ret_arr = data.get("_stk_ret_arr")
        idx_ret_arr = data.get("_idx_ret_arr")
        stock_cols = data.get("_stk_cols")
        if (
            isinstance(stk_ret_arr, np.ndarray)
            and isinstance(idx_ret_arr, np.ndarray)
            and isinstance(stock_cols, list)
            and stk_ret_arr.shape[0] == idx_ret_arr.shape[0]
            and stk_ret_arr.ndim == 2
        ):
            y = stk_ret_arr[-252:, :] - idx_ret_arr[-252:, None]
            # Polars ewm_std(...).last() is effectively an exponentially-weighted
            # std over the window (default adjust semantics).
            days_back = np.arange(y.shape[0] - 1, -1, -1)
            w = np.exp(-np.log(2) * days_back / 42)
            _mean, std = _weighted_mean_std(y, w)
            return pl.DataFrame({"sec_id": stock_cols, "DASTD": std}).lazy()

    stk_ret = data["stk_ret"].filter(pl.col("date") <= trd_date)
    idx_ret = (
        data["idx_ret"].select(["date", "国证A指"]).filter(pl.col("date") <= trd_date)
    )

    dastd_df = (
        stk_ret.join(idx_ret, on="date", how="inner")
        .sort("date", descending=False)
        .tail(252)
        .select(
            (pl.all().exclude("date", "国证A指") - pl.col("国证A指"))
            .ewm_std(half_life=42)
            .last()
        )
        .unpivot(variable_name="sec_id", value_name="DASTD")
    )

    return dastd_df


def cal_cmra(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Residual Volatility

    CMRA 累计收益范围

    过去 12 个月内累计超额对数收益率最高和最低的股票之差。
    设𝑍(𝑇)为过去 T 个月的累计超额对数收益率，每个月定义为
    之前的 21 个交易日。
    """
    if os.getenv("BARRA_DISABLE_NUMPY_FACTORS", "0") != "1":
        stk_close_hist_df = data.get("_stk_adjclose_hist_df")
        stock_cols = data.get("_stk_cols")
        if isinstance(stk_close_hist_df, pl.DataFrame) and isinstance(stock_cols, list):
            if stk_close_hist_df.height >= 253:
                df = stk_close_hist_df.tail(253)
                # positions from most recent: 0, 21, 42, ..., 252
                positions = np.arange(0, 253, 21, dtype=int)
                base = df.height - 1
                idxs = (base - positions).astype(int)
                idxs.sort()  # chronological

                prices = df.select(stock_cols).to_numpy()
                sel = prices[idxs, :]
                sel = np.where(sel > 0, sel, np.nan)
                logp = np.log(sel)
                monthly = np.diff(logp, axis=0)  # (M-1, N)
                # reverse so most recent month first, then cumulative sums
                cum = np.cumsum(monthly[::-1, :], axis=0)
                cmra = np.nanmax(cum, axis=0) - np.nanmin(cum, axis=0)
                return pl.DataFrame({"sec_id": stock_cols, "CMRA": cmra}).lazy()

    # 1. Get target dates (every 21 trading days backwards) using stk_close
    stk_close = data.get("_stk_adjclose_hist_1041")
    if stk_close is None:
        stk_close = data["stk_adjclose"]
    stk_close = stk_close.filter(pl.col("date") <= trd_date).sort("date")

    target_dates = (
        stk_close.select("date")
        .sort("date", descending=True)
        .head(253)
        .with_row_index("idx")
        .filter(pl.col("idx") % 21 == 0)
        .select("date")
    )

    # 2. Filter stock data to target dates
    merged = stk_close.join(target_dates, on="date", how="inner").sort("date")

    # 3. Calculate monthly log returns (relative to 0)
    cmra_df = (
        merged.with_columns(pl.all().exclude("date").log().diff())
        .sort("date", descending=True)
        .select(pl.all().exclude("date").cum_sum())
        .select((pl.all().max() - pl.all().min()))
        .unpivot(variable_name="sec_id", value_name="CMRA")
    )

    return cmra_df


def cal_hsigma(trd_date: date, data: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """
    Residual Volatility

    HSIGMA 历史波动率

    wls_std(epslon) 残差波动率 r_t = alpha + beta * r_mkt + epsilon_t
    使用过去252个交易日的日收益率进行估计，半衰期为63个交易日。
    """
    stk_ret = data["stk_ret"]
    idx_ret = (
        data["idx_ret"].select(["date", "国证A指"]).rename({"国证A指": "idx_return"})
    )

    merged = (
        stk_ret.join(idx_ret, on="date", how="inner")
        .filter(pl.col("date") <= trd_date)
        .sort("date")
        .tail(252)
    )

    merged_collected = merged.collect()
    stock_cols = [
        c for c in merged_collected.columns if c not in ["date", "idx_return"]
    ]

    if merged_collected.height <= 2:
        return pl.LazyFrame(
            {"sec_id": stock_cols, "HSIGMA": [np.nan] * len(stock_cols)}
        )

    y = merged_collected.select(stock_cols).to_numpy().astype(float)
    x = merged_collected.select("idx_return").to_numpy().astype(float).reshape(-1)

    T = x.shape[0]
    days_back = np.arange(T - 1, -1, -1)
    w = np.exp(-np.log(2) * days_back / 63)

    hsigma_values = _wls_resid_std_single_regressor(y, x, w)
    return pl.LazyFrame({"sec_id": stock_cols, "HSIGMA": hsigma_values})


def _prepare_day_data(
    trd_day: date, df_dict: dict[str, pl.LazyFrame]
) -> dict[str, pl.LazyFrame]:
    """
    Build a per-day in-memory cache for the widest panels to avoid re-scanning IPC files
    repeatedly across many factor computations.

    Disable via `BARRA_DISABLE_DAY_CACHE=1`.
    """
    if os.getenv("BARRA_DISABLE_DAY_CACHE", "0") == "1":
        return df_dict

    data = dict(df_dict)

    # Price -> returns (compute pct_change only on a small window).
    try:
        if "stk_adjclose" in df_dict:
            stk_close_hist = (
                df_dict["stk_adjclose"]
                .filter(pl.col("date") <= trd_day)
                .sort("date")
                .tail(1041)  # 1040 returns need 1041 prices
                .collect(engine="streaming")
            )
            if stk_close_hist.height > 0:
                data["_stk_adjclose_hist_1041"] = stk_close_hist.lazy()
                data["_stk_adjclose_hist_df"] = stk_close_hist
                stk_cols = [c for c in stk_close_hist.columns if c != "date"]
                data["_stk_cols"] = stk_cols
                stk_ret_hist = stk_close_hist.with_columns(
                    pl.all().exclude("date").pct_change()
                ).slice(1)
                data["stk_ret"] = stk_ret_hist.lazy()
                try:
                    data["_stk_ret_arr"] = (
                        stk_ret_hist.select(stk_cols).to_numpy().astype(float)
                    )
                except Exception as e:
                    __global_logger.debug(f"Day cache: stk_ret to numpy failed: {e}")
    except Exception as e:
        __global_logger.debug(f"Day cache: stk_adjclose/stk_ret failed: {e}")

    try:
        if "idx_close" in df_dict:
            idx_close_hist = (
                df_dict["idx_close"]
                .filter(pl.col("date") <= trd_day)
                .sort("date")
                .tail(1041)
                .collect(engine="streaming")
            )
            if idx_close_hist.height > 0:
                idx_ret_hist = idx_close_hist.with_columns(
                    pl.all().exclude("date").pct_change()
                ).slice(1)
                data["idx_ret"] = idx_ret_hist.lazy()
                try:
                    if "国证A指" in idx_ret_hist.columns:
                        data["_idx_ret_arr"] = (
                            idx_ret_hist.select("国证A指")
                            .to_numpy()
                            .astype(float)
                            .reshape(-1)
                        )
                except Exception as e:
                    __global_logger.debug(f"Day cache: idx_ret to numpy failed: {e}")
    except Exception as e:
        __global_logger.debug(f"Day cache: idx_close/idx_ret failed: {e}")

    # Turnover-related factors only need <= 252 trading days.
    try:
        if "stk_turn" in df_dict:
            stk_turn_hist = (
                df_dict["stk_turn"]
                .filter(pl.col("date") <= trd_day)
                .sort("date")
                .tail(252)
                .collect(engine="streaming")
            )
            if stk_turn_hist.height > 0:
                data["stk_turn"] = stk_turn_hist.lazy()
                turn_cols = [c for c in stk_turn_hist.columns if c != "date"]
                data["_stk_turn_cols"] = turn_cols
                try:
                    data["_stk_turn_arr"] = (
                        stk_turn_hist.select(turn_cols).to_numpy().astype(float)
                    )
                except Exception as e:
                    __global_logger.debug(f"Day cache: stk_turn to numpy failed: {e}")
    except Exception as e:
        __global_logger.debug(f"Day cache: stk_turn failed: {e}")

    # Market value (many factors only need the latest cross-section).
    try:
        if "stk_market_value" in df_dict:
            mv_wide = (
                df_dict["stk_market_value"]
                .filter(pl.col("date") <= trd_day)
                .sort("date")
                .last()
                .select(pl.all().exclude("date"))
                .collect(engine="streaming")
            )
            if mv_wide.height > 0:
                mv_long = mv_wide.unpivot(variable_name="sec_id", value_name="mv")
                mv_long = mv_long.with_columns(pl.col("mv").log().alias("log_mv"))
                data["_stk_mv_last_long"] = mv_long.lazy()
    except Exception as e:
        __global_logger.debug(f"Day cache: stk_market_value failed: {e}")

    # Today's close (used by ETOPF/ETOPF_STD).
    try:
        if "stk_close" in df_dict:
            close_wide = (
                df_dict["stk_close"]
                .filter(pl.col("date") == trd_day)
                .select(pl.all().exclude("date"))
                .collect(engine="streaming")
            )
            if close_wide.height > 0:
                data["_stk_close_today_long"] = close_wide.unpivot(
                    variable_name="sec_id", value_name="close"
                ).lazy()
    except Exception as e:
        __global_logger.debug(f"Day cache: stk_close failed: {e}")

    return data


def process_single_day(trd_day: date, df_dict: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    # 优化：减少日志输出频率，使用更高效的日志方式
    __global_logger.info(f"Processing trading day: {trd_day}")
    _ensure_fin_tables_from_cache(df_dict)
    df_dict = _prepare_day_data(trd_day, df_dict)
    profile_factors = os.getenv("BARRA_PROFILE_FACTORS", "0") == "1"
    factor_timings: list[tuple[str, float, tuple[int, int]]] = []
    # Placeholder for processing logic for a single trading day
    # This could involve filtering data for the specific day,
    # performing calculations, and saving results.

    # cal beta
    hbeta = BaseFactor(
        "HBETA",
        df_dict,
        cal_hbeta,
    )

    # cal Book-to-Price
    btop = BaseFactor("BTOP", df_dict, cal_btop)

    # cal Dividend-Yield
    dtop = BaseFactor("DTOP", df_dict, cal_dtop)
    dtopf = BaseFactor("DTOPF", df_dict, cal_dtopf)

    # cal Earnings Quality
    abs = BaseFactor("ABS", df_dict, cal_abs)
    acf = BaseFactor("ACF", df_dict, cal_acf)

    # cal Earnings Variability
    vsal = BaseFactor("VSAL", df_dict, cal_vsal)
    vern = BaseFactor("VERN", df_dict, cal_vern)
    vflo = BaseFactor("VFLO", df_dict, cal_vflo)
    etopf_std = BaseFactor("ETOPF_STD", df_dict, cal_etopf_std)

    # cal Earnings Yield
    etopf = BaseFactor("ETOPF", df_dict, cal_etopf)
    cetop = BaseFactor("CETOP", df_dict, cal_cetop)
    etop = BaseFactor("ETOP", df_dict, cal_etop)
    em = BaseFactor("EM", df_dict, cal_em)

    # cal Growth
    egrlf = BaseFactor("EGRLF", df_dict, cal_egrlf)
    egro = BaseFactor("EGRO", df_dict, cal_egro)
    sgro = BaseFactor("SGRO", df_dict, cal_sgro)

    # cal Investment Quality
    agro = BaseFactor("AGRO", df_dict, cal_agro)
    cxgro = BaseFactor("CXGRO", df_dict, cal_cxgro)
    igro = BaseFactor("IGRO", df_dict, cal_igro)

    # cal Leverage
    mlev = BaseFactor("MLEV", df_dict, cal_mlev)
    dtoa = BaseFactor("DTOA", df_dict, cal_dtoa)
    blev = BaseFactor("BLEV", df_dict, cal_blev)

    # cal Liquidity
    stom = BaseFactor("STOM", df_dict, cal_stom)
    stoq = BaseFactor("STOQ", df_dict, cal_stoq)
    stoa = BaseFactor("STOA", df_dict, cal_stoa)
    atvr = BaseFactor("ATVR", df_dict, cal_atvr)

    # cal Long-Term Reversal
    ltrstr = BaseFactor("LTRSTR", df_dict, cal_ltrstr)
    lthalpha = BaseFactor("LTHALPHA", df_dict, cal_lthalpha)

    # cal Mid Capitalization
    mid_cap = BaseFactor("MID-CAP", df_dict, cal_mid_cap)

    # cal Momentum
    rstr = BaseFactor("RSTR", df_dict, cal_rstr)
    halpha = BaseFactor("HALPHA", df_dict, cal_halpha)

    # cal Size
    size = BaseFactor("SIZE", df_dict, cal_size)

    # cal Profitability
    ato = BaseFactor("ATO", df_dict, cal_ato)
    gp = BaseFactor("GP", df_dict, cal_gp)
    gpm = BaseFactor("GPM", df_dict, cal_gpm)
    roa = BaseFactor("ROA", df_dict, cal_roa)

    # cal Residual Volatility
    dastd = BaseFactor("DASTD", df_dict, cal_dastd)
    cmra = BaseFactor("CMRA", df_dict, cal_cmra)
    hsigma = BaseFactor("HSIGMA", df_dict, cal_hsigma)

    # Collect all base factors
    all_base_factors = [
        hbeta,
        btop,
        dtop,
        dtopf,
        abs,
        acf,
        vsal,
        vern,
        vflo,
        etopf_std,
        etopf,
        cetop,
        etop,
        em,
        egrlf,
        egro,
        sgro,
        agro,
        cxgro,
        igro,
        mlev,
        dtoa,
        blev,
        stom,
        stoq,
        stoa,
        atvr,
        ltrstr,
        lthalpha,
        mid_cap,
        rstr,
        halpha,
        size,
        ato,
        gp,
        gpm,
        roa,
        dastd,
        cmra,
        hsigma,
    ]

    # 关键优化：流式处理 - 逐个因子collect并立即join
    # 这样内存中最多只有2个DataFrame（当前结果 + 新因子）
    __global_logger.info(
        "Processing %d base factors with streaming join...", len(all_base_factors)
    )
    combined_base = None

    for i, bf in enumerate(all_base_factors):
        factor_name = bf.name
        __global_logger.info(
            "[%d/%d] Processing %s...",
            i + 1,
            len(all_base_factors),
            factor_name,
        )

        try:
            # 计算因子的LazyFrame
            lf = bf.cal(trd_day)

            # 立即collect（使用streaming engine减少内存）
            t0 = perf_counter()
            df = lf.collect(engine="streaming")
            df_shape = df.shape

            # 如果是第一个因子，直接作为基础
            if combined_base is None:
                combined_base = df
                __global_logger.info(
                    "%s: initialized base (shape=%s)", factor_name, df.shape
                )
            else:
                # Join到现有结果
                combined_base = combined_base.join(df, on="sec_id", how="left")
                __global_logger.info(
                    "%s: joined (combined shape=%s)", factor_name, combined_base.shape
                )
            t2 = perf_counter()
            if profile_factors:
                factor_timings.append((factor_name, t2 - t0, df_shape))

            # 立即释放df
            del df, lf

            # 每5个因子清理一次内存
            if (i + 1) % 5 == 0:
                gc.collect()
                log_memory_usage(f"After {i + 1} factors")

        except Exception as e:
            __global_logger.error(f"Error processing factor {factor_name}: {e}")
            continue

    if combined_base is None:
        return pl.LazyFrame()

    if profile_factors and factor_timings:
        slowest = sorted(factor_timings, key=lambda x: x[1], reverse=True)[:10]
        __global_logger.info("Slowest factors (collect+join):")
        for name, secs, shape in slowest:
            __global_logger.info("%s: %.3fs (shape=%s)", name, secs, shape)

    # 转回LazyFrame以保持接口一致
    return combined_base.lazy()

    # Calculate Barra Factors from the combined DataFrame
    # barra_factors = [
    #     beta,
    #     book_to_price,
    #     dividend_yield,
    #     earnings_quality,
    #     earnings_variability,
    #     earnings_yield,
    #     growth,
    #     investment_quality,
    #     leverage,
    #     liquidity,
    #     long_term_reversal,
    #     mid_capitalization,
    #     momentum,
    #     size_factor,
    #     profitability,
    #     residual_volatility,
    # ]

    # barra_exprs: list[pl.Expr] = []
    # for bf in barra_factors:
    #     weighted_sum = pl.sum_horizontal(
    #         [
    #             pl.col(base.name).fill_null(0) * weight
    #             for base, weight in bf.base_factors
    #         ]
    #     )
    #     barra_exprs.append(weighted_sum.alias(bf.name))

    # return combined_base.select(["stk_id"] + barra_exprs)


def _scan_daily_table(table: str) -> pl.LazyFrame:
    lf = pl.scan_ipc(os.path.join(PATH_DAILY_DATA, f"{table}.feather"), memory_map=True)
    schema = lf.collect_schema()
    if FEATHER_INDEX_NAME in schema.names():
        lf = lf.rename({FEATHER_INDEX_NAME: "date"})
    return lf.with_columns(pl.col("date").str.to_date("%Y%m%d"))


def run_barra_pipeline(trd_days: list[date]) -> None:
    base_in_root = os.getenv("BARRA_BASE_IN_ROOT", PATH_FAC_ROOT)
    barra_out_root = os.getenv(
        "BARRA_BARRA_OUT_ROOT", os.path.join(PATH_FAC_ROOT, "barra")
    )
    os.makedirs(barra_out_root, exist_ok=True)

    industry_table = os.getenv("BARRA_INDUSTRY_TABLE", "stk_citic1_code")
    mv_table = os.getenv("BARRA_MV_TABLE", "stk_neg_market_value")
    industry_lf = _scan_daily_table(industry_table)
    mv_lf = _scan_daily_table(mv_table)

    winsor_p = float(os.getenv("BARRA_WINSOR_P", "0.01"))
    do_ortho = os.getenv("BARRA_ORTHOGONALIZE", "1") == "1"
    print_corr = os.getenv("BARRA_PRINT_CORR", "1") == "1"
    do_check = os.getenv("BARRA_CHECK", "1") == "1"

    for trd_day in tqdm(trd_days, desc="Computing barra factors"):
        base_file = os.path.join(base_in_root, f"{_ymd(trd_day)}.feather")
        if not os.path.exists(base_file):
            continue
        try:
            base_df = pl.read_ipc(base_file, memory_map=True)
        except Exception as e:
            __global_logger.error(f"{trd_day}: read base file failed: {e}")
            continue

        try:
            barra_df = compute_barra_factors_from_base(
                trd_day,
                base_df,
                industry_lf=industry_lf,
                mv_lf=mv_lf,
                industry_value_name=industry_table,
                mv_value_name=mv_table,
                winsor_p=winsor_p,
                do_orthogonalize=do_ortho,
                print_corr=print_corr,
                do_check=do_check,
            )
        except Exception as e:
            __global_logger.error(f"{trd_day}: compute barra factors failed: {e}")
            continue

        if barra_df.height == 0:
            continue
        out_file = os.path.join(barra_out_root, f"{_ymd(trd_day)}.feather")
        barra_df.write_ipc(out_file, compression="zstd")


def run_base_pipeline(trd_days: list[date]) -> None:
    existing_files: set[str] = set()
    if os.path.isdir(PATH_FAC_ROOT):
        existing_files = {
            f for f in os.listdir(PATH_FAC_ROOT) if f.endswith(".feather")
        }

    pending_days: list[date] = []
    skipped_days = 0
    for trd_day in trd_days:
        out_name = f"{trd_day.year:04d}{trd_day.month:02d}{trd_day.day:02d}.feather"
        if out_name in existing_files:
            skipped_days += 1
            continue
        pending_days.append(trd_day)

    if skipped_days:
        __global_logger.info(
            "Skipping %d days with existing outputs in %s",
            skipped_days,
            PATH_FAC_ROOT,
        )
    if not pending_days:
        __global_logger.info("All outputs exist; skipping base pipeline")
        return

    trd_days = pending_days

    # 优化：预先收集所有文件名，减少重复操作
    feather_files = [
        f
        for f in os.listdir(PATH_DAILY_DATA)
        if f.endswith(".feather") and (f.startswith("stk_") or f.startswith("idx_"))
    ]

    df_dict: dict[str, pl.LazyFrame] = {}
    for f in feather_files:
        df_name = f.split(".")[0]
        lf = pl.scan_ipc(os.path.join(PATH_DAILY_DATA, f), memory_map=True)

        # 优化：延迟schema收集，只在需要时进行
        try:
            schema = lf.collect_schema()
            if FEATHER_INDEX_NAME in schema.names():
                lf = lf.rename({FEATHER_INDEX_NAME: "date"})
        except Exception as e:
            __global_logger.error(f"Error collecting schema for {df_name}: {e}")
            continue

        lf = lf.with_columns(
            pl.col("date").str.to_date("%Y%m%d"),
        )

        # NOTE: full-scan "all-null column" detection is very expensive on wide panels.
        # Default off; enable by setting BARRA_FILTER_ALL_NULL_COLS=1.
        if os.getenv("BARRA_FILTER_ALL_NULL_COLS", "0") == "1":
            try:
                null_cols_expr = pl.all().is_null().all()
                null_cols_df = lf.select(null_cols_expr).collect(engine="streaming")
                non_null_cols = [
                    col for col in null_cols_df.columns if not null_cols_df[col][0]
                ]
                if non_null_cols:
                    df_dict[df_name] = lf.select(non_null_cols)
                else:
                    df_dict[df_name] = lf
            except Exception as e:
                __global_logger.error(
                    f"Error filtering null columns for {df_name}: {e}"
                )
                df_dict[df_name] = lf
        else:
            df_dict[df_name] = lf

    # 优化：确保数据存在后再计算收益率
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
    else:
        __global_logger.error("Required data for return calculation not found")

    cache_dir = os.path.join(PATH_FD_DATA, "barra_fd_cache")
    os.makedirs(cache_dir, exist_ok=True)

    try:
        # fin_start_date = trd_days[0].replace(year=trd_days[0].year - 5)
        fin_start_date = date(2015, 12, 31)
        fin_end_date = date(2025, 12, 31)
    except ValueError:
        fin_start_date = trd_days[0].replace(year=trd_days[0].year - 5, day=28)
        fin_end_date = trd_days[-1].replace(year=trd_days[-1].year, day=28)

    # 优化：添加内存监控和缓存管理
    log_memory_usage("Before loading financial data")

    for fin_table in TABLES:
        cache_file = os.path.join(
            cache_dir, f"{fin_table}_{fin_start_date}_{fin_end_date}.feather"
        )

        if os.path.exists(cache_file):
            __global_logger.info("Loading %s from cache: %s", fin_table, cache_file)
            df_dict[fin_table] = pl.scan_ipc(cache_file, memory_map=True)
        else:
            __global_logger.info("Fetching %s from MySQL...", fin_table)
            lf = get_fdmt_data_from_mysql(
                fin_table,
                FIN_TABLE_REQUIRED_COLS.get(fin_table, []),
                fin_start_date,
                fin_end_date,
            )
            # Cache the data
            __global_logger.info("Saving %s to cache: %s", fin_table, cache_file)
            # Prefer streaming sink to avoid huge in-memory materialization.
            if hasattr(lf, "sink_ipc"):
                lf.sink_ipc(cache_file, compression="zstd")
            else:
                lf.collect(engine="streaming").write_ipc(cache_file, compression="zstd")
            df_dict[fin_table] = lf

            # 优化：定期清理内存
            gc.collect()

    log_memory_usage("After loading financial data")

    multi_day_fac: list[pl.LazyFrame] = [pl.LazyFrame()] * len(trd_days)

    def process_day_task(args: tuple[int, date]) -> tuple[int, pl.LazyFrame]:
        idx, trd_day = args
        return (
            idx,
            process_single_day(
                trd_day,
                df_dict,
            ),
        )

    # Avoid nested parallelism: Polars/NumExpr/BLAS already use threads.
    # Default to sequential day processing; override via BARRA_MAX_WORKERS.
    import multiprocessing

    requested_workers = int(os.getenv("BARRA_MAX_WORKERS", "1"))
    max_workers = max(
        1, min(requested_workers, len(trd_days), multiprocessing.cpu_count())
    )
    __global_logger.info("Using %d workers for parallel processing", max_workers)

    if max_workers == 1:
        for i, day in tqdm(list(enumerate(trd_days)), desc="Processing factor data"):
            try:
                _, lf = process_day_task((i, day))
                multi_day_fac[i] = lf
                if (i + 1) % 5 == 0:
                    gc.collect()
                    log_memory_usage(f"After processing {i + 1} days")
            except Exception as e:
                __global_logger.error(f"Error processing day {i} ({day}): {e}")
                multi_day_fac[i] = pl.LazyFrame()
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_day_task, (i, day)): (i, day)
                for i, day in enumerate(trd_days)
            }
            for future in tqdm(
                as_completed(futures),
                total=len(trd_days),
                desc="Processing factor data",
            ):
                i, day = futures[future]
                try:
                    idx, lf = future.result()
                    multi_day_fac[idx] = lf
                    if (i + 1) % 5 == 0:
                        gc.collect()
                        log_memory_usage(f"After processing {i + 1} days")
                except Exception as e:
                    __global_logger.error(f"Error processing day {i} ({day}): {e}")
                    multi_day_fac[i] = pl.LazyFrame()

    # 优化：添加结果保存逻辑和最终内存清理
    log_memory_usage("Before saving results")

    # Write results day-by-day to keep memory bounded.
    # Use sink_ipc to avoid materializing large DataFrames in Python memory.
    for trd_day, lf in tqdm(
        list(zip(trd_days, multi_day_fac)),
        desc="Writing factor data",
    ):
        out_path = os.path.join(
            PATH_FAC_ROOT,
            f"{trd_day.year:04d}{trd_day.month:02d}{trd_day.day:02d}.feather",
        )
        try:
            # Polars will execute the lazy plan and stream results to disk.
            lf.sink_ipc(out_path, compression="zstd")
        except AttributeError:
            # Backward compatibility for older Polars versions.
            df = None
            try:
                df = lf.collect(engine="streaming")
                df.write_ipc(out_path, compression="zstd")
            except Exception as e:
                __global_logger.error(f"Error writing results for {trd_day}: {e}")
                continue
            finally:
                if df is not None:
                    del df
        except Exception as e:
            __global_logger.error(f"Error sinking results for {trd_day}: {e}")
            continue

        gc.collect()

    # 最终内存清理
    gc.collect()
    log_memory_usage("After processing completed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--step",
        choices=["base", "barra", "both"],
        default=os.getenv("BARRA_STEP", "base"),
    )
    parser.add_argument("--begin-year", default=os.getenv("BARRA_BEGIN_YEAR", "2021"))
    parser.add_argument("--end-year", default=os.getenv("BARRA_END_YEAR", "2025"))
    args = parser.parse_args()

    trd_days, _ = get_trade_days_pl(begin_year=args.begin_year, end_year=args.end_year)
    trd_days.sort()
    trd_days = [trd_day for trd_day in trd_days if trd_day <= datetime.now().date()]

    if args.step in {"base", "both"}:
        run_base_pipeline(trd_days)
    if args.step in {"barra", "both"}:
        run_barra_pipeline(trd_days)


if __name__ == "__main__":
    main()
