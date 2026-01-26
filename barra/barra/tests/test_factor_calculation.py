from __future__ import annotations

from datetime import date
import os
import sys

import polars as pl

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))


def test_cal_hbeta_signs():
    from main import cal_hbeta

    dates = pl.Series(
        ["20210104", "20210105", "20210106", "20210107", "20210108"]
    ).str.to_date("%Y%m%d")

    idx_ret = pl.LazyFrame(
        {
            "date": dates,
            "国证A指": [0.0, 0.02, -0.02, 0.02, -0.02],
        }
    )
    stk_ret = pl.LazyFrame(
        {
            "date": dates,
            "stk1": [0.0, 0.02, -0.02, 0.02, -0.02],
            "stk2": [0.0, -0.02, 0.02, -0.02, 0.02],
        }
    )

    out = cal_hbeta(
        date(2021, 1, 8), {"idx_ret": idx_ret, "stk_ret": stk_ret}
    ).collect()
    assert set(out.columns) == {"sec_id", "HBETA"}
    assert out.height == 2

    stk1_beta = out.filter(pl.col("sec_id") == "stk1").get_column("HBETA")[0]
    stk2_beta = out.filter(pl.col("sec_id") == "stk2").get_column("HBETA")[0]
    assert stk1_beta > 0
    assert stk2_beta < 0


def test_cal_btop_shape():
    from main import cal_btop

    dates = pl.Series(["20210104", "20210105", "20210106"]).str.to_date("%Y%m%d")
    pb = pl.LazyFrame(
        {
            "date": dates,
            "stk1": [1.0, 2.0, 3.0],
            "stk2": [2.0, 4.0, 6.0],
        }
    )
    out = cal_btop(date(2021, 1, 6), {"stk_PB": pb}).collect()
    assert set(out.columns) == {"sec_id", "BTOP"}
    assert out.height == 2


def test_compute_barra_from_precomputed_base_uses_available_weights():
    from main import compute_barra_factors_from_base

    trd_day = date(2021, 1, 8)
    base_df = pl.DataFrame(
        {
            "sec_id": ["s1", "s2", "s3"],
            "DTOP": [1.0, 100.0, 2.0],
            "DTOPF": [1.0, None, 2.0],
            "SIZE": [1.0, 2.0, 3.0],
        }
    )

    industry_lf = pl.LazyFrame(
        {
            "date": [trd_day],
            "s1": [1],
            "s2": [1],
            "s3": [2],
        }
    )
    mv_lf = pl.LazyFrame(
        {
            "date": [trd_day],
            "s1": [10.0],
            "s2": [20.0],
            "s3": [30.0],
        }
    )

    out = compute_barra_factors_from_base(
        trd_day,
        base_df,
        industry_lf=industry_lf,
        mv_lf=mv_lf,
        winsor_p=0.0,
        do_orthogonalize=False,
        print_corr=False,
        do_check=False,
    )

    assert "DIVIDEND-YIELD" in out.columns
    # For s2, DTOPF is null, so composite equals centered DTOP (denom=0.5, numerator=0.5*DTOP)
    dtop_centered = base_df.get_column("DTOP") - base_df.get_column("DTOP").mean()
    s2_expected = dtop_centered[1]
    s2_got = out.filter(pl.col("sec_id") == "s2").get_column("DIVIDEND-YIELD")[0]
    assert s2_got == s2_expected
