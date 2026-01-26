import polars as pl
import numpy as np


def calc_rolling_ewma(series: pl.Expr, window: int, half_life: float) -> pl.Expr:
    """
    Calculate rolling EWMA with a finite window.

    Formula:
    EWMA_finite = (EWMA_inf - decay^window * EWMA_inf_lagged) / (1 - decay^window)

    where EWMA_inf is calculated with adjust=False (recursive).

    Note: This assumes the series has enough history (at least 'window' periods)
    for the approximation to be accurate in the region of interest.
    For t < window, this returns a value normalized by the full window weight,
    which effectively shrinks the value.
    """
    decay: float = np.exp(-np.log(2) / half_life)
    decay_window: float = decay**window

    # Calculate recursive EWMA (infinite history)
    ewma_inf = series.ewm_mean(half_life=half_life, adjust=False, min_samples=0)

    # Shift by window size
    ewma_inf_lagged = ewma_inf.shift(window)

    # Apply formula
    # If lagged value is null (start of series), treat as 0
    numerator = ewma_inf - decay_window * ewma_inf_lagged.fill_null(0)
    denominator = 1 - decay_window

    return numerator / denominator
