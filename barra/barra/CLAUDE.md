# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Barra risk factor calculation system for Chinese A-shares market. Calculates style factors (Beta, Size, Momentum, Value, etc.) for quantitative portfolio risk management.

## Commands

```bash
# Install
pip install -r requirements.txt

# Run
python src/main.py

# Test
pytest                              # All tests
pytest tests/test_factor_calculation.py::test_cal_hbeta  # Single test

# Lint
ruff check src/ tests/
ruff format src/ tests/
```

## Architecture

### Core Pattern: Two-Level Factor Composition

**BaseFactor** (`src/main.py`): Atomic calculation unit

- Takes name, data dict, and calc function
- Returns `pl.LazyFrame` with `stk_id` and factor value

**BarraFactor**: Weighted composite of BaseFactors

- Combines multiple BaseFactors with weights into final Barra factor

### Data Flow

1. Load feather files from `PATH_DAILY_DATA` as `pl.LazyFrame`
2. Filter to trading day via `prepare_trd_day_data_dict()`
3. Execute lazy factor calculations
4. Save results to `PATH_FAC_ROOT`

### Data Conventions

- Date strings as `YYYYMMDD`, convert via `str.to_date("%Y%m%d")`
- Source index column `__index_level_0__` renamed to `date`
- Input: wide format (date rows × stock columns)
- Output: long format (`stk_id`, factor_name)

### Configuration

`src/config.py` contains all paths:

- `PATH_DAILY_DATA`: Input data
- `PATH_FAC_ROOT`: Output factors
- `MYSQL_URI`: Database connection

### Key Dependencies

- **polars**: DataFrame operations (LazyFrame for deferred execution)
- **connectorx**: MySQL connectivity
