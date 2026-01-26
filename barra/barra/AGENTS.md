# Repository Guidelines

## Project Structure & Module Organization

- `src/`: Barra style factor calculation code.
  - `src/main.py`: core factor definitions (`BaseFactor`, `BarraFactor`) and factor calculators; writes outputs and logs to `polars_errors.log`.
  - `src/data.py`: trading-day calendars / data loading helpers.
  - `src/prepare_fin_data.py`: pulls financial statement data (MySQL) used by some factors.
  - `src/config.py`: paths and runtime configuration (data roots, output root, DB URI).
- `tests/`: `pytest` tests (mostly unit-style with mocked `polars` data). Notebooks (`*.ipynb`) are exploratory.

## Build, Test, and Development Commands

- `pip install -r requirements.txt`: install minimal dev dependencies (currently only `pytest`).
- `pip install polars numpy statsmodels tqdm`: install common runtime deps used by `src/main.py`.
- `pip install "connectorx[mysql]"`: optional, for MySQL-backed workflows/tests.
- `pip install -e .`: install the package in editable mode (uses `pyproject.toml` + setuptools).
- `python src/main.py`: run the factor pipeline (expects data paths in `src/config.py` to exist).
- `pytest`: run the full test suite (`pyproject.toml` sets `testpaths=["tests"]`).
- `ruff check src/ tests/`: lint.
- `ruff format src/ tests/`: auto-format.
- `pyright`: optional type checks (`typeCheckingMode = "basic"`).

## Coding Style & Naming Conventions

- Python: 4-space indentation; prefer type hints for public functions and factor outputs.
- Formatting/linting: use `ruff` (configured in `pyproject.toml`, line length 88; `E501` ignored).
- Naming: factor calculators commonly use `cal_<factor>()` (e.g., `cal_hbeta`); tests follow `test_cal_<factor>()`.

## Testing Guidelines

- Prefer deterministic unit tests using small `polars.LazyFrame` fixtures.
- Avoid hitting external systems by default (MySQL/feather mounts). If you add integration tests, clearly gate them (e.g., via `pytest` markers and environment variables).

## Commit & Pull Request Guidelines

- Commits: keep subjects short and descriptive; English or Chinese is fine (history is mixed). Example: `daily: update factor preprocessing`.
- PRs: describe the change, how to run it, and include `pytest` + `ruff` results. Don’t commit large data/log artifacts (e.g., `*.feather`, `*.pkl`, `polars_errors.log`).

## Configuration & Data Notes

- `src/config.py` contains machine-specific paths (often under `/mnt/...`) and a `MYSQL_URI`; adjust locally and avoid committing credentials.
- `site.cfg` may provide BLAS/LAPACK tuning for specific environments; only edit it if you understand the runtime impact.
