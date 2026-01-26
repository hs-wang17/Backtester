# GEMINI.md

## Project Overview

This project is a financial data analysis application written in Python. Its primary purpose is to calculate "Barra" style risk factors from raw financial market data.

The application is built using the `polars` library for high-performance data manipulation. It reads market data from Feather-formatted files, performs a series of transformations and calculations to derive various financial factors, and then saves these calculated factors back into Feather files.

The core logic is organized around `BaseFactor` and `BarraFactor` classes, which define the individual calculations and their composition into higher-level factors. The main entry point of the application processes data for specific trading days.

## Building and Running

### Dependencies

The project's dependencies are listed in `requirements.txt`. To install them, run:

```bash
pip install -r requirements.txt
```

Additionally, the test files indicate the use of `connectorx` for reading from a MySQL database. This may need to be installed separately if you intend to run those parts of the code:

```bash
pip install "connectorx[mysql]"
```

The project also heavily relies on `polars`, which should be installed as well.

### Running the Application

The main application can be run from the project root directory:

```bash
python src/main.py
```

This will execute the factor calculation process as defined in the `main()` function in `src/main.py`.

### Running Tests

The project uses `pytest` for testing. To run the tests, execute the following command from the project root directory:

```bash
pytest
```

## Development Conventions

### Code Style

The project uses `ruff` for linting and code formatting. The configuration can be found in the `pyproject.toml` file.

### Testing

Unit tests for factor calculations are located in `tests/test_factor_calculation.py`. These tests use mocked data to verify the correctness of the calculation logic. Other files in the `tests` directory appear to be used for exploratory analysis and data preparation.

### Configuration

Project configuration, including file paths for data input and output, and a MySQL database connection string, is managed in `src/config.py`. The project expects a specific directory structure and data files to be present at the configured paths.
