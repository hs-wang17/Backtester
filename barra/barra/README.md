# My Python Project

This is a typical Python project structure.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python src/main.py --step base
python src/main.py --step barra
python src/main.py --step both
```

### Barra factor options

- Disable daily checks: `BARRA_CHECK=0`
- Disable correlation printing: `BARRA_PRINT_CORR=0`
- Disable industry/SIZE orthogonalization: `BARRA_ORTHOGONALIZE=0`
- Winsorize percentile: `BARRA_WINSOR_P=0.01`
- Orthogonalization weight source: `BARRA_MV_TABLE=stk_neg_market_value` (WLS weights use `sqrt(stk_neg_market_value)`)

## Testing

```bash
pytest
```
