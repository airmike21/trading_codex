# Trading Codex

Systematic trading research scaffold with a minimal daily-bar backtest engine.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pytest
python scripts/run_backtest.py
```

## Workflow

Fetch -> cache -> backtest:

```bash
python scripts/fetch_bars.py --provider tastytrade --symbols AAPL MSFT --start 2023-01-01 --end 2023-12-31
python scripts/run_backtest.py
```

Data flow:
- `scripts/fetch_bars.py` asks a `DataSource` for bars.
- `LocalStore` writes symbol parquet files under `./data/`.
- Backtest code reads cached data and builds aligned panels.

## Notes

- `TastytradeDataSource` is scaffolded only; auth and API calls are intentionally not implemented yet.
- Signals are expected to use information available up to t-1 to avoid lookahead bias.
