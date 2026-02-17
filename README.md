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

## Notes

- Signals are expected to use information available up to t-1 to avoid lookahead bias.
- The demo uses synthetic data only; no external data fetching is included yet.
