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

### Next-action alerts (one line only)
- `--next-action` prints exactly one human-readable line (for alerting).
- `--next-action-json` prints exactly one minified JSON line (for alerting / automation).

Example:
```bash
.venv/bin/python scripts/run_backtest.py --strategy dual_mom --symbols SPY QQQ IWM EFA --defensive TLT \
  --start 2005-01-01 --end 2005-05-02 --no-plot --next-action-json --vol-target 0.10 --vol-update rebalance
```

### Alerts: `next_action_alert.py` (prints only on change)
Use the repo venv + worktree source path:
```bash
PY=~/trading_codex/.venv/bin/python
export PYTHONPATH=$PWD/src
```

One-line JSON directly from backtest:
```bash
$PY scripts/run_backtest.py --strategy dual_mom_v1 --symbols SPY QQQ IWM EFA --dm-defensive-symbol TLT \
  --start 2005-01-01 --end 2005-05-02 --no-plot --next-action-json \
  --dm-lookback 252 --dm-top-n 1 --dm-rebalance 21 --data-dir ~/trading_codex/data
```

Canonical wrapper invocation (pass `run_backtest.py` args after `--`; no `--input` flag):
```bash
$PY scripts/next_action_alert.py --state-file /tmp/na_state.json --emit json -- \
  --strategy dual_mom_v1 --symbols SPY QQQ IWM EFA --dm-defensive-symbol TLT \
  --start 2005-01-01 --end 2005-05-02 --no-plot --dm-lookback 252 --dm-top-n 1 --dm-rebalance 21 \
  --data-dir ~/trading_codex/data
```
- First run prints one line.
- Second run prints nothing if `event_id` is unchanged.
- Add `--mode change_or_rebalance_due` to also emit once when `next_rebalance` is due, even if `event_id` is unchanged.

### Windows Alert Monitors
- Multi-monitor config example: `scripts/windows/trading_codex_alerts.example.json`
- Windows config location: `%USERPROFILE%\trading_codex_alerts.json`

On Windows PowerShell:
```powershell
Copy-Item .\scripts\windows\trading_codex_alerts.example.json "$env:USERPROFILE\trading_codex_alerts.json"
.\scripts\windows\generate_schtasks.ps1 -ConfigPath "$env:USERPROFILE\trading_codex_alerts.json"
```

The generator prints one `schtasks.exe /Create` command per monitor plus a matching `/Run` command for a quick test.

Keyed state files for the wrapper are stored under:
- `~/.cache/trading_codex/next_action_alert/`

Manual force-toast options:
1. Use a fresh `--state-key` value in monitor `next_action_args` (for example `--state-key force_test_123`) and run the task.
2. Or delete the matching keyed file under `~/.cache/trading_codex/next_action_alert/` and run again.
