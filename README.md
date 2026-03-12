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

## Review Dashboard Launcher

The read-only Streamlit review dashboard can be launched from Windows while still
running inside a dedicated clean WSL workspace.

Recommended workspace:
- `~/.codex-workspaces/trading-review`

Do not point the launcher at `~/trading_codex`. The PowerShell wrapper refuses that path.

One-time setup inside the clean review workspace:

```bash
cd ~/.codex-workspaces/trading-review
python -m venv .venv
source .venv/bin/activate
pip install -e .[dashboard]
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$(wslpath -w "$PWD/scripts/windows/install_review_dashboard_shortcut.ps1")"
```

Daily use:
- Double-click the Windows desktop shortcut `Trading Codex Review Hub`.

Direct launcher command:

```bash
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$(wslpath -w "$PWD/scripts/windows/trading_codex_review_dashboard.ps1")"
```

Behavior:
- Runs `scripts/review_dashboard.py` from the clean WSL review workspace.
- Binds Streamlit only to `127.0.0.1` on port `8501` by default.
- Reuses only a launcher-owned dashboard instance for the same repo and port.
- Refuses to reuse a different app already occupying that port, even if it is another healthy Streamlit server.
- Fails clearly if the configured workspace is missing, dirty, or points at `~/trading_codex`.

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

### Compatibility Note
- Older branches may not include newer CLI flags (for example `next_action_alert.py --mode`) or newer strategy names.
- If you hit argparse `unrecognized arguments` or `invalid choice` errors, update to `origin/master` (or merge it into your branch).

### Windows Alert Monitors
- Multi-monitor config example: `scripts/windows/trading_codex_alerts.example.json`
- Windows config location: `%USERPROFILE%\trading_codex_alerts.json`
- Launcher supports optional `-Mode change_only` (default) or `-Mode change_or_rebalance_due`.
- Launcher also supports optional lock knobs (`-NoLock`, `-LockTimeoutSeconds`, `-LockStaleSeconds`) forwarded to `next_action_alert.py`.
- `next_action_alert.py` uses best-effort lockfiles plus atomic state writes. If a lock is held and not stale, it exits silently with no output.

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

## Daily runner and presets

Use `scripts/daily_signal.py` to run `next_action_alert.py` through a named preset.

- Prints exactly one line only when an alert emits.
- Prints nothing when there is no emit (no blank line).
- Presets load from `configs/presets.json` (local, gitignored) if present, else `configs/presets.example.json`.

Examples:

```bash
python scripts/daily_signal.py --preset vm_core
python scripts/daily_signal.py --preset vm_core --emit json --log-csv ~/.trading_codex/alerts.csv
python scripts/daily_signal.py --preset vm_core --mode change_or_rebalance_due
```
**Recommended invocation (ensures repo venv + deps):**

```bash
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core
```

## Data Updates (EOD)

If a required ticker is missing in your local store (for example `SHY`), `daily_signal` and `run_backtest` can fail.
Use `scripts/update_data_eod.py` to refresh `LocalStore` parquet files.

Tiingo is recommended (requires `TIINGO_API_KEY`):

```bash
TIINGO_API_KEY=... ~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider tiingo --symbols SPY QQQ IWM BIL
```

Stooq is the free fallback:

```bash
~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider stooq --symbols SPY QQQ IWM BIL
```

If `--symbols` is omitted, the script infers symbols from `configs/presets.json` (or `configs/presets.example.json`).
