# Daily Trading Runbook

Last updated: 2026-04-06

This runbook turns the documented preset behavior into a repeatable manual trading routine.
It is grounded in the audited runtime behavior from 2026-03-06 and the local preset layer captured during that audit.
Use `docs/PROJECT_STATE.md` for current project state. This file is a trading reference, not startup truth.

## Before You Start

- Use the repo venv path for commands: `~/trading_codex/.venv/bin/python`
- Treat `configs/presets.json` as local-only configuration. Do not commit it.
- The documented defensive asset across the verified presets in this runbook is `BIL`.
- In this sandboxed WSL environment, preset state and CSV logging currently point at `/tmp/trading_codex/...`.
- `daily_signal.py` prints exactly one line when it emits and prints nothing at all when there is no emit.

## Daily Checklist

1. Refresh local market data before checking signals.
   Prefer Tiingo when `TIINGO_API_KEY` is set, otherwise use Stooq.
2. Choose the preset you want to trade manually.
   Use `vm_core` for change-only value+momentum checks.
   Use `vm_core_due` when you want the same signal plus one due-date reminder.
   Use `dual_mom_core` for the documented dual momentum preset.
   An opt-in `dual_mom_core_vt` variant is available in `configs/presets.example.json` if you want the evaluated `0.12 / 21` vol-target overlay without changing the existing preset.
3. Run `daily_signal.py` for that preset once.
4. If stdout is empty, stop.
   That means there is no new emit for the current state.
5. If stdout contains one line, read the action, symbol, share count, price, and `next_rebalance`.
6. If the line implies a manual trade, execute and record it outside the repo.
7. If you want a spot check before trading, rerun with `--emit json` or run the matching `run_backtest.py --next-action-json` command.
8. If you rerun the same preset again immediately with unchanged data and state, expect no output.
   `vm_core_due` is the exception only when the rebalance date has become due and that due reminder has not been emitted yet.

## Command Cheat Sheet

Refresh preset symbols with Tiingo:

```bash
TIINGO_API_KEY=... ~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider tiingo
```

Refresh preset symbols with Stooq:

```bash
~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider stooq
```

Run the verified presets with their configured default emit mode:

```bash
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core_due
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset dual_mom_core
```

Run the opt-in dual momentum overlay variant from the tracked example preset file:

```bash
~/trading_codex/.venv/bin/python scripts/daily_signal.py --presets-file configs/presets.example.json --preset dual_mom_core_vt
```

Run the same presets with JSON output for inspection:

```bash
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core --emit json
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core_due --emit json
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset dual_mom_core --emit json
```

Spot-check the exact `run_backtest.py` payload for `vm_core` or `vm_core_due`:

```bash
~/trading_codex/.venv/bin/python scripts/run_backtest.py \
  --strategy valmom_v1 \
  --symbols SPY QQQ IWM \
  --vm-defensive-symbol BIL \
  --vm-mom-lookback 63 \
  --vm-val-lookback 126 \
  --vm-top-n 2 \
  --vm-rebalance 21 \
  --start 2015-01-02 \
  --end 2026-01-01 \
  --no-plot \
  --data-dir ~/trading_codex/data \
  --next-action-json
```

Spot-check the exact `run_backtest.py` payload for `dual_mom_core`:

```bash
~/trading_codex/.venv/bin/python scripts/run_backtest.py \
  --strategy dual_mom \
  --symbols SPY QQQ IWM EFA \
  --defensive BIL \
  --data-dir ~/trading_codex/data \
  --no-plot \
  --next-action-json
```

## How To Use The Output

- `HOLD`: no manual trade. The current target symbol and size are unchanged.
- `ROTATE`: sell the current holding and buy the new target holding. This includes rotating into `BIL`.
- `RESIZE`: keep the same symbol but adjust share count.
- `ENTER`: open a new position from cash.
- `EXIT`: close a position to cash.

The one-line text output is the fastest trading prompt.
Use JSON when you want the exact `event_id`, `next_rebalance`, or resize fields before acting.

## Practical Rules

- Do not expect duplicate emits from repeated identical runs.
- A blank line is not a valid no-op. The correct no-op is truly zero bytes.
- CSV logging appends only when an emit happens.
- `vm_core_due` can emit once when `today >= next_rebalance` even if the `event_id` is unchanged.
- Do not edit repo logic just to force alerts. Preserve the stdout and `event_id` contracts.

## Audit Snapshot Notes

These were true during the 2026-03-06 preset audit and should be treated as examples, not permanent promises:

- `vm_core` emitted `HOLD SPY` with `next_rebalance=2026-01-08`.
- `vm_core_due` had the same payload as `vm_core`.
- `dual_mom_core` emitted `HOLD EFA` with `next_rebalance=2026-03-31`.

## Scheduled Dual Compare

The Windows + WSL scheduled comparison layer runs two weekday tasks:

- `TradingCodex\morning_0825_dual_compare`
- `TradingCodex\afternoon_1535_dual_compare`

Each task runs all of the following in sequence:

- `dual_mom_core`
- `dual_mom_core_vt`
- `daily_summary.py --preset dual_mom_core --preset dual_mom_core_vt --emit json`

The scheduler writes durable artifacts under `~/.trading_codex/scheduled_runs/`:

- `logs/scheduled_runs.jsonl`: append-only machine log with timestamp, job name, preset, stdout line, stderr, exit code, and snapshot path.
- `logs/dual_mom_core_alerts.csv`: emit-only CSV rows for the base preset.
- `logs/dual_mom_core_vt_alerts.csv`: emit-only CSV rows for the vol-target preset.
- `snapshots/YYYY-MM-DD/*.json`: timestamped combined snapshots for each morning or afternoon run.
- `daily_reviews/YYYY-MM-DD_dual_compare.md`: same-day side-by-side review showing morning vs afternoon and both presets.
- `state/*.json`: isolated scheduled-run alert state so the weekday tasks do not depend on your ad hoc manual run history.

Create the tasks from Windows PowerShell:

```powershell
.\scripts\windows\install_dual_mom_compare_tasks.ps1
```

Print the exact `schtasks.exe` commands without creating anything:

```powershell
.\scripts\windows\install_dual_mom_compare_tasks.ps1 -PrintOnly
```

Manual reruns:

```powershell
.\scripts\windows\trading_codex_scheduled_dual_compare.ps1 -Window morning_0825
.\scripts\windows\trading_codex_scheduled_dual_compare.ps1 -Window afternoon_1535
```

Disable the tasks:

```powershell
schtasks.exe /Delete /TN "TradingCodex\morning_0825_dual_compare" /F
schtasks.exe /Delete /TN "TradingCodex\afternoon_1535_dual_compare" /F
```

Still not automated:

- No broker connection.
- No live order placement.
- No trade confirmation loop back into the repo.
