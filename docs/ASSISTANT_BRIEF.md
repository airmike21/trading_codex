# Trading Codex Assistant Brief

Last updated: 2026-03-06

## Current State

- `origin/master` includes the daily runner presets flow and the EOD data updater.
- `scripts/update_data_eod.py` is merged, tested, and usable.
- `scripts/daily_signal.py` and `scripts/next_action_alert.py` are validated for one-line emit / zero-byte no-op behavior.
- The next implementation priority is accuracy: improve and validate strategy/backtest signal correctness, not more alert-runner plumbing unless a regression is found.

## Hard Invariants

- `scripts/run_backtest.py --next-action-json` must print exactly one line of minified JSON.
- `scripts/run_backtest.py --next-action` must print exactly one line of human text.
- `scripts/next_action_alert.py` must print one line only on emit and nothing if unchanged.
- `event_id` must remain:
  `"{date}:{strategy}:{action}:{symbol}:{target_shares}:{resize_new_shares}:{next_rebalance}"`

## Working Rules

- Do not commit directly to `master` unless explicitly instructed.
- Preserve intentional uncommitted Windows PS1 work in `scripts/windows/trading_codex_next_action_alert.ps1`.
- Keep `configs/presets.json` local-only, ignored, and uncommitted.
- For local presets in this environment, prefer cash-like defensive tickers in this order:
  `BIL`, `SGOV`, `SHY`, `IEF`, `TLT`.
- For local state/log paths, prefer `~/.trading_codex`, then `~/.cache/trading_codex`, then `/tmp/trading_codex`. In this sandboxed WSL session, `/tmp/trading_codex` is the writable fallback that works end-to-end.

## Known Good Commands

```bash
~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider stooq --verbose
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core_due --emit json
~/trading_codex/.venv/bin/python -m pytest -q
```

## Session Notes

- On 2026-03-06, `origin/feat/data-updater-eod-20260305_154519` was fast-forwarded into `origin/master` and full `pytest -q` passed in a `/tmp` validation worktree.
- In this environment, `~/.trading_codex` creation is blocked by sandbox permissions and writes under `~/.cache/trading_codex` are not permitted to the alert state/log path, so `/tmp/trading_codex` is the active local fallback for workflow verification.
