# Trading Codex Assistant Brief

Last updated: 2026-03-25

## Current State

- Promoted `origin/master` baseline: `0723b45a1a0b9dca725fd03117643deb7df641f6` (`fix: block ungated live canary submits`).
- The prior live-canary release-gate slice is complete and promoted.
- Current priority is staged first-live program execution, not generic plumbing expansion.
- Known current blockers are operational rather than repo defects.
- Read these first before proposing work: `docs/FIRST_LIVE_PROGRAM.md`, `docs/FIRST_LIVE_EXIT_CRITERIA.md`, `docs/STRATEGY_REGISTRY.md`.
- Expected next implementation sequence:
  1. bounded tastytrade sandbox completion
  2. persistent paper broker lane
  3. additional strategies in shadow/paper
  4. funded clean tastytrade account
  5. one-strategy limited live
- Current primary live candidate: simple long-only ETF trend/momentum with cash fallback; daily/weekly execution; whole shares initially; no options, no shorting, no leverage initially.

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
- Tastytrade remains the live target unless evidence clearly justifies change.
- Prefer the meaningful next move that closes the current first-live stage over lateral expansion.
- For local presets in this environment, prefer cash-like defensive tickers in this order:
  `BIL`, `SGOV`, `SHY`, `IEF`, `TLT`.
- For local state/log paths, prefer `~/.trading_codex`, then `~/.cache/trading_codex`, then `/tmp/trading_codex`. In this sandboxed WSL session, `/tmp/trading_codex` is the writable fallback that works end-to-end.

## Known Good Commands

```bash
~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider stooq --verbose
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core_due --emit json
~/trading_codex/.venv/bin/python -m pytest -q
```
