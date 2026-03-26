# Trading Codex Assistant Brief

Last updated: 2026-03-26

## Current State

- Promoted `origin/master` baseline: `01bf644668460fbfdeeeddc8c07a230c35a8957b` (`fix: reject stale paper lane marks`).
- Stage 1 bounded tastytrade sandbox work is complete and promoted at `ed91cb19f64f132a16a6c7ecf03a4c5323cee53f`.
- Stage 2 persistent paper lane work is complete and promoted at `origin/master`.
- Current priority is staged first-live program execution, not generic plumbing expansion.
- Known current blockers are operational rather than repo defects.
- Read these first before proposing work: `docs/FIRST_LIVE_PROGRAM.md`, `docs/FIRST_LIVE_EXIT_CRITERIA.md`, `docs/STRATEGY_REGISTRY.md`.
- First-live sequence remains coherent:
  1. Stage 1 bounded tastytrade sandbox work complete
  2. Stage 2 persistent paper lane complete
  3. explicit, justified Stage 3 bench work only if warranted
  4. Stage 4 funded clean live account
  5. Stage 5 one-strategy limited live
- Expected next move after this doc sync: operate the promoted persistent paper lane forward and otherwise HOLD unless a concrete repo defect appears.
- Current primary live candidate: simple long-only ETF trend/momentum with cash fallback; daily/weekly execution; whole shares initially; no options, no shorting, no leverage initially.
- Stage 1 sandbox capability command: `scripts/tastytrade_sandbox_capability.py` with slice notes in `docs/TASTYTRADE_SANDBOX_CAPABILITY.md`.
- Stage 2 paper lane command: `scripts/paper_lane.py` for durable local paper state, status/reconcile, and apply flow.

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
~/trading_codex/.venv/bin/python scripts/tastytrade_sandbox_capability.py --preset dual_mom_vol10_cash_core --emit json
~/trading_codex/.venv/bin/python scripts/paper_lane.py status --preset dual_mom_vol10_cash_core --emit json
~/trading_codex/.venv/bin/python -m pytest -q
```
