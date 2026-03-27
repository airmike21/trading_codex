# Trading Codex Assistant Brief

Last updated: 2026-03-27

## Current State

- Promoted `origin/master` baseline: `a48d815f5e15e2d8dc50f0a098a02bf72d1b3942` (`fix: harden stage2 daily ops scheduling`).
- Prior promoted docs sync: `95cf8e5095c8ea4deafb9793de14016a340a76b5` (`docs: sync control plane after stage2 paper lane`).
- Stage 1 bounded tastytrade sandbox work is complete and promoted at `ed91cb19f64f132a16a6c7ecf03a4c5323cee53f`.
- Stage 2 is reopened under the clarified program definition: promoted master has useful local persistent paper-lane groundwork and daily ops evidence infrastructure, but not yet one real persistent paper-execution lane.
- Current priority is to close clarified Stage 2, not generic plumbing expansion.
- Known current gap is no longer Stage 1 ambiguity; it is the absence of one real persistent paper-execution lane for the primary live candidate.
- Read these first before proposing work: `docs/FIRST_LIVE_PROGRAM.md`, `docs/FIRST_LIVE_EXIT_CRITERIA.md`, `docs/STRATEGY_REGISTRY.md`.
- First-live sequence remains coherent:
  1. Stage 1 bounded tastytrade sandbox work complete
  2. Stage 2 one real persistent paper-execution lane reopened/in progress
  3. explicit, justified Stage 3 bench work only if warranted
  4. Stage 4 funded clean live account
  5. Stage 5 one-strategy limited live
- Expected next move after this doc sync: choose/build one real persistent paper-execution lane for `primary_live_candidate_v1` that best preserves parity with the eventual live path when practical. The existing local paper lane and daily ops routine remain useful groundwork and retained-evidence infrastructure, but are not alone sufficient for Stage 2 exit.
- Current primary live candidate: simple long-only ETF trend/momentum with cash fallback; daily/weekly execution; whole shares initially; no options, no shorting, no leverage initially.
- Stage 1 sandbox capability command: `scripts/tastytrade_sandbox_capability.py` with slice notes in `docs/TASTYTRADE_SANDBOX_CAPABILITY.md`.
- Local Stage 2 groundwork command: `scripts/paper_lane.py` for durable local paper state, status/reconcile, and apply flow.
- Local Stage 2 daily ops evidence routine: `scripts/paper_lane_daily_ops.py` with retained artifact locations and scheduling notes in `docs/STAGE2_PAPER_OPS.md`.

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
- Do not reopen Stage 3 by default while clarified Stage 2 remains open.
- For local presets in this environment, prefer cash-like defensive tickers in this order:
  `BIL`, `SGOV`, `SHY`, `IEF`, `TLT`.
- For local state/log paths, prefer `~/.trading_codex`, then `~/.cache/trading_codex`, then `/tmp/trading_codex`. In this sandboxed WSL session, `/tmp/trading_codex` is the writable fallback that works end-to-end.

## Known Good Commands

```bash
~/trading_codex/.venv/bin/python scripts/update_data_eod.py --provider stooq --verbose
~/trading_codex/.venv/bin/python scripts/daily_signal.py --preset vm_core_due --emit json
~/trading_codex/.venv/bin/python scripts/tastytrade_sandbox_capability.py --preset dual_mom_vol10_cash_core --emit json
~/trading_codex/.venv/bin/python scripts/paper_lane.py --emit json status --preset dual_mom_vol10_cash_core
~/trading_codex/.venv/bin/python scripts/paper_lane.py --emit json apply --preset dual_mom_vol10_cash_core
~/trading_codex/.venv/bin/python scripts/paper_lane_daily_ops.py --preset dual_mom_vol10_cash_core --provider stooq
~/trading_codex/.venv/bin/python -m pytest -q
```
