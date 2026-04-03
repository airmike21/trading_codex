# Stage 2 IBKR Paper Ops

Last updated: 2026-04-03

This is the narrow daily operations routine for the clarified Stage 2 IBKR PaperTrader lane.
It exists so `primary_live_candidate_v1` can keep running through the real IBKR PaperTrader control plane with retained forward evidence, while keeping scope limited to one strategy, long-only ETFs, whole shares, and daily/weekly execution.
This slice advances Stage 2, but it does not claim Stage 2 is complete.

## Daily Command

WSL / Linux:

```bash
.venv/bin/python scripts/ibkr_paper_lane_daily_ops.py --preset dual_mom_vol10_cash_core --provider stooq
```

This routine runs three steps in order and stops on the first failure:

1. `scripts/update_data_eod.py`
2. `scripts/ibkr_paper_lane.py --emit json status --preset dual_mom_vol10_cash_core`
3. `scripts/ibkr_paper_lane.py --emit json apply --preset dual_mom_vol10_cash_core`

It reuses the existing IBKR PaperTrader lane CLI.
It does not add generalized broker abstraction.
It does not open Stage 3 bench work.
It does not turn the shadow no-submit lane into the main Stage 2 runner.

## Review Command

Text summary:

```bash
.venv/bin/python scripts/ibkr_paper_ops_review.py --emit text
```

Machine-readable summary:

```bash
.venv/bin/python scripts/ibkr_paper_ops_review.py --emit json --limit 20
```

Optional overrides:

- `--archive-root` uses the same durable archive-root policy as the daily runner
- `--state-key` defaults to `primary_live_candidate_v1`
- `--limit` bounds the number of most-recent runs inspected without mutating any artifacts

This review command is the read-only operator surface for the retained IBKR PaperTrader forward-evidence lane.
It reads the cumulative JSONL log as the source of truth, checks the paired CSV and XLSX artifacts,
and summarizes at least:

- total runs inspected, ok count, failed count
- latest run timestamp, latest overall result, and latest failed step
- latest signal context: date, action, symbol, target shares, next rebalance, and `event_id`
- pending-claim count, duplicate-blocked count, and submitted-order totals
- latest `successful_signal_days_recorded`
- whether the 20-market-day review checkpoint is reached
- cumulative artifact presence and JSONL/CSV row-count consistency
- latest-run and inspected-history manifest/path presence checks
- explicit `review_status` and `attention_flags` when an operator should investigate

## Preconditions

- The IBKR PaperTrader Web API bridge or Client Portal Gateway must already be running locally.
- The PaperTrader session must already be authenticated.
- Set `IBKR_PAPER_ACCOUNT_ID` outside the repo, or pass `--ibkr-account-id` directly to the runner.
- Keep account identifiers, secrets, and credentials out of repo files.

## What Runs Each Day

- The data update refreshes the ETF symbols required by the selected preset.
- The IBKR status step records the latest signal, broker reconciliation state, and pending-claim context before apply.
- The IBKR apply step records the apply result, including duplicate refusal, pending claim, or event receipt outcomes when present.
- Every run writes retained machine-readable artifacts outside the repo working tree.
- The daily runner fails closed if any step exits non-zero or if a JSON-producing step returns invalid JSON.

## Artifact Locations

Trading Codex local state paths prefer:

1. `~/.trading_codex`
2. `~/.cache/trading_codex`
3. `/tmp/trading_codex`

The resolved archive root is whichever of those paths is available first unless `--archive-root` overrides it.

Raw per-run machine-readable artifacts:

- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_lane_daily_ops_run_id>/manifest.json`
- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_lane_daily_ops_run_id>/artifacts/ibkr_paper_lane_daily_ops_run.json`
- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_lane_daily_ops_run_id>/artifacts/update_data_eod.json`
- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_lane_daily_ops_run_id>/artifacts/ibkr_paper_lane_status.json`
- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_lane_daily_ops_run_id>/artifacts/ibkr_paper_lane_apply.json`

Persistent forward-evidence review files:

- JSON log: `<archive_root>/stage2_ibkr_paper_ops/primary_live_candidate_v1/ibkr_paper_lane_daily_ops_log.jsonl`
- CSV log: `<archive_root>/stage2_ibkr_paper_ops/primary_live_candidate_v1/ibkr_paper_lane_daily_ops_runs.csv`
- Excel workbook: `<archive_root>/stage2_ibkr_paper_ops/primary_live_candidate_v1/ibkr_paper_lane_daily_ops_runs.xlsx`

Single-instance lock:

- `<archive_root>/stage2_ibkr_paper_ops/primary_live_candidate_v1/ibkr_paper_lane_daily_ops.lock`

If a second scheduler or manual launch starts while a run is active, it exits non-zero immediately and does not rewrite the cumulative JSONL, CSV, or XLSX artifacts.

## Review Fields Captured

The cumulative log captures enough structure to make forward evidence operationally useful, including:

- `timestamp_chicago`, `run_id`, `preset`, `provider`, `state_key`
- data update exit status and updated-symbol count
- signal date, action, symbol, target shares, next rebalance, and `event_id`
- IBKR status fields such as `submission_ready`, `drift_present`, `event_already_applied`, `event_claim_pending`, trade-required count, and execution blockers
- pending-claim summary fields when present
- IBKR apply fields such as `result`, `duplicate_event_blocked`, `event_claim_pending`, `event_claim_path`, `event_receipt_path`, and submitted broker order ids
- archive manifest paths for the status step, apply step, and daily ops run
- durable local state paths such as the IBKR paper state file, ledger, event-receipts directory, and pending-claims directory

The per-run JSON archive remains the detailed source of truth.
The cumulative JSONL and CSV files are the durable forward-evidence rollup.
The XLSX workbook is regenerated from the structured data for convenient review.
`scripts/ibkr_paper_ops_review.py` is the intended read-only surface for reviewing these artifacts over time.

## Review Checkpoint

- The first operational review checkpoint is 20 market-day runs.
- That checkpoint is about IBKR PaperTrader operational reliability and retained evidence.
- It is not proof of strategy edge.
- It is not, by itself, Stage 2 exit.
- Use `scripts/ibkr_paper_ops_review.py` to judge whether the retained lane has reached 20 successful signal days,
  whether recent run health stayed clean, and whether pending claims, duplicate refusals, or artifact inconsistencies need attention before calling the checkpoint operationally meaningful.

## Explain Like I Am 12

What this runner does:

- refreshes market data
- checks what the strategy wants versus what IBKR PaperTrader already shows
- tries the narrow paper apply step
- saves receipts and logs so later review is not guesswork

What it does not do:

- it does not declare Stage 2 complete
- it does not open live trading
- it does not broaden into multiple strategies
- it does not replace the manual claim-review workflow when IBKR apply fails closed
