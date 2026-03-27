# Stage 2 Paper Ops

Last updated: 2026-03-27

This is the narrow daily operations routine for the existing local Stage 2 paper-lane groundwork.
It exists so the primary live candidate can keep running forward cleanly, with retained evidence, without opening Stage 3 bench work or live broker work.
Under the clarified program definition, this routine is useful groundwork and retained-evidence infrastructure, but by itself it does not complete or exit Stage 2 because Stage 2 now requires one real persistent paper-execution lane, with IBKR PaperTrader as the preferred primary lane.

## Daily Command

WSL / Linux:

```bash
.venv/bin/python scripts/paper_lane_daily_ops.py --preset dual_mom_vol10_cash_core --provider stooq
```

This routine runs three steps in order and stops on the first failure:

1. `scripts/update_data_eod.py`
2. `scripts/paper_lane.py --emit json status --preset dual_mom_vol10_cash_core`
3. `scripts/paper_lane.py --emit json apply --preset dual_mom_vol10_cash_core`

It only updates local market data and local paper-lane state.
It does not place live broker orders.
It does not place external paper-execution orders through a broker or paper service.
It does not open Stage 3 work by default.

## What Runs Each Day

- The data update refreshes the symbols required by the selected preset.
- The paper-lane status step records the current paper state versus the latest target.
- The paper-lane apply step records the paper-lane action result for that same daily run.
- Every run writes retained machine-readable artifacts outside the repo working tree.
- The daily runner fails closed if any step exits non-zero or if a JSON-producing step returns invalid JSON.

## Artifact Locations

Trading Codex local state paths prefer:

1. `~/.trading_codex`
2. `~/.cache/trading_codex`
3. `/tmp/trading_codex`

The resolved archive root is whichever of those paths is available first.

Raw per-run machine-readable artifacts:

- `<archive_root>/runs/YYYY-MM-DD/<paper_lane_daily_ops_run_id>/manifest.json`
- `<archive_root>/runs/YYYY-MM-DD/<paper_lane_daily_ops_run_id>/artifacts/daily_ops_run.json`
- `<archive_root>/runs/YYYY-MM-DD/<paper_lane_daily_ops_run_id>/artifacts/update_data_eod.json`
- `<archive_root>/runs/YYYY-MM-DD/<paper_lane_daily_ops_run_id>/artifacts/paper_lane_status.json`
- `<archive_root>/runs/YYYY-MM-DD/<paper_lane_daily_ops_run_id>/artifacts/paper_lane_apply.json`

Persistent review log files:

- JSON log: `<archive_root>/stage2_paper_ops/primary_live_candidate_v1/paper_lane_daily_ops_log.jsonl`
- CSV log: `<archive_root>/stage2_paper_ops/primary_live_candidate_v1/paper_lane_daily_ops_runs.csv`
- Excel workbook: `<archive_root>/stage2_paper_ops/primary_live_candidate_v1/paper_lane_daily_ops_runs.xlsx`

Source of truth:

- Per-run JSON artifacts in the run archive
- The cumulative JSONL + CSV logs

Convenience artifact:

- The XLSX workbook is regenerated from the structured data so Excel review is easy, but JSON + CSV remain the durable source.
- A single-instance lock file lives at `<archive_root>/stage2_paper_ops/primary_live_candidate_v1/paper_lane_daily_ops.lock`.
  If a second scheduler launch starts while a run is active, it exits non-zero immediately and does not rewrite the cumulative JSONL/CSV/XLSX artifacts.

## Review Checkpoint

- The first operational review checkpoint is 20 market-day runs.
- That checkpoint is about ops reliability and retained evidence.
- It is not proof of strategy edge or full Stage 2 exit by itself.
- At that checkpoint, review whether the routine stayed clean, repeatable, and explainable for 20 market days in a row.

## Windows Task Scheduler

Schedule this only after the locking and Windows-path safety fixes in this slice are present on the promoted checkout you are using.
Do not point the scheduled job at a Builder worktree.
Point it at a separate promoted checkout that is synced to the promoted `origin/master` HOLD posture.

Use the repo-managed PowerShell wrapper:

- Wrapper path: `scripts/windows/trading_codex_stage2_daily_ops.ps1`
- The wrapper launches WSL and runs `scripts/paper_lane_daily_ops.py`.
- The repo does not auto-register the task for you from this slice.

Example Task Scheduler action:

- Program/script: `powershell.exe`
- Add arguments:

```powershell
-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "C:\path\to\promoted\trading_codex\scripts\windows\trading_codex_stage2_daily_ops.ps1" -WslRepoPath "/home/aarondaugherty/trading_codex" -Provider stooq
```

In that example, `/home/aarondaugherty/trading_codex` must be the promoted WSL checkout that matches the promoted `origin/master` operational lane.

You can inspect the exact WSL command before scheduling it:

```powershell
.\scripts\windows\trading_codex_stage2_daily_ops.ps1 -PrintOnly -WslRepoPath /home/aarondaugherty/trading_codex
```

## Existing Shadow Evidence Helper

If `/home/aarondaugherty/.local/bin/trading_codex_shadow_evidence.sh` exists locally, leave it separate.

Why it stays separate:

- it collects shadow-validation evidence, not paper-lane state/status/apply evidence
- it is local-only and not repo-managed
- chaining it into the Stage 2 paper-lane daily runner would mix two different review lanes and make failures harder to interpret

Use it only if you also want separate shadow evidence in addition to the Stage 2 paper-lane ops artifacts.

## Explain Like I Am 12

What is already finished:

- Stage 1 sandbox understanding is done.
- The repo has useful Stage 2 groundwork: a persistent local paper lane, a daily button for running it forward, and retained receipts.

What is not finished yet:

- Clarified Stage 2 is not done yet. It now requires one real persistent paper-execution lane with reviewable paper order, fill, scheduling, reconciliation, and restart behavior accumulating evidence over time.
- Stage 3 bench expansion is not the default next step.
- Live funding and a clean live account are later stages.
- Live broker trading is still later.

What the next move is:

- Keep running the local paper lane cleanly when it is useful as groundwork.
- Keep the evidence.
- After control-plane alignment, the next implementation move is a minimal IBKR PaperTrader operational acceptance path for the primary live candidate.
- Use tastytrade sandbox as secondary regression coverage for tastytrade-specific auth/account/order-flow behavior, not as the main Stage 2 paper lane.
- Review the first 20 market-day block for operational reliability, but do not treat that local checkpoint alone as full Stage 2 exit.

What the intended live target still is:

- tastytrade remains the intended live target unless evidence clearly justifies a change.
