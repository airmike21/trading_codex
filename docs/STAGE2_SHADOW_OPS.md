# Stage 2 Shadow Ops

Last updated: 2026-04-09

This is the reference runbook for the bounded local-only Stage 2 shadow daily-ops lane.
Use `docs/PROJECT_STATE.md` for current stage status, blockers, and expected next move.
This doc covers the explicit config surface, retained artifacts, daily EOD scheduler surface, and automation/manual boundary only.
It does not open Stage 3, does not broaden the approved IBKR PaperTrader lane, and does not auto-write control-plane docs.
The control-plane policy for this lane is reusable across any explicitly opened/configured Stage 2 shadow strategy or pair, but the current promoted runtime mapping remains intentionally armed to the reopened `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` pair until a later manual slice changes that repo truth.

## Daily Command

WSL / Linux:

```bash
.venv/bin/python scripts/stage2_shadow_daily_ops.py --provider stooq
```

This runner stays fail-closed around pair selection and replay eligibility.
The runner still requires an explicit pair config and never guesses from docs.
The tracked repo config `configs/stage2_shadow_ops.json` is intentionally armed to the currently reopened pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`, so the command refreshes retained evidence for that local-only pair instead of producing an explicit retained no-op.

## Explicit Config Surface

Tracked default:

```json
{
  "schema_name": "stage2_shadow_ops_config",
  "schema_version": 1,
  "active_pair": {
    "pair_id": "primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed",
    "primary_strategy_id": "primary_live_candidate_v1",
    "shadow_strategy_id": "primary_live_candidate_v1_vol_managed",
    "local_replay": {
      "enabled": true,
      "state_key": "primary_live_candidate_v1_vol_managed_shadow_replay",
      "starting_cash": 100000.0
    }
  }
}
```

To return the runner to explicit fail-closed no-op behavior after a future manual control-plane change or in a local override:

```json
{
  "schema_name": "stage2_shadow_ops_config",
  "schema_version": 1,
  "active_pair": null
}
```

Boundaries for this config:

- It is the only automation input. The runner does not infer an active shadow pair from `docs/PROJECT_STATE.md` or `docs/STRATEGY_REGISTRY.md`.
- Stage 2 policy allows the same local-only recurring evidence workflow to be reused for any shadow strategy/pair that is explicitly opened and configured in the manual control plane.
- The current promoted runner/config validation remains bounded to `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`; repointing the tracked config or broadening runtime support is a later manual control-plane/repo slice, not something automation does itself.
- The tracked repo config currently arms that reopened pair; changing or clearing it remains a manual control-plane action.
- Local replay stays separate from the primary local paper lane by requiring its own `state_key`.
- If replay is enabled, the runner still fails closed unless the refreshed shadow review bundle reports `automation_decision: allow`.

## What Runs Each Day

When an explicit active pair is configured, the runner executes these steps in order and stops on the first failure:

1. `scripts/update_data_eod.py`
2. `scripts/stage2_shadow_compare.py`
3. optional `scripts/paper_lane.py init` for the separate shadow replay state if replay is enabled and the shadow replay state does not exist yet
4. optional `scripts/paper_lane.py status --signal-json-file ...`
5. optional `scripts/paper_lane.py apply --signal-json-file ...`

What this refreshes:

- EOD market data for the explicitly configured primary/shadow ETF universe
- retained shadow robustness artifacts, including parameter stability, subperiod tests, cost sensitivity, benchmark comparison, drawdown clustering, and walk-forward output
- primary-vs-shadow comparison artifacts and the shadow scoreboard
- optional local-only shadow replay evidence through the existing local paper-lane infrastructure

What this does not do:

- register or open a shadow strategy in `docs/STRATEGY_REGISTRY.md`
- change queue/state in `docs/PROJECT_STATE.md`
- auto-change `current_decision`
- promote any shadow strategy to paper
- replace the primary live candidate
- touch IBKR or tastytrade order paths

## Automation Boundary

Once a bounded shadow strategy/pair has been opened manually in the control plane and intentionally configured in `configs/stage2_shadow_ops.json` or a local override, the same repeatable daily EOD evidence-refresh workflow is the approved local-only automation surface in this lane:

- retained backtest refresh through the retained comparison/report pipeline
- retained walk-forward / robustness refresh through `scripts/stage2_shadow_compare.py`
- primary-vs-shadow comparison refresh
- shadow scoreboard/report refresh
- optional local-only shadow replay through the separate shadow replay paper state when `local_replay.enabled` is `true` and the refreshed bundle reports `automation_decision: allow`
- cumulative retained manifests, JSON artifacts, JSONL/CSV/XLSX logs, and summary output
- other recurring information-gathering steps appropriate to the explicitly opened/configured Stage 2 shadow target
- fail-closed locking, first-failure stop behavior, and explicit no-op behavior when no active pair is configured

Daily EOD automation in this lane means scheduled invocation of the existing local-only runner only.
It does not mean automatic control-plane changes.
In promoted repo truth today, the tracked config/runtime exercises that workflow for `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`; future explicitly opened/configured Stage 2 shadow targets may inherit the same local-only workflow only through a later manual control-plane/config update and any needed repo slice.

## Artifact Locations

Trading Codex local state paths prefer:

1. `~/.trading_codex`
2. `~/.cache/trading_codex`
3. `/tmp/trading_codex`

The resolved archive root is whichever of those paths is available first unless `--archive-root` overrides it.

Per-run machine-readable artifacts:

- `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/manifest.json`
- `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/artifacts/stage2_shadow_daily_ops_run.json`
- `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/artifacts/update_data_eod.json`
- `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/artifacts/stage2_shadow_compare.json`
- optional `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/artifacts/shadow_paper_lane_init.json`
- optional `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/artifacts/shadow_paper_lane_status.json`
- optional `<archive_root>/runs/YYYY-MM-DD/<stage2_shadow_daily_ops_run_id>/artifacts/shadow_paper_lane_apply.json`

Persistent cumulative review logs:

- JSON log: `<archive_root>/stage2_shadow_ops/<pair_id_or_unconfigured>/stage2_shadow_daily_ops_log.jsonl`
- CSV log: `<archive_root>/stage2_shadow_ops/<pair_id_or_unconfigured>/stage2_shadow_daily_ops_runs.csv`
- Excel workbook: `<archive_root>/stage2_shadow_ops/<pair_id_or_unconfigured>/stage2_shadow_daily_ops_runs.xlsx`

Retained comparison/report refresh:

- `<archive_root>/stage2_shadow_compare/<pair_id>/<as_of_date>/comparison_report.json`
- `<archive_root>/stage2_shadow_compare/<pair_id>/<as_of_date>/comparison_report.md`
- `<archive_root>/stage2_shadow_compare/<pair_id>/<as_of_date>/scoreboard.csv`
- `<archive_root>/stage2_shadow_compare/<pair_id>/<as_of_date>/candidate_reviews/...`
- `<archive_root>/stage2_shadow_compare/<pair_id>/<as_of_date>/candidate_signals/...`

Single-instance lock:

- `<archive_root>/stage2_shadow_ops/<pair_id_or_unconfigured>/stage2_shadow_daily_ops.lock`

If a second scheduler launch starts while a run is active, it exits non-zero immediately and does not rewrite the cumulative JSONL/CSV/XLSX artifacts.

## No-Op Behavior

If `active_pair` is `null`, the runner:

- exits `0`
- writes a retained run manifest outside the repo tree
- appends a cumulative JSONL/CSV/XLSX row with `overall_result=noop`
- records `no_op_reason=no_active_pair_configured`

This is intentional.
The runner never guesses a shadow target from docs or from the last completed shadow slice.

## Manual Boundary

These remain manual even when the daily EOD runner is scheduled:

- `docs/STRATEGY_REGISTRY.md`
- `docs/PROJECT_STATE.md`
- opening or reopening a shadow strategy/pair in the control plane
- queue ordering in `docs/PROJECT_STATE.md`
- the official `current_decision` decision gate
- any shadow -> paper promotion
- any primary-live-candidate replacement
- anything that would broaden into Stage 3 or the approved IBKR PaperTrader lane

The runner may compute and retain a suggested status in artifacts, but the official control plane stays manual.

## Windows Scheduler Surface

Schedule this only from a promoted checkout that includes the required shadow runner, wrapper, installer, and locking behavior.
Do not point the scheduled job at a Builder worktree.
Point it at a separate promoted checkout that is synced to the promoted `origin/master` state you intend to operate.

Use the repo-managed Windows entrypoints:

- Wrapper path: `scripts/windows/trading_codex_stage2_shadow_daily_ops.ps1`
- Task installer path: `scripts/windows/install_stage2_shadow_daily_ops_task.ps1`
- The wrapper launches WSL and runs `scripts/stage2_shadow_daily_ops.py`.
- The installer creates one weekday Task Scheduler job that invokes the staged local wrapper once per EOD window.
- The installer does not arm a pair, does not edit `configs/stage2_shadow_ops.json`, and does not change control-plane docs.

Inspect the exact WSL command before scheduling it:

```powershell
.\scripts\windows\trading_codex_stage2_shadow_daily_ops.ps1 -PrintOnly -WslRepoPath /home/aarondaugherty/trading_codex
```

Inspect the exact Task Scheduler install plan before registering it:

```powershell
.\scripts\windows\install_stage2_shadow_daily_ops_task.ps1 -PrintOnly -WslRepoPath /home/aarondaugherty/trading_codex
```

## Control-Plane Boundary

- This doc describes the local-only Stage 2 shadow ops lane and retained artifacts only.
- It does not replace `docs/PROJECT_STATE.md` as the current-state checkpoint.
- Current repo truth keeps the tracked runtime armed to one explicitly reopened pair, but Stage 2 policy allows the same local-only workflow to be reused for later explicitly opened/configured shadow targets without changing the manual control-plane boundary.
- Use `docs/FIRST_LIVE_PROGRAM.md` for stage policy and `docs/FIRST_LIVE_EXIT_CRITERIA.md` for stage gates.
