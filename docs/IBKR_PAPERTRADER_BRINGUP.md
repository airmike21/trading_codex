# IBKR PaperTrader Bring-Up

Last updated: 2026-03-30

This is the narrow Stage 2 bring-up / acceptance path for `primary_live_candidate_v1`.
It exists to prove the preferred IBKR PaperTrader lane is reachable, explicitly paper-verified, and operationally reviewable without opening Stage 3, daily scheduler work, or broad broker abstraction.

It reuses the existing `scripts/ibkr_paper_lane.py` status/apply machinery.
By default it is fail-closed and no-write at the broker.

## Purpose

Use `scripts/ibkr_paper_bringup.py` to:

- resolve the current signal from `--preset` or `--signal-json-file`
- resolve the allowed ETF universe
- run a safe no-write acceptance check first
- optionally attempt a real IBKR PaperTrader apply only behind an unmistakable opt-in flag
- retain machine-readable evidence plus a short human-readable summary outside the repo tree

## Prerequisites / External Blockers

- IBKR Client Portal / Web API must be running and reachable.
- The configured account must be the IBKR PaperTrader `DU...` account you intend to use for Stage 2.
- The account/session must be explicitly reported as paper by the IBKR account-selection metadata.
- If you use `--preset`, the preset must resolve cleanly through `run_backtest --next-action-json`.
- If you use `--signal-json-file`, you must also pass `--allowed-symbols`.

If any of those are not true, treat the result as a Stage 2 HOLD rather than more coding.

## First-Run Commands

Safe no-write preflight:

```bash
.venv/bin/python scripts/ibkr_paper_bringup.py \
  --mode preflight \
  --preset dual_mom_vol10_cash_core \
  --ibkr-account-id DU1234567 \
  --emit text
```

No-write status acceptance:

```bash
.venv/bin/python scripts/ibkr_paper_bringup.py \
  --mode status \
  --preset dual_mom_vol10_cash_core \
  --ibkr-account-id DU1234567 \
  --emit json
```

Explicit paper-order apply attempt:

```bash
.venv/bin/python scripts/ibkr_paper_bringup.py \
  --mode apply \
  --enable-ibkr-paper-apply \
  --preset dual_mom_vol10_cash_core \
  --ibkr-account-id DU1234567 \
  --emit text
```

Existing signal JSON variant:

```bash
.venv/bin/python scripts/ibkr_paper_bringup.py \
  --mode preflight \
  --signal-json-file /path/to/next_action.json \
  --allowed-symbols EFA,BIL,SPY,QQQ,IWM \
  --ibkr-account-id DU1234567 \
  --emit json
```

Notes:

- `preflight` and `status` are broker no-write modes.
- `apply` is refused unless `--enable-ibkr-paper-apply` is present.
- `--confirm-replies` is apply-only and stays fail-closed by default if omitted.

## Evidence Written

Trading Codex archive roots prefer:

1. `~/.trading_codex`
2. `~/.cache/trading_codex`
3. `/tmp/trading_codex`

Each bring-up run writes a standard archive bundle outside the repo tree:

- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_bringup_acceptance_run_id>/manifest.json`
- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_bringup_acceptance_run_id>/artifacts/bringup_report.json`
- `<archive_root>/runs/YYYY-MM-DD/<ibkr_paper_bringup_acceptance_run_id>/artifacts/bringup_summary.txt`

The archived `bringup_report.json` includes:

- whether the run was `no_write` or `write_enabled`
- whether the lane was reachable
- whether the account/session was explicitly paper-verified
- whether the lane was blocked
- drift / duplicate / pending-claim state
- the resolved signal and allowed-symbol universe
- the reused `status_payload` and `apply_payload`
- references to the underlying `ibkr_paper_lane` archive manifests when available

The reused IBKR paper-lane state and ledger still live under the normal IBKR paper-lane base dir reported in the payload.

## Successful Bring-Up

Count the bring-up as successful when the command exits `0` and the archived report shows:

- `overall_status=ok`
- `lane_reachable=true`
- `paper_account_verified=true`
- `lane_blocked=false`
- `blocking_reasons=[]`
- retained manifest/report/summary paths were written

For no-write bring-up, drift may be present. That is acceptable and should be reviewed, not hidden.

For explicit apply, success means:

- `apply_result` is `applied` or `applied_noop`
- no pending claim remains
- no duplicate-event block fired

## HOLD Instead Of More Coding

Stop and HOLD for operations instead of opening more repo work when:

- IBKR access, account setup, or paper service availability is the blocker
- the run says the account/session was not explicitly verified as paper
- duplicate-event state is already present for the current signal
- a pending submit claim exists and requires manual clearance
- the execution plan is blocked, for example by unmanaged positions or other account drift
- the remaining work is forward evidence accumulation, not a repo defect

## Boundaries

- No scheduler or Windows task work in this slice
- No broad broker abstraction
- No Stage 3 bench expansion
- No live-account promotion
