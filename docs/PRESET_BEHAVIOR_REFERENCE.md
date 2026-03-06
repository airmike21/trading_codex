# Preset Behavior Reference

This reference documents the verified production-style presets currently defined in local `configs/presets.json`.
It is based on the audited behavior from 2026-03-06 and the current preset arguments.

## Ground Rules

- Presets are loaded through `scripts/daily_signal.py`.
- Default output for all three verified presets is one line of human text.
- `--emit json` is available for spot checks and should still produce exactly one minified JSON line when it emits.
- Repeated identical runs should produce zero bytes unless a due-mode preset has become due and has not emitted that due reminder yet.
- CSV logging appends only on emit.

## vm_core

- Purpose: value+momentum core manual trading preset without due reminders.
- Strategy: `valmom_v1`
- Defensive asset: `BIL`
- Mode: `change_only`
- Default emit: `text`
- Expected emit behavior: emits on a new `event_id`
- Repeated identical run: zero bytes
- CSV behavior: appends one row only when an emit occurs
- When it should emit: the underlying `event_id` changes
- When it should not emit: same `event_id`, same state
- Important caveat: this preset does not emit just because `next_rebalance` is now due

Audit snapshot:

- Current payload during the audit: `HOLD SPY`
- Snapshot `next_rebalance`: `2026-01-08`
- First run: one line
- Second identical run: zero bytes

## vm_core_due

- Purpose: same strategy and target logic as `vm_core`, but with one due-date reminder
- Strategy: `valmom_v1`
- Defensive asset: `BIL`
- Mode: `change_or_rebalance_due`
- Default emit: `text`
- Expected emit behavior: emits on a new `event_id`, or once when the same signal becomes due
- Repeated identical run: zero bytes after the due reminder has already emitted
- CSV behavior: appends one row only when an emit occurs
- When it should emit:
  a new `event_id`
  or an unchanged `event_id` whose `next_rebalance` is now due and has not already emitted the due reminder
- When it should not emit:
  unchanged signal with no due transition
  or the same due reminder after it has already fired once
- Important caveat: due is based on the rebalance date in the payload, not on a new trade appearing

What "due" means in practice:

- If `today` in Chicago is at or past `next_rebalance`, `vm_core_due` can emit one reminder line even if the trade fingerprint is unchanged.
- The audited seeded-state check proved the real distinction:
  with the same saved `event_id`, `vm_core` emitted zero bytes while `vm_core_due` emitted once, then zero bytes on the next identical run.

Audit snapshot:

- Current payload during the audit: `HOLD SPY`
- Snapshot `next_rebalance`: `2026-01-08`
- First run: one line
- Second identical run: zero bytes

## dual_mom_core

- Purpose: dual momentum manual trading preset using the current configured strategy
- Strategy: `dual_mom`
- Defensive asset: `BIL`
- Mode: `change_only`
- Default emit: `text`
- Expected emit behavior: emits on a new `event_id`
- Repeated identical run: zero bytes
- CSV behavior: appends one row only when an emit occurs
- When it should emit: the underlying `event_id` changes
- When it should not emit: same `event_id`, same state
- Important caveat: this preset currently uses `dual_mom`, not `dual_mom_v1`

Audit snapshot:

- Current payload during the audit: `HOLD EFA`
- Snapshot `next_rebalance`: `2026-03-31`
- First run: one line
- Second identical run: zero bytes

## Important Distinctions

### vm_core vs vm_core_due

- `vm_core` is change-only.
- `vm_core_due` is change-or-rebalance-due.
- If you want fewer reminders, use `vm_core`.
- If you want a one-shot reminder once the rebalance date is due, use `vm_core_due`.

### dual_mom_core strategy choice

- `dual_mom_core` currently targets `dual_mom`.
- It does not currently target `dual_mom_v1`.
- That is a configuration fact, not an audited runtime bug.
- If you ever intend to trade the v1 strategy instead, change the local preset deliberately and re-audit it.

## State And Logging Notes

- `configs/presets.json` is local-only and must remain uncommitted.
- State and CSV paths are environment-specific.
- In this sandboxed WSL audit, the preset paths under `/tmp/trading_codex` were writable and behaved correctly.
- For a live environment, keep the paths writable and persistent enough for your workflow.
