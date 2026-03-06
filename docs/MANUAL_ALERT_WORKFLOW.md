# Manual Alert Workflow

This guide explains how to interpret and use the verified preset alerts without breaking the underlying contracts.

## What The Alert Line Means

Default preset output is one human-readable line:

```text
YYYY-MM-DD | strategy | ACTION | SYMBOL | shares or resize | price | next=YYYY-MM-DD
```

Interpret it as a manual trading prompt, not an auto-execution order.

## Action Vocabulary

- `ENTER`: open a new position from cash
- `EXIT`: close a position to cash
- `ROTATE`: switch from the current symbol to a different symbol
- `HOLD`: no manual trade; keep the current target
- `RESIZE`: keep the same symbol but change position size

Important note for the current presets:

- Because `BIL` is the configured cash-like defensive, a defensive move is usually a `ROTATE` into `BIL`, not an `EXIT` to literal cash.

## next_rebalance

- `next_rebalance` is the next scheduled rebalance date derived from the strategy configuration.
- Treat it as the next date when the signal is expected to be reviewed again.
- It is not a promise that a new trade will appear on that date.

## event_id

The alert fingerprint is:

```text
{date}:{strategy}:{action}:{symbol}:{target_shares}:{resize_new_shares}:{next_rebalance}
```

How to use it:

- If the `event_id` is unchanged, a `change_only` preset should not emit again.
- `vm_core_due` can still emit once when the same event becomes due.
- If you are debugging a repeated alert, compare the full `event_id` before assuming the signal changed.

## Manual Trading Workflow

1. Update EOD data first.
2. Run the preset you care about.
3. If there is no output, stop. No new manual action is being emitted.
4. If one line appears, read the action and decide whether it implies a trade.
5. If needed, rerun with `--emit json` to inspect `event_id`, `next_rebalance`, and resize fields.
6. Execute the trade manually outside the repo.
7. Record the fill and any notes in your own trading journal.
8. If you rerun the same preset immediately, expect no duplicate output unless due-mode rules apply.

## When To Rerun

- Rerun after a data refresh.
- Rerun after a manual trade if you want to confirm no duplicate emit.
- Rerun with `--emit json` when you need higher-confidence inspection before placing a trade.

Avoid rerunning just to force another alert.
The correct no-op behavior is zero bytes, not a blank line.

## Safety Notes

- `configs/presets.json` is local-only and must remain uncommitted.
- State and CSV log paths may differ by environment.
- In this audit environment, `/tmp/trading_codex/...` worked, but that is an environment choice, not a repo-wide promise.
- CSV logging appends only on emit.
- Do not change repo-tracked signal logic casually.
  Preserve the one-line stdout contracts and the exact `event_id` composition.
- Preserve uncommitted local Windows launcher work in `scripts/windows/trading_codex_next_action_alert.ps1`.
