# Trading Codex Assistant Brief

Last updated: 2026-04-06

This brief is the durable operating contract for assistant work in Trading Codex.
It defines stable rules, control-plane ownership, and source-of-truth order.
Use `docs/PROJECT_STATE.md` for live state.

## Source Of Truth Order

1. Repo code, tracked configs, and committed scripts when runtime behavior is in question.
2. `docs/PROJECT_STATE.md` for current project state, active slice status, blockers, and expected next move.
3. `docs/FIRST_LIVE_PROGRAM.md` for staged program policy and first-live sequencing.
4. `docs/FIRST_LIVE_EXIT_CRITERIA.md` for stage completion, hold, and live gates.
5. `docs/STRATEGY_REGISTRY.md` for strategy roles, maturity, and promotion status.
6. `docs/WORKFLOW.md` for role boundaries and handoff rules.
7. `docs/PROMOTION_RUNBOOK.md` for exact promotion procedure.
8. Lane-specific runbooks and reference docs for task-local commands and artifacts.

## Durable Invariants

- `scripts/run_backtest.py --next-action-json` must print exactly one line of minified JSON.
- `scripts/run_backtest.py --next-action` must print exactly one line of human text.
- `scripts/next_action_alert.py` must print one line only on emit and nothing if unchanged.
- `event_id` must remain `"{date}:{strategy}:{action}:{symbol}:{target_shares}:{resize_new_shares}:{next_rebalance}"`.

## Standing Program Rules

- Do not commit directly to `master` unless explicitly instructed.
- Preserve intentional uncommitted Windows PS1 work in `scripts/windows/trading_codex_next_action_alert.ps1`.
- Keep `configs/presets.json` local-only, ignored, and uncommitted.
- Tastytrade remains the intended live target unless evidence clearly justifies change.
- IBKR PaperTrader is the approved primary persistent paper-execution lane for Stage 2.
- tastytrade sandbox remains secondary regression coverage for tastytrade-specific auth/account/order-flow behavior on the intended live path.
- Choose the next move that closes the current stage instead of reopening later stages early.
- Do not reopen Stage 3 by default while Stage 2 remains unresolved.
- Do not require a generalized broker abstraction before the approved Stage 2 lane proves useful.

## Control-Plane Map

- `docs/BOOTSTRAP_PROMPT.md`: startup launcher that tells future chats what to read first.
- `docs/PROJECT_STATE.md`: single live checkpoint for current state and active slice status.
- `docs/FIRST_LIVE_PROGRAM.md`: durable first-live staged plan and policy.
- `docs/FIRST_LIVE_EXIT_CRITERIA.md`: durable stage gates, hold rules, and live authorization rules.
- `docs/STRATEGY_REGISTRY.md`: durable strategy inventory and promotion state.
- `docs/WORKFLOW.md`: durable four-role operating model.
- `docs/PROMOTION_RUNBOOK.md`: exact approved promotion procedure.
- `docs/STAGE2_PAPER_OPS.md`, `docs/STAGE2_IBKR_PAPER_OPS.md`, `docs/IBKR_PAPERTRADER_BRINGUP.md`, `docs/IBKR_PAPER_LANE.md`: lane-specific runbooks and references.
- `docs/DAILY_TRADING_RUNBOOK.md`, `docs/PRESET_BEHAVIOR_REFERENCE.md`, `docs/MANUAL_ALERT_WORKFLOW.md`: operator references, not startup truth.

## Standing Workflow Rule

Start from the control-plane docs above, then read only the lane-specific runbook needed for the task at hand.
Do not spread live state across multiple startup docs; update `docs/PROJECT_STATE.md` instead.
