# First Live Program

Last updated: 2026-04-06

This document is the durable control-plane for the first-live program.
It exists so future chats and future Builder slices ground on the same staged plan instead of recreating it from conversation.
Use `docs/PROJECT_STATE.md` for current stage status, active objective, and expected next move.

## Staged Program

### Stage 1: Complete bounded tastytrade sandbox understanding

Finish the bounded tastytrade sandbox lane needed for the first-live program:

- authentication flow
- session handling
- account lookup
- positions/orders/API-path understanding relevant to the first live lane

This stage is intentionally bounded. The goal is not to build every tastytrade capability. The goal is to remove ambiguity around the live target broker and produce enough understanding to support later paper and live work without guesswork.

### Stage 2: Build one real persistent paper-execution lane

Build one serious paper-trading lane that is deep enough for multi-month forward testing, while keeping the initial scope narrow.

For Stage 2, the approved primary persistent paper-execution lane is IBKR PaperTrader. The purpose of this stage is not just local mock bookkeeping. The purpose is to run one strategy through IBKR PaperTrader so we can observe paper order handling, paper fills, scheduling behavior, reconciliation, and restart safety in a way that is operationally real enough to judge Stage 2.

Keep the scope narrow:

- one strategy only
- long-only ETFs only
- whole shares only
- daily/weekly execution only
- durable state, retained review artifacts, and a repeatable operational routine

Retain the existing local paper lane and daily ops routine as supporting groundwork and retained evidence. Use tastytrade sandbox as secondary integration/regression coverage for tastytrade-specific auth/account/order-flow behavior relevant to the intended live path. Tastytrade remains the intended live target unless evidence clearly justifies change.

This decision does not open Stage 3, does not promote any strategy live, and does not require a generalized broker abstraction before Stage 2 proves useful.

Stage 2 is complete only when one strategy can keep running persistently in the IBKR PaperTrader lane with forward-testing evidence accumulating over time, and the lane is operationally reviewable enough to detect drift, execution mistakes, scheduling problems, reconciliation issues, or restart problems without ad hoc repo surgery.

### Stage 3: Expand the strategy bench one strategy at a time

Only after the first paper lane is operating cleanly enough and evidence justifies more bench work, additional strategies may be added to shadow or paper one at a time.

This stage exists to widen the bench without destabilizing the first-live path. New strategy work is allowed only after the primary lane is operating cleanly enough that it no longer needs to monopolize attention.

### Stage 4: Fund a clean separate tastytrade live account

Fund a separate tastytrade live account dedicated to the first-live program.

This account must stay clean:

- no discretionary/manual positions
- no unrelated live experiments
- no mixed-purpose holdings that make automated reconciliation ambiguous

### Stage 5: Launch one limited live strategy

Deploy exactly one strategy live into the clean account with tight limits and close review.

The first live deployment is intentionally narrow:

- one strategy only
- long-only ETF exposure only
- whole shares only
- no options initially
- no shorting initially
- no leverage initially

## Durable Policies

- Tastytrade remains the live target unless evidence clearly justifies change.
- For Stage 2, IBKR PaperTrader is the approved primary persistent paper-execution lane.
- tastytrade sandbox remains secondary regression coverage for tastytrade-specific auth/account/order-flow behavior on the intended live path, not the main Stage 2 paper lane.
- The program is one live strategy and many shadow/paper strategies.
- The first live account must be clean and separate from discretionary/manual positions.
- Do not let new strategy work delay the primary live candidate without evidence.
- Do not treat the existing local paper-lane groundwork as Stage 2 exit by itself.
- Do not let local Stage 2 groundwork become automatic permission to start Stage 3 bench expansion.
- Do not require generalized broker abstraction before the approved Stage 2 paper lane proves useful.
- Do not launch multiple strategies live first.
- When in doubt, choose the meaningful next move that closes the current stage rather than opening later-stage work early.

## How Future Chats Should Use This Doc

1. Read `docs/PROJECT_STATE.md` to identify the current stage and active objective.
2. Read the matching exit criteria in `docs/FIRST_LIVE_EXIT_CRITERIA.md`.
3. Use this document to judge whether proposed work closes the current stage or removes a real blocker for it.
4. Use `docs/STRATEGY_REGISTRY.md` to see which strategy is primary, which ones are shadow-only, which ones are paper-enabled, and which ones are live-promoted.
5. If proposing a stage change or broker change, cite the evidence that justifies the deviation.
