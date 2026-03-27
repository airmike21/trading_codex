# First Live Program

Last updated: 2026-03-27

This document is the durable control-plane for the first-live program.
It exists so future chats and future Builder slices ground on the same staged plan instead of recreating it from conversation.

## Grounding

- Promoted baseline: `origin/master` at `a48d815f5e15e2d8dc50f0a098a02bf72d1b3942`
- Most recent promoted purpose: `fix: harden stage2 daily ops scheduling`
- Prior promoted docs sync: `95cf8e5095c8ea4deafb9793de14016a340a76b5` (`docs: sync control plane after stage2 paper lane`)
- Stage 1 bounded tastytrade sandbox work is complete at `ed91cb19f64f132a16a6c7ecf03a4c5323cee53f`.
- Promoted master contains useful local Stage 2 groundwork: `scripts/paper_lane.py`, `scripts/paper_lane_daily_ops.py`, and retained local paper-lane artifacts.
- Under the clarified program definition, Stage 2 is reopened and not yet exited. The existing local persistent paper lane is useful groundwork, but Stage 2 now requires one real persistent paper-execution lane, with IBKR PaperTrader approved as the preferred primary lane.
- After this control-plane alignment, the meaningful next implementation move is a minimal IBKR PaperTrader operational acceptance path for the primary live candidate. That does not open Stage 3 or require generalized broker abstraction first.

## Current Program Status

- Current stage status: Stage 1 complete; Stage 2 reopened/in progress; Stage 3 not started as a default coding priority.
- IBKR PaperTrader is the preferred primary persistent paper-execution lane for clarified Stage 2.
- tastytrade sandbox remains relevant as secondary regression coverage for tastytrade-specific auth/account/order-flow behavior on the intended live path.
- The primary live candidate has useful local persistent paper-lane groundwork and daily ops evidence infrastructure.
- That groundwork is not, by itself, enough to satisfy the clarified Stage 2 definition.
- Stage 2 does not authorize broad bench expansion by default.
- Stage 2 does not authorize live promotion.
- Stage 2 does not require generalized broker abstraction before the approved primary paper lane proves useful.

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

For clarified Stage 2, the preferred primary persistent paper-execution lane is IBKR PaperTrader. The purpose of this stage is not just local mock bookkeeping. The purpose is to run one strategy through IBKR PaperTrader so we can observe paper order handling, paper fills, scheduling behavior, reconciliation, and restart safety in a way that is operationally real enough to judge Stage 2.

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
- For clarified Stage 2, IBKR PaperTrader is the preferred primary persistent paper-execution lane.
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

1. Start by identifying the current stage.
2. Read the matching exit criteria in `docs/FIRST_LIVE_EXIT_CRITERIA.md`.
3. Pick the meaningful next move that closes the current stage or removes a real blocker for it.
4. Use `docs/STRATEGY_REGISTRY.md` to see which strategy is primary, which ones are shadow-only, which ones are paper-enabled, and which ones are live-promoted.
5. If proposing a stage change or broker change, cite the evidence that justifies the deviation.
