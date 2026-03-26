# First Live Program

Last updated: 2026-03-26

This document is the durable control-plane for the first-live program.
It exists so future chats and future Builder slices ground on the same staged plan instead of recreating it from conversation.

## Grounding

- Promoted baseline: `origin/master` at `01bf644668460fbfdeeeddc8c07a230c35a8957b`
- Most recent promoted purpose: Stage 2 persistent paper lane is complete on promoted master, with the latest hardening fix rejecting stale paper-lane marks.
- Stage 1 bounded tastytrade sandbox work is complete.
- Stage 2 persistent paper lane work is complete and exited on promoted master.
- Stage 2 forward paper operations routine now lives in `docs/STAGE2_PAPER_OPS.md` and `scripts/paper_lane_daily_ops.py`.
- The meaningful next move after Stage 2 is conservative operational forward paper running. HOLD unless a concrete repo defect appears.

## Current Program Status

- Current stage status: Stage 1 complete; Stage 2 complete/exited; Stage 3 not started as a coding priority.
- The primary live candidate now has a persistent paper lane and should keep running there operationally.
- Stage 2 completion does not authorize broad bench expansion by default.
- Stage 2 completion does not authorize live promotion.

## Staged Program

### Stage 1: Complete bounded tastytrade sandbox understanding

Finish the bounded tastytrade sandbox lane needed for the first-live program:

- authentication flow
- session handling
- account lookup
- positions/orders/API-path understanding relevant to the first live lane

This stage is intentionally bounded. The goal is not to build every tastytrade capability. The goal is to remove ambiguity around the live target broker and produce enough understanding to support later paper and live work without guesswork.

### Stage 2: Build one persistent paper-trading lane

Build a serious paper lane that can run long enough for multi-month forward testing, while keeping the initial scope narrow:

- one strategy only
- long-only ETFs only
- whole shares only
- daily/weekly execution only
- durable state, review artifacts, and operational routine

Current preference: choose the paper lane that best preserves parity with the eventual live path when practical. That is a current preference, not permanent law. The durable requirement is a persistent paper lane, not a forever paper-broker commitment.

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
- The program is one live strategy and many shadow/paper strategies.
- The first live account must be clean and separate from discretionary/manual positions.
- Do not let new strategy work delay the primary live candidate without evidence.
- Do not treat Stage 2 completion as automatic permission to start Stage 3 bench expansion.
- Do not launch multiple strategies live first.
- When in doubt, choose the meaningful next move that closes the current stage rather than opening later-stage work early.

## How Future Chats Should Use This Doc

1. Start by identifying the current stage.
2. Read the matching exit criteria in `docs/FIRST_LIVE_EXIT_CRITERIA.md`.
3. Pick the meaningful next move that closes the current stage or removes a real blocker for it.
4. Use `docs/STRATEGY_REGISTRY.md` to see which strategy is primary, which ones are shadow-only, which ones are paper-enabled, and which ones are live-promoted.
5. If proposing a stage change or broker change, cite the evidence that justifies the deviation.
