# Strategy Registry

Last updated: 2026-04-09

This registry is the durable control-plane for strategy status in the first-live program.
Use `docs/PROJECT_STATE.md` for current stage, active slice, blockers, and expected next move.
Use this file to keep the primary live candidate distinct from the wider research bench and to record promotion state.

## Registry Rules

- Keep exactly one `Primary Live Candidate` unless evidence justifies replacement.
- A strategy can be in shadow, paper, or live only if its role is explicit here.
- Promotion is sequential: `shadow -> paper -> live`.
- Multiple strategies may exist in shadow or paper, but the first live deployment remains one strategy only.
- Under the Stage 2 policy, `paper-enabled` means the approved primary persistent paper-execution lane is operating with reviewable forward evidence over time. For the primary candidate, that lane is IBKR PaperTrader. The existing local paper lane and daily ops routine are useful groundwork, not by themselves paper promotion.
- Before opening a serious shadow strategy slice, add or update its `Shadow Bench` row here.
- Under the Stage 2 hold policy, shadow strategies remain shadow-only. Optional shadow replays stay in the existing local paper lane and do not broaden the approved IBKR PaperTrader lane.
- Any explicitly opened/configured Stage 2 shadow strategy or pair may use the same local-only recurring retained-evidence workflow, but that automation is opt-in and does not change registry state, queue order, `current_decision`, or promotion status by itself.
- Keep the shadow bench short and ordered. Prefer at most one active next shadow candidate at a time unless evidence justifies more; the current queue belongs in `docs/PROJECT_STATE.md`.
- Update this file whenever a strategy changes status or the primary live candidate changes.

## Primary Live Candidate

| Strategy ID | Role | Status | Summary | Cadence | Instruments | Sizing | Initial constraints | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `primary_live_candidate_v1` | Primary live candidate | Not paper-enabled | Simple long-only ETF trend/momentum with cash fallback | Daily/weekly | ETFs only | Whole shares | No options, no shorting, no leverage initially | Approved Stage 2 primary lane is IBKR PaperTrader. Concrete runtime mapping remains explicit but unchanged: the current approved runtime path uses preset `dual_mom_vol10_cash_core`, which runs `--strategy dual_mom_vol10_cash`, while the durable paper/ops state key remains `primary_live_candidate_v1`. Existing local paper lane and daily ops routine are supporting groundwork, not paper promotion by themselves. |

## Shadow Bench

| Strategy ID | Status | Summary | Why it is not paper-enabled yet | Notes |
| --- | --- | --- | --- | --- |
| `primary_live_candidate_v1_vol_managed` | Coded; shadow-only; local-only; reopened for automated retained-evidence refresh | Near-path volatility-managed version of the current long-only ETF trend/momentum primary candidate that stays close to the first-live path. | The most recent retained primary-vs-shadow comparison package and manual shadow decision gate for the explicit pair against `primary_live_candidate_v1` reported `current_decision: not advancing` as of `2026-04-07`; reopening the pair is for ongoing retained-evidence refresh only, and Stage 2 remains focused on forward evidence for `primary_live_candidate_v1` in the approved IBKR PaperTrader lane. | Concrete local-only mapping is now explicit: `scripts/run_backtest.py --strategy primary_live_candidate_v1_vol_managed` resolves to a `dual_mom_v1`-based shadow implementation with the promoted shadow template and risk-invariants layer. The explicit pair against `primary_live_candidate_v1` is the current reopened Stage 2 shadow-ops target in repo live state through `configs/stage2_shadow_ops.json`; the same local-only recurring evidence workflow is Stage 2 policy-approved for any explicitly opened/configured shadow strategy or pair, but this automation keeps the official `current_decision` manual and does not broaden the approved IBKR PaperTrader lane. |
| `primary_live_candidate_v1_etf_rotation` | Approved next bounded Stage 2 shadow candidate; shadow-only; local-only; not paper-enabled | Closely related long-only ETF rotation / relative-strength variant with cash fallback that stays near the first-live path. | The approved primary persistent paper-execution lane remains IBKR PaperTrader for `primary_live_candidate_v1`, and this candidate is being re-entered only as the next bounded Stage 2 shadow-only slice in parallel; it has not been promoted to any paper lane and is not yet explicitly opened/configured in repo runtime live state for recurring retained-evidence automation. | This row records manual control-plane queue authorization only. Any recurring retained-evidence automation is opt-in only after a later explicit opening/configuration step for this candidate, remains local-only during Stage 2, and does not automatically change `current_decision`, promotion status, or any docs by itself. |

### Shadow Bench Rules

- Register the shadow-bench row before opening a serious shadow strategy slice, and keep the row explicit about the bounded role, current status, and why the strategy remains shadow-only.
- Stage 2 shadow automation is opt-in only: a registry row by itself does not schedule recurring refresh; the current control plane must also explicitly open/configure the target in live state.
- Stage 2 shadow candidates should stay close to the first-live path. Near-path examples include a volatility-managed variant of the current ETF trend/momentum candidate or a closely related ETF rotation variant; the currently preferred next candidate belongs in `docs/PROJECT_STATE.md`.
- Evaluate shadow strategies through a standard template and the same outputs for signal, target weights, diagnostics, and reports so primary-vs-shadow comparison stays consistent.
- Use a retained comparison package for each serious shadow candidate: primary-vs-shadow reporting, robustness checks, a review scoreboard, and risk-invariants review.
- Optional shadow replay remains local-only until later promotion. Do not treat local replay or a `candidate for later paper promotion after Stage 2 exit` decision as paper-enabled status.
- Record a current decision for each serious shadow candidate: `not advancing`, `remain shadow-only`, or `candidate for later paper promotion after Stage 2 exit`.

## Paper-Enabled Strategies

| Strategy ID | Status | Paper lane | Current scope | Notes |
| --- | --- | --- | --- | --- |
| None yet | N/A | No approved primary persistent paper-execution lane has been exited yet under the Stage 2 policy. | Existing local `scripts/paper_lane.py` + `scripts/paper_lane_daily_ops.py` remain useful groundwork and retained-evidence infrastructure; tastytrade sandbox remains secondary regression coverage only. | Add the first row here only after the approved paper lane is operating cleanly with reviewable forward evidence. |

## Live / Promoted Strategies

| Strategy ID | Status | Live account scope | Current limits | Notes |
| --- | --- | --- | --- | --- |
| None yet | N/A | No strategies are live-promoted yet. | First live remains intentionally unopened. | Add the first row here only when Stage 5 is explicitly authorized. |

## Promotion Rules

### Shadow -> Paper

Promote a strategy from shadow to paper only when all of the following are true:

- the strategy thesis, universe, cadence, and constraints are documented in this registry
- the strategy has a bounded role relative to the first-live program
- shadow evidence is clean enough that the strategy is worth spending paper-lane attention on
- promoting it will not delay the primary live candidate without evidence
- one real persistent paper-execution lane exists for that specific strategy and is operationally reviewable enough to accumulate forward paper evidence over time
- for the approved Stage 2 policy, that primary lane is IBKR PaperTrader unless the control plane is explicitly changed
- a `candidate for later paper promotion after Stage 2 exit` decision remains shadow-only until Stage 2 is exited and explicit paper promotion is recorded here

### Paper -> Live

Promote a strategy from paper to live only when all of the following are true:

- the strategy has completed the paper lane required by `docs/FIRST_LIVE_EXIT_CRITERIA.md`
- the funded clean live-account stage has been exited
- the strategy is explicitly selected as the sole live strategy
- live scope remains bounded enough for the first-live program
- the promotion is recorded here before the live launch

### Replacing The Primary Live Candidate

Replace the primary live candidate only when evidence clearly supports the change.
If that happens:

- move the former primary strategy to the correct bench or paper section
- promote the new primary strategy here
- update the rationale in the slice that made the change
