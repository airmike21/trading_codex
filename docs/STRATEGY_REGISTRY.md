# Strategy Registry

Last updated: 2026-03-27

This registry is the durable control-plane for strategy status in the first-live program.
Use it to keep the primary live candidate distinct from the wider research bench.

## Registry Rules

- Keep exactly one `Primary Live Candidate` unless evidence justifies replacement.
- A strategy can be in shadow, paper, or live only if its role is explicit here.
- Promotion is sequential: `shadow -> paper -> live`.
- Multiple strategies may exist in shadow or paper, but the first live deployment remains one strategy only.
- Under the clarified Stage 2 definition, `paper-enabled` means one real persistent paper-execution lane is operating with reviewable forward evidence over time. The existing local paper lane and daily ops routine are useful groundwork, not by themselves paper promotion.
- Update this file whenever a strategy changes status or the primary live candidate changes.

## Primary Live Candidate

| Strategy ID | Status | Summary | Cadence | Instruments | Sizing | Initial constraints | Next meaningful move |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `primary_live_candidate_v1` | Primary live candidate; Stage 2 reopened; not yet real-paper-enabled | Simple long-only ETF trend/momentum with cash fallback | Daily/weekly | ETFs only | Whole shares | No options, no shorting, no leverage initially | Choose/build one real persistent paper-execution lane that best preserves parity with the eventual live path when practical; existing local paper lane and daily ops routine remain useful groundwork only |

## Shadow Bench

| Strategy ID | Status | Summary | Why it is not paper-enabled yet | Notes |
| --- | --- | --- | --- | --- |
| None yet | N/A | No additional shadow-bench entries have been registered yet. | Add a row before opening a new bench strategy slice. | Keep bench work from delaying the primary live candidate without evidence. |

## Paper-Enabled Strategies

| Strategy ID | Status | Paper lane | Current scope | Notes |
| --- | --- | --- | --- | --- |
| None yet | N/A | No real persistent paper-execution lane has been exited yet under the clarified Stage 2 definition. | Existing local `scripts/paper_lane.py` + `scripts/paper_lane_daily_ops.py` remain useful groundwork and retained-evidence infrastructure. | Do not promote paper-enabled status until one real persistent paper-execution lane is operating cleanly for `primary_live_candidate_v1`. |

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
- promoting it will not delay the current primary live candidate without evidence
- one real persistent paper-execution lane exists for that specific strategy and is operationally reviewable enough to accumulate forward paper evidence over time

### Paper -> Live

Promote a strategy from paper to live only when all of the following are true:

- the strategy has completed the paper lane required by `docs/FIRST_LIVE_EXIT_CRITERIA.md`
- the funded clean live-account stage has been exited
- the strategy is explicitly selected as the sole live strategy
- live scope remains narrow enough for the first-live program
- the promotion is recorded here before the live launch

### Replacing The Primary Live Candidate

Replace the primary live candidate only when evidence clearly supports the change.
If that happens:

- move the former primary strategy to the correct bench or paper section
- promote the new primary strategy here
- update the rationale in the slice that made the change
