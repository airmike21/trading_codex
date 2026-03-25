# Strategy Registry

Last updated: 2026-03-25

This registry is the durable control-plane for strategy status in the first-live program.
Use it to keep the primary live candidate distinct from the wider research bench.

## Registry Rules

- Keep exactly one `Primary Live Candidate` unless evidence justifies replacement.
- A strategy can be in shadow, paper, or live only if its role is explicit here.
- Promotion is sequential: `shadow -> paper -> live`.
- Multiple strategies may exist in shadow or paper, but the first live deployment remains one strategy only.
- Update this file whenever a strategy changes status or the primary live candidate changes.

## Primary Live Candidate

| Strategy ID | Status | Summary | Cadence | Instruments | Sizing | Initial constraints | Next meaningful move |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `primary_live_candidate_v1` | Primary live candidate, shadow/pre-paper | Simple long-only ETF trend/momentum with cash fallback | Daily/weekly | ETFs only | Whole shares | No options, no shorting, no leverage initially | Complete Stage 1 sandbox work, then run through the first persistent paper lane |

## Shadow Bench

| Strategy ID | Status | Summary | Why it is not paper-enabled yet | Notes |
| --- | --- | --- | --- | --- |
| None yet | N/A | No additional shadow-bench entries have been registered yet. | Add a row before opening a new bench strategy slice. | Keep bench work from delaying the primary live candidate without evidence. |

## Paper-Enabled Strategies

| Strategy ID | Status | Paper lane | Current scope | Notes |
| --- | --- | --- | --- | --- |
| None yet | N/A | No strategies are paper-enabled yet. | Stage 2 has not been exited yet. | Add rows here only after the paper promotion rules are satisfied. |

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
- a persistent paper lane exists or is being extended in a controlled way for that specific strategy

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
