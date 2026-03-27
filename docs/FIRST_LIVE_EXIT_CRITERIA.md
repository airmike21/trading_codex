# First Live Exit Criteria

Last updated: 2026-03-27

This document defines when each first-live stage is complete, when future chats should continue coding, when they should hold for operational work, when they should stay shadow-only, and when live is allowed.

## Intended Sequencing

The intended sequence is fixed unless evidence justifies a change:

1. bounded tastytrade sandbox work
2. one real persistent paper-execution lane
3. broader strategy bench work in shadow/paper
4. funded clean tastytrade live account
5. one-strategy limited live deployment

## Decision Rules

- Continue coding when the current stage has repo-solvable gaps and the work directly closes them.
- Hold when the current stage is blocked by credentials, account setup, broker operations, or other external constraints rather than a repo defect.
- Stay shadow-only when strategy research or local retained-evidence groundwork can still advance safely but the real paper-execution or live gates are not met.
- Allow live only when Stages 1 through 4 are exited and the Stage 5 criteria are explicitly satisfied for exactly one strategy.

## Stage 1: Bounded Tastytrade Sandbox Work

Exit criteria:

- The sandbox auth path is understood end to end well enough that future chats are not guessing about login, challenge handling, account lookup, positions, or order API paths relevant to the first live lane.
- Repo documentation or repo-adjacent operational notes make the tastytrade API-path understanding reusable.
- Known remaining blockers are clearly identified as operational/external limits or deliberately deferred scope, not unresolved repo ambiguity.
- The output of this stage is narrow and handoff-ready for paper-lane work.

Continue coding when:

- broker auth or endpoint behavior relevant to the first live lane is still unclear
- the repo still needs bounded sandbox plumbing or documentation to remove that ambiguity

Hold when:

- the remaining blocker is access, credentials, device challenge flow, or sandbox availability rather than missing repo understanding

Anti-goals and out of scope:

- full tastytrade feature coverage
- options, shorting, leverage, or margin workflows
- generalized broker abstraction work that does not directly serve the staged first-live path
- opening multiple strategy lanes during sandbox completion

## Stage 2: One Real Persistent Paper-Execution Lane

Exit criteria:

- The approved primary real paper-execution lane for clarified Stage 2 is IBKR PaperTrader, and it can be operated repeatedly for the primary live candidate without ad hoc repo surgery.
- The IBKR PaperTrader lane is deep enough for multi-month forward testing: forward evidence accumulates over time, and paper order handling, paper fills, scheduling behavior, reconciliation, and restart safety are all reviewable.
- The lane remains intentionally narrow: one strategy, long-only ETF exposure, whole shares, and daily/weekly execution.
- Durable state and retained review artifacts exist so drift, execution mistakes, scheduling problems, reconciliation issues, and restart problems can be detected without guesswork.
- The strategy can keep running forward through IBKR PaperTrader without being blocked by unresolved Stage 1 ambiguity.
- The existing local persistent paper lane and daily ops routine may contribute useful groundwork and retained evidence, but they are not by themselves sufficient to exit Stage 2.
- tastytrade sandbox may remain useful as secondary regression coverage for tastytrade-specific auth/account/order-flow behavior, but it is not the main Stage 2 paper lane.

Continue coding when:

- the minimal IBKR PaperTrader operational acceptance path is not built yet or is not yet operationally reviewable
- the lane cannot yet accumulate forward evidence cleanly over time
- paper order, fill, scheduling, reconciliation, or restart behavior in IBKR PaperTrader is not reviewable enough
- the primary strategy still cannot operate end to end through IBKR PaperTrader

Hold when:

- the remaining blocker is IBKR PaperTrader account setup, paper-service access, broker access, credentials, or other external operations rather than a repo defect
- the repo is waiting on forward evidence to accumulate over time rather than on missing code

Shadow-only when:

- strategy logic can still be validated safely while the IBKR PaperTrader lane is being stabilized
- local paper-lane groundwork, retained evidence infrastructure, or tastytrade sandbox regression coverage can keep running, but IBKR PaperTrader is not yet trustworthy enough to serve as the main decision surface

Anti-goals and out of scope:

- broad multi-strategy paper deployment before the IBKR PaperTrader lane is stable
- accidental broker pivot or speculative paper-service lock-in without evidence
- generalized broker abstraction work before the approved Stage 2 lane proves operationally useful
- expanding into options, shorting, leverage, or intraday complexity
- treating tastytrade sandbox or local mock bookkeeping alone as proof that Stage 2 is complete

## Stage 3: Strategy Bench Expansion

Exit criteria:

- Additional strategies are added one at a time with registry entries and clear status.
- Each additional strategy has a defined role: shadow-only, paper-enabled, or not advancing.
- Bench work does not delay the primary live candidate unless evidence shows the primary candidate should change.
- The repo can distinguish clearly between the primary live candidate and the broader research bench.

Continue coding when:

- the paper lane is already stable enough and a new strategy adds justified research value
- the strategy registry or promotion rules need updates to keep the bench orderly

Hold when:

- additional strategy work is mostly curiosity-driven and is distracting from the primary live candidate
- evidence is not yet strong enough to justify changing the primary live candidate

Shadow-only when:

- a strategy is still being evaluated and has not earned paper promotion

Anti-goals and out of scope:

- promoting multiple strategies to first live
- letting broad strategy exploration stall the primary program
- bulk-porting a large bench without clear sequencing or ownership

## Stage 4: Funded Clean Live Account

Exit criteria:

- A separate tastytrade live account exists for the first-live program.
- The account is funded and operationally ready for a limited launch.
- The account is clean: no discretionary/manual holdings and no unrelated automated positions.
- The operational path for using the account is documented without putting secrets, account numbers, or credentials in the repo.
- The account can support exactly one limited live strategy without ambiguity about what the repo should reconcile.

Continue coding when:

- the repo still needs narrow live-account readiness support that directly serves the first limited live launch

Hold when:

- funding, broker account opening, approvals, or other external operations are the blocker
- the account is not cleanly separated from manual/discretionary trading

Anti-goals and out of scope:

- reusing a discretionary trading account
- funding multiple first-live strategies at once
- storing secrets or account identifiers in repo docs

## Stage 5: One-Strategy Limited Live

Exit criteria:

- Exactly one strategy is promoted to live in `docs/STRATEGY_REGISTRY.md`.
- That strategy has completed the shadow and paper gates required by the registry.
- Live execution, review, and reconciliation are working well enough to explain what happened after each live action.
- Initial live scope remains narrow: long-only ETFs, whole shares, no options, no shorting, and no leverage.
- The first evaluation period shows that live behavior matches expectations closely enough to continue deliberately rather than by inertia.

Continue coding when:

- live controls, review, or reconciliation still have repo-solvable gaps directly blocking the single live lane

Hold when:

- the blocker is external operations, account hygiene, funding, or unexplained live discrepancies that require investigation before more code

Shadow-only when:

- a strategy is not the sole promoted live strategy yet
- live controls are not ready, even if shadow or paper evidence is strong

Live is allowed when:

- Stages 1 through 4 are exited
- the strategy is the sole live-promoted strategy in the registry
- no policy in `docs/FIRST_LIVE_PROGRAM.md` is being violated

Anti-goals and out of scope:

- launching multiple strategies live first
- adding options, shorting, leverage, or broad portfolio logic to the first live deployment
- treating early live success as permission to skip review discipline
