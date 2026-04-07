# Project State

Last updated: 2026-04-07

This is the single live checkpoint for Trading Codex.
Use it for current project state, active slice status, blockers, warnings, and expected next move.

## Resume Snapshot

- Current promoted SHA: promoted `origin/master` tip represented by this checkpoint
- Active Builder branch: none recorded on promoted `master`
- Active slice base SHA: n/a on promoted `master`
- Reviewer aligned to Builder: yes for promoted content

## Program Position

- Current stage: Stage 2 in progress; Stage 1 complete; live not authorized
- Current objective: stay in Stage 2 forward-evidence accumulation / hold for the approved IBKR PaperTrader lane for `primary_live_candidate_v1` while using bounded shadow work as the approved parallel repo activity
- Last completed milestone: the IBKR PaperTrader operational acceptance path is in place and the repo control-plane now needs forward evidence, not a Stage 2 lane redesign
- Runtime / lane status:
  - the approved primary IBKR PaperTrader operational acceptance path is in place and remains the only approved Stage 2 persistent paper-execution lane
  - Stage 2 is not exited because forward evidence is still accumulating over time
  - the existing local Stage 2 paper lane remains supporting groundwork, retained-evidence infrastructure, and the only approved replay lane for shadow work
  - IBKR PaperTrader bring-up, lane, review, and scheduled-run docs exist
  - no repo doc says Stage 2 is exited

## Open Items

### Blockers / Warnings

- the approved IBKR operational acceptance path is working, but Stage 2 still requires more operationally reviewable forward evidence in the approved IBKR PaperTrader lane
- the remaining dependency is continued forward-evidence accumulation over time with normal operator and broker availability, not unresolved IBKR access or authenticated paper-session setup
- bounded shadow work is approved in parallel, but it must stay local-only for shadow strategies and must not broaden the Stage 2 IBKR PaperTrader lane or auto-open Stage 3

### Expected Next Move

- keep the approved primary IBKR PaperTrader lane running under forward-evidence accumulation / hold for `primary_live_candidate_v1`
- execute the approved bounded shadow-work path in parallel without broadening the Stage 2 lane

### Approved Shadow-Work Queue

- Only one active next shadow candidate is approved unless evidence clearly justifies more.
- Priority 1: register one shadow-bench entry in `docs/STRATEGY_REGISTRY.md` before opening a serious shadow slice.
- Priority 2: preferred first shadow candidate is a volatility-managed version of the current ETF trend/momentum candidate; the fallback next candidate, only if needed, is a closely related ETF rotation variant.
- Priority 3: build the common shadow-strategy template and risk-invariants layer.
- Priority 4: build the primary-vs-shadow comparison/reporting layer, robustness harness, and shadow review scoreboard.
- Priority 5: run backtest/walk-forward, then optional local-only paper replay, then a shadow decision gate.

## State Rules

- This is the only file for current project state, active slice status, blockers, warnings, and expected next move.
- Do not store Builder-only in-progress branch metadata here.
- Do not duplicate live state in `docs/ASSISTANT_BRIEF.md`, `docs/BOOTSTRAP_PROMPT.md`, `docs/FIRST_LIVE_PROGRAM.md`, or `docs/STRATEGY_REGISTRY.md`.
- Keep durable policy, staged program, workflow, exit criteria, and runbooks in their dedicated docs.
