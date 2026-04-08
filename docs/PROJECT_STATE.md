# Project State

Last updated: 2026-04-08

This is the single live checkpoint for Trading Codex.
Use it for current project state, active slice status, blockers, warnings, and expected next move.

## Resume Snapshot

- Current promoted SHA: `ed57381ea0204b8f781de7801245a61d4e68c894` (`origin/master` and runtime SHA represented by this checkpoint)
- Active Builder branch: none recorded on promoted `master`
- Active slice base SHA: n/a on promoted `master`
- Reviewer aligned to Builder: yes for promoted content

## Program Position

- Current stage: Stage 2 in progress; Stage 1 complete; live not authorized
- Current objective: stay in Stage 2 forward-evidence accumulation / hold for the approved IBKR PaperTrader lane for `primary_live_candidate_v1` while using bounded shadow work as the approved parallel repo activity
- Last completed milestone: the promoted Stage 2 shadow-candidate mapping slice made the runtime mapping explicit for `primary_live_candidate_v1` and made the registered `primary_live_candidate_v1_vol_managed` shadow candidate runnable locally through `scripts/run_backtest.py --strategy primary_live_candidate_v1_vol_managed`, so the next repo slice can compare the correct primary-vs-shadow pair without broadening the approved IBKR PaperTrader lane
- Runtime / lane status:
  - the approved primary IBKR PaperTrader operational acceptance path is in place and remains the only approved Stage 2 persistent paper-execution lane
  - Stage 2 is not exited because forward evidence is still accumulating over time
  - the existing local Stage 2 paper lane remains supporting groundwork, retained-evidence infrastructure, and the only approved replay lane for shadow work
  - concrete primary runtime mapping is now explicit in repo truth: control-plane `primary_live_candidate_v1` currently maps to preset `dual_mom_vol10_cash_core`, which runs `--strategy dual_mom_vol10_cash`, while the paper/ops state key remains `primary_live_candidate_v1`
  - the registered `primary_live_candidate_v1_vol_managed` shadow candidate is now runnable locally through `scripts/run_backtest.py --strategy primary_live_candidate_v1_vol_managed` and remains outside the approved IBKR PaperTrader lane
  - IBKR PaperTrader bring-up, lane, review, and scheduled-run docs exist
  - no repo doc says Stage 2 is exited

## Open Items

### Blockers / Warnings

- the approved IBKR operational acceptance path is working, but Stage 2 still requires more operationally reviewable forward evidence in the approved IBKR PaperTrader lane
- the remaining dependency is continued forward-evidence accumulation over time with normal operator and broker availability, not unresolved IBKR access or authenticated paper-session setup
- bounded shadow work is approved in parallel, but it must stay local-only for shadow strategies and must not broaden the Stage 2 IBKR PaperTrader lane or auto-open Stage 3

### Expected Next Move

- keep the approved primary IBKR PaperTrader lane running under forward-evidence accumulation / hold for `primary_live_candidate_v1`
- next bounded shadow-only repo work is to build the primary-vs-shadow comparison/reporting layer, robustness harness, and shadow review scoreboard for the registered `primary_live_candidate_v1_vol_managed` shadow candidate while keeping shadow strategies local-only and without broadening the approved IBKR PaperTrader lane or opening Stage 3

### Approved Shadow-Work Queue

- Only one active next shadow candidate is approved unless evidence clearly justifies more.
- Registered current next shadow candidate: `primary_live_candidate_v1_vol_managed`, the preferred first volatility-managed variant tied to the current ETF trend/momentum primary candidate; the fallback next candidate, only if needed later, remains a closely related ETF rotation variant.
- Priority 1: build the primary-vs-shadow comparison/reporting layer, robustness harness, and shadow review scoreboard for the explicit primary-vs-shadow pair: `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`.
- Priority 2: run backtest/walk-forward, then optional local-only paper replay, then a shadow decision gate while keeping shadow strategies local-only during Stage 2 and outside the approved IBKR PaperTrader lane.

## State Rules

- This is the only file for current project state, active slice status, blockers, warnings, and expected next move.
- Do not store Builder-only in-progress branch metadata here.
- Do not duplicate live state in `docs/ASSISTANT_BRIEF.md`, `docs/BOOTSTRAP_PROMPT.md`, `docs/FIRST_LIVE_PROGRAM.md`, or `docs/STRATEGY_REGISTRY.md`.
- Keep durable policy, staged program, workflow, exit criteria, and runbooks in their dedicated docs.
