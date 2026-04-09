# Project State

Last updated: 2026-04-09

This is the single live checkpoint for Trading Codex.
Use it for current project state, active slice status, blockers, warnings, and expected next move.

## Resume Snapshot

- Current promoted SHA: `c8c1a12a5d2aa63f1bea39a437457f1dc16dcb75` (`origin/master` and runtime SHA represented by this checkpoint)
- Active Builder branch: none recorded on promoted `master`
- Active slice base SHA: n/a on promoted `master`
- Reviewer aligned to Builder: yes for promoted content

## Program Position

- Current stage: Stage 2 in progress; Stage 1 complete; live not authorized
- Current objective: stay in Stage 2 forward-evidence accumulation / hold for the approved IBKR PaperTrader lane for `primary_live_candidate_v1`; the bounded Stage 2 shadow daily-ops lane now exists as local-only support infrastructure with daily EOD scheduler surface available, but no new shadow build slice or active next shadow candidate is open
- Last completed milestone: the bounded Stage 2 shadow daily-ops runner slice is now promoted in repo truth with tracked config, retained-artifact runbook, Windows launcher, Windows Task Scheduler installer/helper, and tests; the shipped repo config keeps `active_pair: null`, so the lane defaults to an explicit fail-closed no-op until a future bounded shadow pair is manually opened in the control plane and configured locally
- Runtime / lane status:
  - the approved primary IBKR PaperTrader operational acceptance path is in place and remains the only approved Stage 2 persistent paper-execution lane
  - Stage 2 is not exited because forward evidence is still accumulating over time
  - the promoted shadow-only comparison flow is now in repo truth for the explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`: retained comparison/reporting, robustness, and shadow review scoreboard output are in place without broadening the approved IBKR PaperTrader lane
  - the retained comparison package has now been run for that explicit pair from the clean runtime checkout, and the shadow decision gate outcome is `current_decision: not advancing` with `as_of_date: 2026-04-07`
  - the bounded Stage 2 shadow daily-ops lane now exists in promoted repo truth as a local-only, shadow-only runner with a tracked default config of `active_pair: null`; by default it is fail-closed into an explicit retained no-op result and does not imply an open shadow candidate
  - a repo-managed Windows daily EOD scheduler install surface now exists for that same bounded shadow runner, but it only schedules the existing local-only refresh lane after a human explicitly arms a pair; it does not edit docs, does not arm a pair by itself, and does not broaden the approved IBKR PaperTrader lane
  - that runner remains bounded support infrastructure only: it does not broaden the approved IBKR PaperTrader lane, does not auto-open Stage 3, does not auto-write `docs/STRATEGY_REGISTRY.md`, does not auto-write `docs/PROJECT_STATE.md`, and does not auto-change `current_decision` or any live control-plane decision by itself
  - the existing local Stage 2 paper lane remains supporting groundwork, retained-evidence infrastructure, and the only approved replay lane for shadow work
  - concrete primary runtime mapping is now explicit in repo truth: control-plane `primary_live_candidate_v1` currently maps to preset `dual_mom_vol10_cash_core`, which runs `--strategy dual_mom_vol10_cash`, while the paper/ops state key remains `primary_live_candidate_v1`
  - the registered `primary_live_candidate_v1_vol_managed` shadow candidate is now runnable locally through `scripts/run_backtest.py --strategy primary_live_candidate_v1_vol_managed`, remains outside the approved IBKR PaperTrader lane, and now has current decision `not advancing`
  - IBKR PaperTrader bring-up, lane, review, and scheduled-run docs exist
  - no repo doc says Stage 2 is exited

## Open Items

### Blockers / Warnings

- the approved IBKR operational acceptance path is working, but Stage 2 still requires more operationally reviewable forward evidence in the approved IBKR PaperTrader lane
- the remaining dependency is continued forward-evidence accumulation over time with normal operator and broker availability, not unresolved IBKR access or authenticated paper-session setup and not a repo defect in the primary lane
- bounded shadow work is approved in parallel, but it must stay local-only for shadow strategies; the promoted shadow daily-ops runner remains fail-closed by default, any pair arming and control-plane decisions remain manual, and the scheduler surface must not broaden the Stage 2 IBKR PaperTrader lane or auto-open Stage 3

### Expected Next Move

- keep the approved primary IBKR PaperTrader lane running under forward-evidence accumulation / hold for `primary_live_candidate_v1`
- keep the bounded Stage 2 shadow daily-ops lane in its current support-only posture unless a future `docs/PROJECT_STATE.md` update explicitly opens a bounded shadow pair; with tracked config still at `active_pair: null`, no active next shadow candidate is open from this checkpoint refresh and any scheduled run remains an explicit retained no-op
- do not open a new shadow build slice from this checkpoint refresh; continue primary-lane forward-evidence accumulation / hold, and only reopen bounded shadow work later if a future `docs/PROJECT_STATE.md` update explicitly records a new need without broadening the approved IBKR PaperTrader lane or opening Stage 3

### Approved Shadow-Work Queue

- Only one active next shadow candidate is approved unless evidence clearly justifies more.
- No active next shadow candidate is currently open in repo live state.
- Most recent completed shadow decision gate: the retained primary-vs-shadow comparison package for the explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` reported `as_of_date: 2026-04-07` and `current_decision: not advancing`.
- Any future shadow candidate or bounded shadow slice must be re-entered here explicitly in a later state update while keeping shadow strategies local-only during Stage 2 and outside the approved IBKR PaperTrader lane.

## State Rules

- This is the only file for current project state, active slice status, blockers, warnings, and expected next move.
- Do not store Builder-only in-progress branch metadata here.
- Do not duplicate live state in `docs/ASSISTANT_BRIEF.md`, `docs/BOOTSTRAP_PROMPT.md`, `docs/FIRST_LIVE_PROGRAM.md`, or `docs/STRATEGY_REGISTRY.md`.
- Keep durable policy, staged program, workflow, exit criteria, and runbooks in their dedicated docs.
