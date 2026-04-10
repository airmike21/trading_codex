# Project State

Last updated: 2026-04-10

This is the single live checkpoint for Trading Codex.
Use it for current project state, active slice status, blockers, warnings, and expected next move.

## Resume Snapshot

- Checkpoint reference: use the promoted commit containing this file as the resume anchor; resolve the current `origin/master` SHA from git when that exact value matters
- Active Builder branch: none recorded in this live checkpoint
- Active slice base SHA: n/a until the next active Builder slice is opened from this checkpoint state
- Workspace alignment note: Builder and Reviewer are still parked on the completed Brain-in-Codex cutover state, so neither workspace is itself the active current slice from repo truth

## Program Position

- Current stage: Stage 2 in progress; Stage 1 complete; live not authorized
- Current objective: stay in Stage 2 forward-evidence accumulation / hold for the approved IBKR PaperTrader lane for `primary_live_candidate_v1`; keep the bounded Stage 2 shadow daily-ops lane armed to the explicitly reopened target `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` so the installed scheduler refreshes retained evidence automatically in that same local-only shadow lane while the official `current_decision` remains manual; keep `primary_live_candidate_v1_etf_rotation` available as a coded, runnable, local-only Stage 2 shadow candidate outside the approved IBKR PaperTrader lane without claiming it is already opened/configured in tracked runtime live state
- Last completed milestone: the bounded Stage 2 shadow-only build slice for `primary_live_candidate_v1_etf_rotation` is now part of promoted repo truth, adding the runnable local shadow candidate path plus standard shadow artifacts/tests while leaving the Stage 2 forward-evidence hold, the reopened explicit shadow pair, and tracked runtime live-state config unchanged in substance
- Runtime / lane status:
  - the approved primary IBKR PaperTrader operational acceptance path is in place and remains the only approved Stage 2 persistent paper-execution lane
  - Stage 2 is not exited because forward evidence is still accumulating over time
  - the promoted shadow-only comparison flow is now in repo truth for the explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`: retained comparison/reporting, robustness, and shadow review scoreboard output are in place without broadening the approved IBKR PaperTrader lane
  - the most recent completed retained comparison package for that explicit pair reported `as_of_date: 2026-04-07` and official `current_decision: not advancing`; reopening the pair for daily retained-evidence refresh does not change that manual decision by itself
  - the bounded Stage 2 shadow daily-ops lane now exists in promoted repo truth as a local-only, shadow-only runner whose schema-version-2 config surface supports multiple explicitly opened/configured targets in order; the tracked repo config is still armed only to the reopened `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` target, so current live-state/control-plane truth does not open any additional target by itself
  - bounded Stage 2 shadow-only work has been re-entered in parallel by human override, but queue discipline remains explicit: this is still a Stage 2 shadow-only allowance, not Stage 3 bench expansion, not a second paper lane, and not permission to auto-open any new target
  - the tracked shadow-ops config also keeps optional local-only replay enabled through a separate shadow replay state key, so any replay evidence stays outside the primary local paper lane and still depends on the refreshed bundle reporting `automation_decision: allow`
  - a repo-managed Windows daily EOD scheduler install surface now exists for that same bounded shadow runner, and the installed scheduler can now execute that already-open local-only refresh lane without editing docs, auto-arming any additional target, or broadening the approved IBKR PaperTrader lane
  - that runner remains bounded support infrastructure only: it does not infer or auto-open targets from docs, does not broaden the approved IBKR PaperTrader lane, does not auto-open Stage 3, does not auto-write `docs/STRATEGY_REGISTRY.md`, does not auto-write `docs/PROJECT_STATE.md`, and does not auto-change `current_decision` or any live control-plane decision by itself
  - the existing local Stage 2 paper lane remains supporting groundwork, retained-evidence infrastructure, and the only approved replay lane for shadow work
  - concrete primary runtime mapping is now explicit in repo truth: control-plane `primary_live_candidate_v1` currently maps to preset `dual_mom_vol10_cash_core`, which runs `--strategy dual_mom_vol10_cash`, while the paper/ops state key remains `primary_live_candidate_v1`
  - the registered `primary_live_candidate_v1_vol_managed` shadow candidate is runnable locally through `scripts/run_backtest.py --strategy primary_live_candidate_v1_vol_managed`, remains outside the approved IBKR PaperTrader lane, and is the current explicitly reopened shadow-ops target for automated retained-evidence refresh
  - the registered `primary_live_candidate_v1_etf_rotation` shadow candidate is now coded in promoted repo truth and runnable locally through `scripts/run_backtest.py --strategy primary_live_candidate_v1_etf_rotation`; it remains shadow-only, local-only, outside the approved IBKR PaperTrader lane, and not yet explicitly opened/configured in tracked runtime live state for recurring automation
  - IBKR PaperTrader bring-up, lane, review, and scheduled-run docs exist
  - no repo doc says Stage 2 is exited

## Open Items

### Blockers / Warnings

- the approved IBKR operational acceptance path is working, but Stage 2 still requires more operationally reviewable forward evidence in the approved IBKR PaperTrader lane
- the remaining dependency is continued forward-evidence accumulation over time with normal operator and broker availability, not unresolved IBKR access or authenticated paper-session setup and not a repo defect in the primary lane
- bounded Stage 2 shadow work is approved in parallel by human override, and the promoted local-only runner now supports multiple explicitly opened/configured targets; however, the tracked repo config still arms only the reopened explicit pair, `primary_live_candidate_v1_etf_rotation` remains coded/runnable but not yet opened/configured in tracked runtime live state for recurring automation, any added, reordered, or cleared target remains a manual control-plane action, all control-plane decisions remain manual, and the scheduler surface must not broaden the Stage 2 IBKR PaperTrader lane or auto-open Stage 3

### Expected Next Move

- keep the approved primary IBKR PaperTrader lane running under forward-evidence accumulation / hold for `primary_live_candidate_v1`
- keep the bounded Stage 2 shadow daily-ops lane running for the currently reopened explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`, and review the refreshed retained artifacts manually without treating automation as a decision-gate change
- keep `primary_live_candidate_v1_etf_rotation` in the bounded Stage 2 shadow bench as a coded, runnable, local-only near-path candidate for manual evidence work only; do not treat it as opened/configured in tracked runtime live state and do not infer recurring automation from docs alone
- keep queue discipline explicit: this remains bounded Stage 2 shadow work in parallel with the primary-lane hold, not Stage 3 bench expansion, not a second paper lane, and not an automatic decision or docs-write path

### Approved Shadow-Work Queue

- Only one active next shadow candidate beyond the currently reopened explicit pair is approved from this checkpoint unless evidence clearly justifies more and that choice is explicitly recorded here.
- Current explicitly opened shadow pair in repo live state: `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`, reopened for automated retained-evidence refresh only; promoted multi-target runtime support does not by itself open or queue any additional target.
- Next approved bounded shadow candidate in the manual control-plane queue: `primary_live_candidate_v1_etf_rotation`, a near-path long-only ETF rotation / relative-strength variant with cash fallback, is now coded in promoted repo truth and runnable locally via `scripts/run_backtest.py --strategy primary_live_candidate_v1_etf_rotation`; it still is not opened/configured in tracked runtime live state.
- Most recent completed shadow decision gate: the retained primary-vs-shadow comparison package for the explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` reported `as_of_date: 2026-04-07` and `current_decision: not advancing`.
- The official `current_decision` remains manual; daily automation may refresh retained evidence for the reopened pair but does not change queue status, registry state, or promotion outcome by itself.
- `primary_live_candidate_v1_etf_rotation` remains the only approved next bounded shadow target beyond that reopened pair; any recurring retained-evidence automation for it is opt-in only after a later explicit manual opening/configuration step, and nothing in this queue update changes promotion status, queue order beyond this next slot, or the approved IBKR PaperTrader lane.
- Any additional shadow candidate or further bounded shadow slice beyond that next slot must be re-entered and justified here explicitly in a later state update while keeping shadow strategies local-only during Stage 2 and outside the approved IBKR PaperTrader lane.

## State Rules

- This is the only file for current project state, active slice status, blockers, warnings, and expected next move.
- Do not store Builder-only in-progress branch metadata here.
- Do not duplicate live state in `docs/ASSISTANT_BRIEF.md`, `docs/BOOTSTRAP_PROMPT.md`, `docs/FIRST_LIVE_PROGRAM.md`, or `docs/STRATEGY_REGISTRY.md`.
- Keep durable policy, staged program, workflow, exit criteria, and runbooks in their dedicated docs.
