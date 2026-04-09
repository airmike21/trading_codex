# Project State

Last updated: 2026-04-09

This is the single live checkpoint for Trading Codex.
Use it for current project state, active slice status, blockers, warnings, and expected next move.

## Resume Snapshot

- Current promoted SHA: `44696d94c9b72508956052e830eee1694a585894` (`origin/master` and runtime SHA represented by this checkpoint)
- Active Builder branch: none recorded on promoted `master`
- Active slice base SHA: n/a on promoted `master`
- Reviewer aligned to Builder: yes for promoted content

## Program Position

- Current stage: Stage 2 in progress; Stage 1 complete; live not authorized
- Current objective: stay in Stage 2 forward-evidence accumulation / hold for the approved IBKR PaperTrader lane for `primary_live_candidate_v1`; keep the bounded Stage 2 shadow daily-ops lane armed to the explicitly reopened target `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` so the installed scheduler refreshes retained evidence automatically in that same local-only shadow lane while the official `current_decision` remains manual; the promoted runtime may now serve multiple explicitly opened/configured local-only Stage 2 shadow targets, but the tracked repo config still opens only the current target and does not auto-open any new target by itself
- Last completed milestone: the bounded Stage 2 shadow daily-ops runtime slice promoted at `44696d94c9b72508956052e830eee1694a585894` now supports schema-version-2 `targets` for multiple explicitly opened/configured local-only shadow targets, while the tracked repo config remains intentionally armed to the reopened `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` target so scheduled runs refresh retained evidence there instead of producing an explicit retained no-op
- Runtime / lane status:
  - the approved primary IBKR PaperTrader operational acceptance path is in place and remains the only approved Stage 2 persistent paper-execution lane
  - Stage 2 is not exited because forward evidence is still accumulating over time
  - the promoted shadow-only comparison flow is now in repo truth for the explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`: retained comparison/reporting, robustness, and shadow review scoreboard output are in place without broadening the approved IBKR PaperTrader lane
  - the most recent completed retained comparison package for that explicit pair reported `as_of_date: 2026-04-07` and official `current_decision: not advancing`; reopening the pair for daily retained-evidence refresh does not change that manual decision by itself
  - the bounded Stage 2 shadow daily-ops lane now exists in promoted repo truth as a local-only, shadow-only runner whose schema-version-2 config surface supports multiple explicitly opened/configured targets in order; the tracked repo config is still armed only to the reopened `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` target, so current live-state/control-plane truth does not open any additional target by itself
  - the tracked shadow-ops config also keeps optional local-only replay enabled through a separate shadow replay state key, so any replay evidence stays outside the primary local paper lane and still depends on the refreshed bundle reporting `automation_decision: allow`
  - a repo-managed Windows daily EOD scheduler install surface now exists for that same bounded shadow runner, and the installed scheduler can now execute that already-open local-only refresh lane without editing docs, auto-arming any additional target, or broadening the approved IBKR PaperTrader lane
  - that runner remains bounded support infrastructure only: it does not infer or auto-open targets from docs, does not broaden the approved IBKR PaperTrader lane, does not auto-open Stage 3, does not auto-write `docs/STRATEGY_REGISTRY.md`, does not auto-write `docs/PROJECT_STATE.md`, and does not auto-change `current_decision` or any live control-plane decision by itself
  - the existing local Stage 2 paper lane remains supporting groundwork, retained-evidence infrastructure, and the only approved replay lane for shadow work
  - concrete primary runtime mapping is now explicit in repo truth: control-plane `primary_live_candidate_v1` currently maps to preset `dual_mom_vol10_cash_core`, which runs `--strategy dual_mom_vol10_cash`, while the paper/ops state key remains `primary_live_candidate_v1`
  - the registered `primary_live_candidate_v1_vol_managed` shadow candidate is runnable locally through `scripts/run_backtest.py --strategy primary_live_candidate_v1_vol_managed`, remains outside the approved IBKR PaperTrader lane, and is the current explicitly reopened shadow-ops target for automated retained-evidence refresh
  - IBKR PaperTrader bring-up, lane, review, and scheduled-run docs exist
  - no repo doc says Stage 2 is exited

## Open Items

### Blockers / Warnings

- the approved IBKR operational acceptance path is working, but Stage 2 still requires more operationally reviewable forward evidence in the approved IBKR PaperTrader lane
- the remaining dependency is continued forward-evidence accumulation over time with normal operator and broker availability, not unresolved IBKR access or authenticated paper-session setup and not a repo defect in the primary lane
- bounded shadow work is approved in parallel, and the promoted local-only runner now supports multiple explicitly opened/configured targets; however, the tracked repo config still arms only the reopened explicit pair, any added, reordered, or cleared target remains a manual control-plane action, all control-plane decisions remain manual, and the scheduler surface must not broaden the Stage 2 IBKR PaperTrader lane or auto-open Stage 3

### Expected Next Move

- keep the approved primary IBKR PaperTrader lane running under forward-evidence accumulation / hold for `primary_live_candidate_v1`
- keep the bounded Stage 2 shadow daily-ops lane running for the currently reopened explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`, and review the refreshed retained artifacts manually without treating automation as a decision-gate change
- do not open a new shadow build slice or add another shadow target or pair from this checkpoint refresh; continue primary-lane forward-evidence accumulation / hold while keeping the current local-only shadow target bounded and outside the approved IBKR PaperTrader lane unless a later manual control-plane/config update explicitly changes repo truth

### Approved Shadow-Work Queue

- Only one active next shadow candidate is approved from this checkpoint unless evidence clearly justifies more and that choice is explicitly recorded here.
- Current explicitly opened shadow pair in repo live state: `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`, reopened for automated retained-evidence refresh only; promoted multi-target runtime support does not by itself open or queue any additional target.
- Most recent completed shadow decision gate: the retained primary-vs-shadow comparison package for the explicit pair `primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed` reported `as_of_date: 2026-04-07` and `current_decision: not advancing`.
- The official `current_decision` remains manual; daily automation may refresh retained evidence for the reopened pair but does not change queue status, registry state, or promotion outcome by itself.
- Any future shadow candidate or additional bounded shadow slice must be re-entered and justified here explicitly in a later state update while keeping shadow strategies local-only during Stage 2 and outside the approved IBKR PaperTrader lane.

## State Rules

- This is the only file for current project state, active slice status, blockers, warnings, and expected next move.
- Do not store Builder-only in-progress branch metadata here.
- Do not duplicate live state in `docs/ASSISTANT_BRIEF.md`, `docs/BOOTSTRAP_PROMPT.md`, `docs/FIRST_LIVE_PROGRAM.md`, or `docs/STRATEGY_REGISTRY.md`.
- Keep durable policy, staged program, workflow, exit criteria, and runbooks in their dedicated docs.
