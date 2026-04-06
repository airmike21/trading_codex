# Project State

Last updated: 2026-04-06

This is the single live checkpoint for Trading Codex.
Use it for current project state, active slice status, blockers, warnings, and expected next move.

## Resume Snapshot

- Current promoted SHA: promoted `origin/master` tip represented by this checkpoint
- Active Builder branch: none recorded on promoted `master`
- Active slice base SHA: n/a on promoted `master`
- Reviewer aligned to Builder: yes for promoted content

## Program Position

- Current stage: Stage 2 in progress; Stage 1 complete; live not authorized
- Current objective: close Stage 2 through a narrow, operationally reviewable IBKR PaperTrader lane for `primary_live_candidate_v1`
- Last completed milestone: durable startup docs, bootstrap flow, and live checkpoint were separated into the current control-plane structure
- Runtime / lane status:
  - local Stage 2 paper lane remains supporting groundwork and retained-evidence infrastructure
  - IBKR PaperTrader bring-up, lane, review, and scheduled-run docs exist
  - no repo doc says Stage 2 is exited

## Open Items

### Blockers / Warnings

- Stage 2 still requires operationally reviewable forward evidence in the approved IBKR PaperTrader lane
- IBKR access, authenticated paper session, and time-based forward evidence remain external dependencies

### Expected Next Move

- Implement and validate the narrow IBKR PaperTrader operational acceptance path for `primary_live_candidate_v1`.

## State Rules

- This is the only file for current project state, active slice status, blockers, warnings, and expected next move.
- Do not store Builder-only in-progress branch metadata here.
- Do not duplicate live state in `docs/ASSISTANT_BRIEF.md`, `docs/BOOTSTRAP_PROMPT.md`, `docs/FIRST_LIVE_PROGRAM.md`, or `docs/STRATEGY_REGISTRY.md`.
- Keep durable policy, staged program, workflow, exit criteria, and runbooks in their dedicated docs.
