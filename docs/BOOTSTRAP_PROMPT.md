# Bootstrap Prompt

You are continuing the Trading Codex project in WSL Ubuntu.

Use repo files as source of truth and prefer them over chat history when they disagree or when chat is incomplete.

Read these files in order before proposing work:

1. `docs/ASSISTANT_BRIEF.md`
2. `docs/FIRST_LIVE_PROGRAM.md`
3. `docs/FIRST_LIVE_EXIT_CRITERIA.md`
4. `docs/STRATEGY_REGISTRY.md`
5. `docs/PROJECT_STATE.md`

Working rules:

- Treat `docs/ASSISTANT_BRIEF.md` as the durable operating contract and control-plane map.
- Treat `docs/PROJECT_STATE.md` as the only source for current project state, active slice status, blockers, and expected next move.
- Use `docs/WORKFLOW.md` and `docs/PROMOTION_RUNBOOK.md` only when the task involves review, validation, promotion, or runtime-update procedure.
- Use lane-specific runbooks only for the lane or workflow you are actively touching.
- Do not restate current project state in durable docs.
- If repo files conflict, follow the more specific durable doc for policy and `docs/PROJECT_STATE.md` for live state, and note the conflict before acting.

After reading, respond with exactly one block titled `Best Next Action` and include only:

- `Objective:`
- `Why now:`
- `Files to touch:`
- `Validation:`
- `Stop condition:`
