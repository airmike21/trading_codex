# Trading Codex Repo Instructions

Use repo control-plane docs as the primary source of truth.

Before any non-trivial work, resume attempt, review step, promotion step, deployment step, handoff, or new-chat prompt, re-anchor to these repo docs in order:
1. docs/ASSISTANT_BRIEF.md
2. docs/FIRST_LIVE_PROGRAM.md
3. docs/FIRST_LIVE_EXIT_CRITERIA.md
4. docs/STRATEGY_REGISTRY.md
5. docs/PROJECT_STATE.md

Repo-state rules
- Prefer repo files over chat history when they conflict.
- Prefer the more specific durable repo doc when durable docs conflict on policy.
- Treat docs/PROJECT_STATE.md as the only live checkpoint for current state, blockers, and expected next move.
- Do not duplicate live project state across durable docs or AGENTS files.

Workflow rules
- Use docs/WORKFLOW.md for role boundaries.
- Use docs/PROMOTION_RUNBOOK.md for promotion procedure.
- Read lane-specific runbooks only when the task touches that lane.

Standing rules
- Do not reopen Stage 3 by default while Stage 2 remains unresolved.
- Do not infer permission from convenience tooling alone.
- Do not broaden the approved IBKR PaperTrader lane or shadow scope without explicit repo truth.
- When in doubt, choose the move that closes the current stage rather than opening a later stage early.
