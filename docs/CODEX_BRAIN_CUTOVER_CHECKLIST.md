# Codex Brain Cutover Checklist

Use this only for the first Brain-in-Codex verification pass.

## Required proof-of-load
The Brain must explicitly confirm all of the following before being trusted for normal workflow:

1. It read these docs in order:
   - docs/ASSISTANT_BRIEF.md
   - docs/FIRST_LIVE_PROGRAM.md
   - docs/FIRST_LIVE_EXIT_CRITERIA.md
   - docs/STRATEGY_REGISTRY.md
   - docs/PROJECT_STATE.md

2. It understands:
   - docs/PROJECT_STATE.md is the only live checkpoint
   - Builder is the only repo file editor
   - Reviewer validates the exact Builder commit
   - Human runs terminal, promotion, and deployment commands
   - promotion must happen from a clean /tmp clone
   - runtime checkout is not a commit-authoring surface

3. It confirms:
   - Brain is read-only by role
   - Brain will not edit, stage, commit, push, promote, or deploy
   - Brain will not infer permission from convenience tooling alone

4. It reports:
   - current promoted/master SHA from repo truth
   - current stage
   - current objective
   - expected next move from docs/PROJECT_STATE.md

5. It outputs exactly one best next action grounded in repo truth.

## Stop conditions
Stop the cutover if any of the following occurs:
- Brain cannot clearly identify the startup docs it used
- Brain invents live state not present in docs/PROJECT_STATE.md
- Brain proposes edits directly instead of using Builder
- Brain widens Stage 2 scope without explicit repo truth
- Brain proposes promotion without exact scope, validation, and review conditions
