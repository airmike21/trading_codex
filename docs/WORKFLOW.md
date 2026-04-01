# Workflow

Last updated: 2026-04-01

This document defines the strict four-role workflow for Trading Codex changes and promotions.
Use it with `docs/PROMOTION_RUNBOOK.md`.

## Architecture

### Brain

- Define the exact terminal commands.
- Tell Human which commands to run, in order.
- Verify the command output.
- Verify Builder's exact file list before any promotion.
- Refuse promotion when file scope, validation, review freshness, base ref, or environment is wrong.

### Builder

- Perform all repo file edits.
- Never ask Human to patch repo files by hand.
- Output the exact file list every time.
- Output the exact diff every time.
- Return work to Brain when validation fails or file scope must change.

### Reviewer

- Validate Builder's changes when review is required.
- Approve or reject the exact Builder commit under review.
- Treat review as stale if Builder changes the commit after review.

### Human

- Run all terminal commands.
- Run all promotion and deployment commands.
- Do not manually edit repo files in any workspace.
- Stop when Brain says stop; do not improvise fixes.

## Core Rules

- Builder is the only file editor. Human does not manually edit repo files.
- Brain owns command selection and output verification.
- Human owns terminal execution.
- Reviewer validates Builder changes before promotion when required.
- Builder must always provide the exact file list and exact diff.
- Brain must compare the approved file list to the Builder diff before promotion.
- Promote from a clean `/tmp` clone only.
- Never author commits from the runtime checkout.
- A dirty runtime checkout does not block promotion if the `/tmp` promotion clone is clean and runtime is updated only by final `fetch` and `reset`.
- Validation source-of-truth rule: Promotion validation decisions must be based on results from the Builder workspace or a clean `/tmp` clone, not from a stale or unrelated local checkout.
- If local checkout results disagree with Builder or clean-clone results, treat the clean Builder workspace or clean `/tmp` clone as authoritative.
- Stage and commit only approved files.
- Never use broad staging such as `git add .`.
- Validation must pass before commit.
- If anything is wrong, stop and return to Builder.
- Never patch the runtime checkout or promotion workspace by hand.

## Operating Sequence

1. Builder edits files and returns the exact file list plus exact diff.
2. Reviewer validates the exact Builder commit when review is required.
3. Brain checks the exact Builder file list, validation evidence, review freshness, base ref, and promotion commands.
4. Human runs the promotion commands from a clean `/tmp` clone.
5. Human updates the runtime checkout only after promotion succeeds, using `git fetch` and `git reset`.

## Promotion Boundary

- Treat `~/trading_codex` as the runtime checkout.
- Do not author commits there.
- Do not use it to test staged-file selection.
- Do not fix promotion mistakes there.
- Use a separate clean clone under `/tmp` for promotion work every time.
