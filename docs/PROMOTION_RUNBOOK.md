# Promotion Runbook

Last updated: 2026-04-01

This runbook defines the only approved promotion path.
Use it after Builder has delivered an exact diff and exact file list.

## Architecture

- Brain defines the commands and verifies the output.
- Builder edits repo files and supplies the exact diff plus exact file list.
- Reviewer validates the exact Builder commit when review is required.
- Human runs every terminal command.
- Human never patches repo files by hand.

## Preflight Checklist

Do not start promotion until every item below is true.

- Brain has the exact Builder branch, exact Builder commit, exact base ref, exact commit message, exact approved file list, and exact validation commands.
- Builder's exact file list has been compared against the Builder diff and matches exactly.
- Reviewer approval is present when required and is fresh against the exact Builder commit being promoted.
- The approved file list contains every intended path and no extra paths.
- The promotion workspace will be a new clean clone under `/tmp`.
- The runtime checkout is treated as runtime only, not as a place to author commits.
- Human is prepared to stop immediately if any check fails.

Recommended shell variables:

```bash
export REPO_URL=git@github.com:airmike21/trading_codex.git
export PROMO_DIR=/tmp/trading_codex_promotion
export RUNTIME_DIR=~/trading_codex
export BASE_REF=origin/master
export BASE_SHA=<approved-origin-master-sha>
export BUILDER_BRANCH=<builder-branch>
export BUILDER_COMMIT=<approved-builder-commit-sha>
export COMMIT_MSG='<approved-promotion-commit-message>'
APPROVED_FILES=(
  path/to/file1
  path/to/file2
)
```

## Clean /tmp Promotion Procedure

Run promotion only from a new clean clone under `/tmp`.
Do not author commits from `~/trading_codex`.
A dirty runtime checkout alone does not block promotion if the `/tmp` clone is clean and the runtime checkout is updated only after promotion by final `fetch` and `reset`.

```bash
rm -rf "$PROMO_DIR"
git clone "$REPO_URL" "$PROMO_DIR"
cd "$PROMO_DIR"
git fetch origin --prune
test "$(git rev-parse "$BASE_REF")" = "$BASE_SHA"
git switch --detach "$BASE_SHA"
git status --short --branch
git fetch origin "$BUILDER_BRANCH"
git show --no-patch --oneline "$BUILDER_COMMIT"
diff -u <(printf '%s\n' "${APPROVED_FILES[@]}" | sort) <(git diff --name-only "$BASE_SHA".."$BUILDER_COMMIT" | sort)
git restore --source "$BUILDER_COMMIT" -- "${APPROVED_FILES[@]}"
git status --short
```

If any command fails, stop and return to Builder.

## File Selection Rules

- Approve an exact file list before promotion.
- Restore only the approved paths from the Builder commit.
- Do not pull extra files into the promotion workspace.
- Do not use broad staging.
- Verify the Builder diff file list exactly matches the approved file list before any promotion.
- Verify the staged file list exactly matches the approved file list before commit.

Approved-file audit:

```bash
printf '%s\n' "${APPROVED_FILES[@]}"
git diff --name-only "$BASE_SHA".."$BUILDER_COMMIT"
```

## Validation Steps

Run Brain's exact validation commands before commit.
- Validation source-of-truth rule: Promotion validation decisions must be based on results from the Builder workspace or a clean `/tmp` clone, not from a stale or unrelated local checkout.
- If local checkout results disagree with Builder or clean-clone results, treat the clean Builder workspace or clean `/tmp` clone as authoritative.
If Brain did not specify validation commands and the repo offers no narrower doc-specific validation, use the repo-standard default plus whitespace checks:

```bash
python -m pytest
git diff --check
```

If the change is intentionally narrower and Brain specifies a smaller valid command set, use Brain's command set instead.
Do not invent commands that do not fit this repo.

## Commit Rules

- Never use `git add .`.
- Stage only the approved paths explicitly.
- Verify the staged file list before commit.
- Commit only after validation passes.

```bash
git add -- "${APPROVED_FILES[@]}"
git diff --cached --stat
diff -u <(printf '%s\n' "${APPROVED_FILES[@]}" | sort) <(git diff --cached --name-only | sort)
git commit -m "$COMMIT_MSG"
git fetch origin --prune
test "$(git rev-parse origin/master)" = "$BASE_SHA"
git push origin HEAD:master
```

If `origin/master` moved after review or validation, stop.
Do not rework the promotion clone by hand.
Return to Builder for a fresh slice if the base changed.

## Runtime Update

Update the runtime checkout only after the promotion push succeeds.
Never author commits from the runtime repo.
Never use the runtime repo to rescue a bad promotion.

```bash
cd "$RUNTIME_DIR"
git fetch origin --prune
git reset --hard origin/master
git status --short --branch
```

If the runtime checkout was dirty before promotion, that did not block promotion.
The runtime checkout remains runtime-only.

## Verification Checklist

Human and Brain must confirm all of the following after promotion:

- The promoted commit is on `origin/master`.
- The promoted commit matches the approved Builder content for the approved files.
- The approved file list matched the Builder diff file list exactly.
- The staged file list matched the approved file list exactly before commit.
- Validation passed on the content that was promoted.
- The runtime checkout now resolves to the promoted `origin/master` after `fetch` and `reset`.
- No manual patching occurred in the runtime checkout or promotion workspace.

Useful checks:

```bash
git -C "$PROMO_DIR" rev-parse HEAD
git -C "$PROMO_DIR" rev-parse origin/master
git -C "$PROMO_DIR" show --stat --oneline -1 HEAD
git -C "$RUNTIME_DIR" rev-parse HEAD
git -C "$RUNTIME_DIR" rev-parse origin/master
git -C "$RUNTIME_DIR" status --short --branch
```

## Failure Handling

Stop immediately and return to Builder if any of the following is true:

- validation fails
- review is required but missing
- review is stale against the Builder commit being promoted
- the approved file list does not match the Builder diff
- the staged file list does not match the approved file list
- the wrong branch or wrong base ref is in use
- the promotion is not happening from a clean `/tmp` clone
- `origin/master` moved after review or validation
- promotion reveals missing files, extra files, or unapproved changes
- the runtime repo was used to author a commit

Do not patch the runtime checkout.
Do not patch the promotion workspace by hand.
Do not continue with partial fixes.

## Common Failure Modes

- Extra files were staged by accident. Stop, discard the promotion clone, and return to Builder.
- Someone tried to promote directly from a dirty runtime checkout. Stop and restart from a clean `/tmp` clone.
- Reviewer approval was for an older Builder commit. Stop and get fresh review for the exact commit.
- Validation was skipped or run against different content. Stop and rerun promotion from a clean `/tmp` clone after Builder or Brain corrects the gap.
- The wrong branch or wrong base was used. Stop and restart from the approved base ref.
- The runtime repo was used to author or amend a commit. Stop and return to Builder. Do not salvage it in place.
- No one compared the approved file list to the Builder diff. Stop before staging anything and perform the exact comparison.
