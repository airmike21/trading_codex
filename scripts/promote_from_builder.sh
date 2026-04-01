#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/promote_from_builder.sh \
    --repo-url <repo-url> \
    --runtime-dir <runtime-dir> \
    --base-sha <approved-base-sha-40hex> \
    --builder-branch <builder-branch> \
    --builder-commit <approved-builder-commit-40hex> \
    --commit-message <approved-commit-message> \
    --approved-file <path> \
    [--approved-file <path> ...] \
    [--base-ref origin/master]

This helper creates a fresh clean clone under /tmp, restores only the approved
paths from the approved Builder commit, runs the required promotion checks, and
pushes the resulting commit to master. Use full 40-character SHAs for
--base-sha and --builder-commit.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

consume_option_value() {
  local option="$1"
  shift

  [[ $# -gt 0 ]] || die "missing value for $option"

  local value="$1"
  [[ -n "$value" ]] || die "missing value for $option"
  [[ "$value" != -* ]] || die "missing value for $option (got another flag: $value)"

  printf '%s\n' "$value"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_nonempty() {
  local value="$1"
  local name="$2"
  [[ -n "$value" ]] || die "missing required argument: $name"
}

require_full_sha() {
  local value="$1"
  local name="$2"
  [[ "$value" =~ ^[0-9a-fA-F]{40}$ ]] || die "$name must be a full 40-character commit SHA"
}

validate_repo_path() {
  local path="$1"
  [[ -n "$path" ]] || die "approved files must not be empty"
  [[ "$path" != /* ]] || die "approved files must be repo-relative: $path"
  case "$path" in
    ..|../*|*/../*|*/..)
      die "approved files must stay within the repo: $path"
      ;;
  esac
}

print_header() {
  printf '\n== %s ==\n' "$1"
}

print_inputs() {
  print_header "Promotion Inputs"
  printf 'Repo URL: %s\n' "$repo_url"
  printf 'Runtime dir: %s\n' "$runtime_dir"
  printf 'Base ref: %s\n' "$base_ref"
  printf 'Approved base SHA: %s\n' "$base_sha"
  printf 'Builder branch: %s\n' "$builder_branch"
  printf 'Approved Builder commit: %s\n' "$builder_commit"
  printf 'Commit message: %s\n' "$commit_message"
  printf 'Promotion clone: %s\n' "$promo_dir"
  printf 'Approved files:\n'
  printf '  %s\n' "${approved_files[@]}"
}

write_sorted_list() {
  local output_path="$1"
  shift
  printf '%s\n' "$@" | LC_ALL=C sort >"$output_path"
}

summarize_success() {
  local promoted_head="$1"

  cat <<EOF

== Promotion Summary ==
Promotion clone: $promo_dir
Promoted commit: $promoted_head
Runtime checkout left unchanged: $runtime_dir

Update the runtime checkout separately only after Brain confirms the push:
  git -C "$runtime_dir" fetch origin --prune
  git -C "$runtime_dir" reset --hard origin/master
  git -C "$runtime_dir" status --short --branch
EOF
}

repo_url=""
runtime_dir=""
base_ref="origin/master"
base_sha=""
builder_branch=""
builder_commit=""
commit_message=""
approved_files=()
promo_dir=""
approved_list=""
builder_diff_list=""
staged_list=""

cleanup_notice() {
  local exit_code=$?
  if [[ -n "$promo_dir" ]]; then
    if [[ $exit_code -eq 0 ]]; then
      printf '\nPromotion helper completed. Promotion clone retained at %s\n' "$promo_dir"
    else
      printf '\nPromotion helper stopped. Inspect the promotion clone at %s\n' "$promo_dir" >&2
    fi
  fi
}

trap cleanup_notice EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-url)
      repo_url="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --runtime-dir)
      runtime_dir="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --base-ref)
      base_ref="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --base-sha)
      base_sha="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --builder-branch)
      builder_branch="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --builder-commit)
      builder_commit="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --commit-message)
      commit_message="$(consume_option_value "$1" "${@:2}")"
      shift 2
      ;;
    --approved-file)
      approved_files+=("$(consume_option_value "$1" "${@:2}")")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

require_nonempty "$repo_url" "--repo-url"
require_nonempty "$runtime_dir" "--runtime-dir"
require_nonempty "$base_ref" "--base-ref"
require_nonempty "$base_sha" "--base-sha"
require_nonempty "$builder_branch" "--builder-branch"
require_nonempty "$builder_commit" "--builder-commit"
require_nonempty "$commit_message" "--commit-message"
require_full_sha "$base_sha" "--base-sha"
require_full_sha "$builder_commit" "--builder-commit"
[[ ${#approved_files[@]} -gt 0 ]] || die "at least one --approved-file is required"
[[ -d "$runtime_dir" ]] || die "runtime dir does not exist: $runtime_dir"
[[ "$base_ref" == "origin/master" ]] || die "this helper only supports --base-ref origin/master"

for approved_file in "${approved_files[@]}"; do
  validate_repo_path "$approved_file"
done

require_cmd git
require_cmd diff
require_cmd mktemp
require_cmd sort

promo_dir="$(mktemp -d /tmp/trading_codex_promotion.XXXXXX)"
approved_list="$(mktemp /tmp/trading_codex_approved_files.XXXXXX)"
builder_diff_list="$(mktemp /tmp/trading_codex_builder_diff.XXXXXX)"
staged_list="$(mktemp /tmp/trading_codex_staged_files.XXXXXX)"

print_inputs

print_header "Create Clean Promotion Clone"
git clone "$repo_url" "$promo_dir"
git -C "$promo_dir" fetch origin --prune

resolved_base_sha="$(git -C "$promo_dir" rev-parse "${base_sha}^{commit}")"
resolved_base_ref_sha="$(git -C "$promo_dir" rev-parse "$base_ref")"
[[ "$resolved_base_ref_sha" == "$resolved_base_sha" ]] || die "$base_ref resolved to $resolved_base_ref_sha, expected $resolved_base_sha"

print_header "Pin Approved Base"
printf 'Resolved base SHA: %s\n' "$resolved_base_sha"
git -C "$promo_dir" switch --detach "$resolved_base_sha"
git -C "$promo_dir" status --short --branch

print_header "Verify Approved Builder Commit"
git -C "$promo_dir" fetch origin "$builder_branch"
resolved_builder_commit="$(git -C "$promo_dir" rev-parse "${builder_commit}^{commit}")"
fetched_builder_tip="$(git -C "$promo_dir" rev-parse FETCH_HEAD)"
[[ "$fetched_builder_tip" == "$resolved_builder_commit" ]] || die "fetched $builder_branch tip $fetched_builder_tip does not match approved commit $resolved_builder_commit"
printf 'Resolved Builder commit: %s\n' "$resolved_builder_commit"
git -C "$promo_dir" show --no-patch --oneline "$resolved_builder_commit"

print_header "Approved File Audit"
write_sorted_list "$approved_list" "${approved_files[@]}"
git -C "$promo_dir" diff --name-only "$resolved_base_sha".."$resolved_builder_commit" | LC_ALL=C sort >"$builder_diff_list"
diff -u "$approved_list" "$builder_diff_list"

print_header "Restore Approved Files Only"
git -C "$promo_dir" restore --source "$resolved_builder_commit" -- "${approved_files[@]}"
git -C "$promo_dir" status --short

print_header "Validation"
if [[ -x "$promo_dir/.venv/bin/python" ]]; then
  "$promo_dir/.venv/bin/python" -m pytest -q
else
  printf '%s\n' "Clean promotion clone has no .venv/bin/python. Relying on Builder-side validation already approved for promotion, plus any required review approval, per docs/PROMOTION_RUNBOOK.md. Validation is still required and must not be skipped."
fi
git -C "$promo_dir" diff --check

print_header "Stage Approved Files"
git -C "$promo_dir" add -- "${approved_files[@]}"
git -C "$promo_dir" diff --cached --stat
git -C "$promo_dir" diff --cached --name-only | LC_ALL=C sort >"$staged_list"
diff -u "$approved_list" "$staged_list"

print_header "Commit Promotion"
git -C "$promo_dir" commit -m "$commit_message"

print_header "Final Base Recheck"
git -C "$promo_dir" fetch origin --prune
final_base_sha="$(git -C "$promo_dir" rev-parse "$base_ref")"
[[ "$final_base_sha" == "$resolved_base_sha" ]] || die "$base_ref moved to $final_base_sha after validation; expected $resolved_base_sha"

print_header "Push To Master"
git -C "$promo_dir" push origin HEAD:master
promoted_head="$(git -C "$promo_dir" rev-parse HEAD)"

summarize_success "$promoted_head"
