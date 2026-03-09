#!/usr/bin/env bash

set -euo pipefail

window=""
base_dir="~/.trading_codex/scheduled_runs"
python_bin=""
presets_file=""
timestamp=""

expand_tilde() {
  local value="${1:-}"
  case "$value" in
    "~")
      printf '%s\n' "$HOME"
      ;;
    "~/"*)
      printf '%s/%s\n' "$HOME" "${value#~/}"
      ;;
    *)
      printf '%s\n' "$value"
      ;;
  esac
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --window)
      window="${2:-}"
      shift 2
      ;;
    --base-dir)
      base_dir="${2:-}"
      shift 2
      ;;
    --python)
      python_bin="${2:-}"
      shift 2
      ;;
    --presets-file)
      presets_file="${2:-}"
      shift 2
      ;;
    --timestamp)
      timestamp="${2:-}"
      shift 2
      ;;
    *)
      printf '[scheduled_dual_compare] ERROR: unknown arg: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$window" ]]; then
  printf '[scheduled_dual_compare] ERROR: --window is required\n' >&2
  exit 2
fi

if [[ -z "$python_bin" ]]; then
  python_bin="${repo_root}/.venv/bin/python"
else
  python_bin="$(expand_tilde "$python_bin")"
fi

if [[ -n "$presets_file" ]]; then
  presets_file="$(expand_tilde "$presets_file")"
fi

cd "$repo_root"

cmd=(
  "$python_bin"
  "scripts/scheduled_dual_compare.py"
  "--window"
  "$window"
  "--base-dir"
  "$base_dir"
)

if [[ -n "$presets_file" ]]; then
  cmd+=("--presets-file" "$presets_file")
fi

if [[ -n "$timestamp" ]]; then
  cmd+=("--timestamp" "$timestamp")
fi

exec "${cmd[@]}"
