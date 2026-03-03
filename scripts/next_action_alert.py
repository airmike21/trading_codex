#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


def _default_state_dir() -> Path:
    return Path.home() / ".cache" / "trading_codex" / "next_action_alert"


def _run_cmd(argv: List[str]) -> str:
    """Run a command and return stdout as a string (no trailing enforcement here)."""
    p = subprocess.run(argv, capture_output=True, text=True)
    if p.returncode != 0:
        msg = (p.stderr or "") + (p.stdout or "")
        raise RuntimeError(f"Command failed ({p.returncode}): {' '.join(argv)}\n{msg}")
    return p.stdout


def _expect_one_line(output: str, context: str) -> str:
    lines = output.splitlines()
    if len(lines) != 1:
        raise RuntimeError(f"{context} expected exactly 1 line, got {len(lines)} lines: {lines!r}")
    return lines[0]


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def _read_state(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except FileNotFoundError:
        return None


def _extract_option_value(args: List[str], flag: str) -> Optional[str]:
    for i, arg in enumerate(args):
        if arg == flag:
            if i + 1 < len(args):
                return args[i + 1]
            return None
        prefix = flag + "="
        if arg.startswith(prefix):
            return arg.split("=", 1)[1]
    return None


def _extract_option_values(args: List[str], flag: str) -> List[str]:
    for i, arg in enumerate(args):
        if arg == flag:
            values: List[str] = []
            j = i + 1
            while j < len(args) and not args[j].startswith("-"):
                values.append(args[j])
                j += 1
            return values
        prefix = flag + "="
        if arg.startswith(prefix):
            raw = arg.split("=", 1)[1]
            if not raw:
                return []
            return [item for item in raw.split(",") if item]
    return []


def resolve_state_path(
    state_file: Optional[str | Path],
    state_dir: Path,
    state_key: Optional[str],
    derived_key_inputs: dict[str, object],
) -> Path:
    if state_file is not None:
        return Path(state_file)

    if state_key:
        key_source = state_key
    else:
        key_source = json.dumps(
            derived_key_inputs,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    derived_key = hashlib.sha1(key_source.encode("utf-8")).hexdigest()[:12]
    return state_dir / f"next_action_alert.{derived_key}.json"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Emit a one-line alert ONLY when next_action event_id changes."
    )
    p.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="Explicit state file path for last seen event_id. If set, keyed state files are disabled.",
    )
    p.add_argument(
        "--state-key",
        default=None,
        help="Optional key for per-monitor state isolation when --state-file is not set.",
    )
    p.add_argument(
        "--emit",
        choices=["json", "text"],
        default="json",
        help="What to print when event_id changes (default: json).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not update state-file (still performs change detection).",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print debug info to stderr (never affects stdout).",
    )
    p.add_argument(
        "run_backtest_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed to scripts/run_backtest.py. Use `--` before these args.",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    # Expect the user to separate wrapper args from run_backtest args with "--"
    rb_args = list(args.run_backtest_args)
    if rb_args and rb_args[0] == "--":
        rb_args = rb_args[1:]

    # Guard: don't allow user to pass next-action flags; wrapper controls them.
    forbidden = {"--next-action", "--next-action-json"}
    if any(a in forbidden for a in rb_args):
        raise SystemExit("Do not pass --next-action/--next-action-json to next_action_alert.py; wrapper controls output mode.")

    script_dir = Path(__file__).resolve().parent
    run_backtest = script_dir / "run_backtest.py"
    py = sys.executable  # use same interpreter/venv the wrapper is run with

    # 1) Get next-action JSON payload (one line).
    json_cmd = [py, str(run_backtest)] + rb_args + ["--next-action-json"]
    if args.verbose:
        print(f"[next_action_alert] Running: {' '.join(json_cmd)}", file=sys.stderr)

    json_out = _run_cmd(json_cmd)
    json_line = _expect_one_line(json_out, "run_backtest --next-action-json")
    payload = json.loads(json_line)

    if "event_id" not in payload:
        raise RuntimeError("Payload missing event_id. Ensure --next-action-json includes event_id.")

    event_id = str(payload["event_id"])
    derived_key_inputs = {
        "strategy": payload.get("strategy"),
        "symbol": payload.get("symbol"),
        "symbols": _extract_option_values(rb_args, "--symbols"),
        "defensive": _extract_option_value(rb_args, "--defensive"),
        "args_fingerprint": " ".join(rb_args),
    }
    state_path = resolve_state_path(
        state_file=args.state_file,
        state_dir=_default_state_dir(),
        state_key=args.state_key,
        derived_key_inputs=derived_key_inputs,
    )
    prev = _read_state(state_path)

    if args.verbose:
        print(f"[next_action_alert] prev={prev!r} new={event_id!r} state={state_path}", file=sys.stderr)

    # No change -> no output
    if prev == event_id:
        return 0

    # Change -> update state, then emit exactly one line
    if not args.dry_run:
        _atomic_write(state_path, event_id + "\n")

    if args.emit == "json":
        # print exactly one line (the JSON line from run_backtest)
        print(json_line)
        return 0

    # args.emit == "text": call run_backtest again for canonical one-line text
    text_cmd = [py, str(run_backtest)] + rb_args + ["--next-action"]
    if args.verbose:
        print(f"[next_action_alert] Running: {' '.join(text_cmd)}", file=sys.stderr)

    text_out = _run_cmd(text_cmd)
    text_line = _expect_one_line(text_out, "run_backtest --next-action")

    # Still must be one line on stdout
    print(text_line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
