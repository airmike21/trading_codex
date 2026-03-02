#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


def _default_state_file() -> Path:
    base = Path.home() / ".cache" / "trading_codex"
    return base / "next_action_event_id.txt"


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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Emit a one-line alert ONLY when next_action event_id changes."
    )
    p.add_argument(
        "--state-file",
        type=Path,
        default=_default_state_file(),
        help="Where to store last seen event_id (default: ~/.cache/trading_codex/next_action_event_id.txt)",
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
    prev = _read_state(args.state_file)

    if args.verbose:
        print(f"[next_action_alert] prev={prev!r} new={event_id!r} state={args.state_file}", file=sys.stderr)

    # No change -> no output
    if prev == event_id:
        return 0

    # Change -> update state, then emit exactly one line
    if not args.dry_run:
        _atomic_write(args.state_file, event_id + "\n")

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
