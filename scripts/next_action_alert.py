#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

MODE_CHANGE_ONLY = "change_only"
MODE_CHANGE_OR_REBALANCE_DUE = "change_or_rebalance_due"


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
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
        except OSError:
            dir_fd = None
        if dir_fd is not None:
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


@contextmanager
def _state_lock(path: Path, timeout_seconds: float, stale_seconds: float):
    lock_path = Path(str(path) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    timeout = max(float(timeout_seconds), 0.0)
    stale_after = max(float(stale_seconds), 0.0)
    deadline = time.monotonic() + timeout

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age_seconds = time.time() - lock_path.stat().st_mtime
            except FileNotFoundError:
                continue

            if age_seconds >= stale_after:
                try:
                    lock_path.unlink()
                    continue
                except FileNotFoundError:
                    continue
                except OSError:
                    pass

            if timeout == 0.0 or time.monotonic() >= deadline:
                yield False
                return
            time.sleep(0.05)
            continue
        except OSError:
            # Best effort: if lock handling fails unexpectedly, continue unlocked.
            yield True
            return

        with os.fdopen(fd, "w", encoding="utf-8", newline="") as lock_file:
            lock_file.write(f"pid={os.getpid()} created={time.time():.6f}\n")

        try:
            yield True
        finally:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
        return


def _read_state_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except FileNotFoundError:
        return None


def _load_state(path: Path) -> tuple[dict[str, object], str]:
    raw = _read_state_text(path)
    if raw is None:
        return {}, "missing"

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"last_event_id": raw}, "legacy"

    if isinstance(payload, str):
        return {"last_event_id": payload}, "legacy"
    if isinstance(payload, dict):
        return dict(payload), "dict"
    return {}, "missing"


def _state_event_id(state: dict[str, object]) -> Optional[str]:
    last_event_id = state.get("last_event_id")
    if isinstance(last_event_id, str) and last_event_id:
        return last_event_id

    legacy_event_id = state.get("event_id")
    if isinstance(legacy_event_id, str) and legacy_event_id:
        return legacy_event_id
    return None


def _state_due_fingerprint(state: dict[str, object]) -> Optional[str]:
    due_fingerprint = state.get("last_due_fingerprint")
    if isinstance(due_fingerprint, str) and due_fingerprint:
        return due_fingerprint
    return None


def _serialize_state(
    state: dict[str, object],
    *,
    use_legacy_string: bool,
) -> str:
    if use_legacy_string:
        event_id = _state_event_id(state)
        if event_id is None:
            raise RuntimeError("Cannot write legacy state without event_id.")
        return event_id + "\n"
    return json.dumps(
        state,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ) + "\n"


def _today_chicago() -> date:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Chicago")).date()
    except Exception:
        return date.today()


def _now_chicago_iso() -> str:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0).isoformat()
    except Exception:
        return datetime.now().replace(microsecond=0).isoformat()


def _append_emit_csv_log(
    *,
    log_path: Path,
    payload: dict[str, object],
    emit_kind: str,
    emit_line: str,
    verbose: bool,
) -> None:
    row = {
        "ts_chicago": _now_chicago_iso(),
        "date": "" if payload.get("date") is None else str(payload.get("date")),
        "strategy": "" if payload.get("strategy") is None else str(payload.get("strategy")),
        "action": "" if payload.get("action") is None else str(payload.get("action")),
        "symbol": "" if payload.get("symbol") is None else str(payload.get("symbol")),
        "target_shares": "" if payload.get("target_shares") is None else str(payload.get("target_shares")),
        "resize_new_shares": (
            "" if payload.get("resize_new_shares") is None else str(payload.get("resize_new_shares"))
        ),
        "next_rebalance": "" if payload.get("next_rebalance") is None else str(payload.get("next_rebalance")),
        "event_id": "" if payload.get("event_id") is None else str(payload.get("event_id")),
        "emit_kind": emit_kind,
        "emit_line": emit_line,
    }
    fieldnames = list(row.keys())
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        need_header = (not log_path.exists()) or (log_path.stat().st_size == 0)
        with log_path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if need_header:
                writer.writeheader()
            writer.writerow(row)
            f.flush()
            os.fsync(f.fileno())
    except Exception as exc:
        if verbose:
            print(
                f"[next_action_alert] WARN: failed to append --log-csv {log_path}: {exc}",
                file=sys.stderr,
            )


def _parse_next_rebalance_date(value: object) -> Optional[date]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"Payload next_rebalance must be string or null, got {type(value).__name__}.")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f"Payload next_rebalance is not a valid YYYY-MM-DD date: {value!r}") from exc


def _due_fingerprint(payload: dict[str, object], next_rebalance: str) -> str:
    strategy = "" if payload.get("strategy") is None else str(payload.get("strategy"))
    symbol = "" if payload.get("symbol") is None else str(payload.get("symbol"))
    return f"{strategy}:{symbol}:{next_rebalance}"


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


def _handle_alert_under_lock(
    *,
    args: argparse.Namespace,
    payload: dict[str, object],
    json_line: str,
    rb_args: List[str],
    py: str,
    run_backtest: Path,
    state_path: Path,
) -> int:
    event_id = str(payload["event_id"])
    state, state_kind = _load_state(state_path)
    prev_event_id = _state_event_id(state)
    prev_due_fingerprint = _state_due_fingerprint(state)

    next_rebalance_raw = payload.get("next_rebalance")
    next_rebalance_date = _parse_next_rebalance_date(next_rebalance_raw)
    due_fingerprint = (
        _due_fingerprint(payload, str(next_rebalance_raw))
        if next_rebalance_date is not None
        else None
    )
    due_now = (
        args.mode == MODE_CHANGE_OR_REBALANCE_DUE
        and next_rebalance_date is not None
        and _today_chicago() >= next_rebalance_date
    )
    due_already_emitted = due_fingerprint is not None and prev_due_fingerprint == due_fingerprint
    event_changed = prev_event_id != event_id
    should_emit = event_changed or (
        args.mode == MODE_CHANGE_OR_REBALANCE_DUE
        and due_now
        and not due_already_emitted
    )

    if args.verbose:
        print(
            (
                f"[next_action_alert] prev={prev_event_id!r} new={event_id!r} state={state_path} "
                f"mode={args.mode} due_now={due_now} due_fp={due_fingerprint!r} "
                f"due_already_emitted={due_already_emitted}"
            ),
            file=sys.stderr,
        )

    if not should_emit:
        return 0

    # Emit -> update state unless dry-run, then print exactly one line.
    if not args.dry_run:
        next_state = dict(state)
        next_state["last_event_id"] = event_id
        if due_now and due_fingerprint is not None:
            next_state["last_due_fingerprint"] = due_fingerprint

        use_legacy_string = (
            args.mode == MODE_CHANGE_ONLY
            and state_kind in {"missing", "legacy"}
            and next_state.keys() == {"last_event_id"}
        )
        _atomic_write(
            state_path,
            _serialize_state(next_state, use_legacy_string=use_legacy_string),
        )

    if args.emit == "json":
        if args.log_csv is not None:
            _append_emit_csv_log(
                log_path=args.log_csv,
                payload=payload,
                emit_kind="json",
                emit_line=json_line,
                verbose=args.verbose,
            )
        # print exactly one line (the JSON line from run_backtest)
        print(json_line)
        return 0

    # args.emit == "text": call run_backtest again for canonical one-line text
    text_cmd = [py, str(run_backtest)] + rb_args + ["--next-action"]
    if args.verbose:
        print(f"[next_action_alert] Running: {' '.join(text_cmd)}", file=sys.stderr)

    text_out = _run_cmd(text_cmd)
    text_line = _expect_one_line(text_out, "run_backtest --next-action")

    if args.log_csv is not None:
        _append_emit_csv_log(
            log_path=args.log_csv,
            payload=payload,
            emit_kind="text",
            emit_line=text_line,
            verbose=args.verbose,
        )

    # Still must be one line on stdout
    print(text_line)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Emit a one-line alert when next_action changes (or when rebalance becomes due)."
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
        "--log-csv",
        type=Path,
        default=None,
        help="Optional CSV path. Appends one row only when an alert emits.",
    )
    p.add_argument(
        "--no-lock",
        action="store_true",
        help="Disable best-effort state-file lock.",
    )
    p.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait for lock acquisition (default: 0, do not wait).",
    )
    p.add_argument(
        "--lock-stale-seconds",
        type=float,
        default=3600.0,
        help="Treat existing lockfile as stale after this many seconds (default: 3600).",
    )
    p.add_argument(
        "--emit",
        choices=["json", "text"],
        default="json",
        help="What to print when event_id changes (default: json).",
    )
    p.add_argument(
        "--mode",
        choices=[MODE_CHANGE_ONLY, MODE_CHANGE_OR_REBALANCE_DUE],
        default=MODE_CHANGE_ONLY,
        help=(
            "Alert mode: change_only emits on event change; "
            "change_or_rebalance_due also emits once when next_rebalance is due."
        ),
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
    if args.no_lock:
        return _handle_alert_under_lock(
            args=args,
            payload=payload,
            json_line=json_line,
            rb_args=rb_args,
            py=py,
            run_backtest=run_backtest,
            state_path=state_path,
        )

    with _state_lock(state_path, args.lock_timeout_seconds, args.lock_stale_seconds) as acquired:
        if not acquired:
            return 0
        return _handle_alert_under_lock(
            args=args,
            payload=payload,
            json_line=json_line,
            rb_args=rb_args,
            py=py,
            run_backtest=run_backtest,
            state_path=state_path,
        )


if __name__ == "__main__":
    raise SystemExit(main())
