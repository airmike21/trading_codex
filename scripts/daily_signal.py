#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Preset:
    name: str
    description: str
    run_backtest_args: list[str]
    mode: str = "change_only"
    emit: str = "text"
    state_file: str | None = None
    state_key: str | None = None
    log_csv: str | None = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _expand_user(value: str) -> str:
    return os.path.expandvars(os.path.expanduser(value))


def _expand_known_path_args(args: list[str]) -> list[str]:
    out: list[str] = []
    i = 0
    path_flags = {"--data-dir"}
    while i < len(args):
        arg = args[i]
        out.append(arg)
        if arg in path_flags and i + 1 < len(args):
            out.append(_expand_user(args[i + 1]))
            i += 2
            continue
        i += 1
    return out


def _coerce_preset(name: str, raw: Any) -> Preset:
    if not isinstance(raw, dict):
        raise ValueError(f"Preset {name!r} must be an object.")
    rb_args = raw.get("run_backtest_args")
    if not isinstance(rb_args, list) or not all(isinstance(x, str) for x in rb_args):
        raise ValueError(f"Preset {name!r} missing run_backtest_args list[str].")
    return Preset(
        name=name,
        description=str(raw.get("description", "")),
        run_backtest_args=list(rb_args),
        mode=str(raw.get("mode", "change_only")),
        emit=str(raw.get("emit", "text")),
        state_file=str(raw["state_file"]) if raw.get("state_file") is not None else None,
        state_key=str(raw["state_key"]) if raw.get("state_key") is not None else None,
        log_csv=str(raw["log_csv"]) if raw.get("log_csv") is not None else None,
    )


def _load_presets_json(path: Path) -> dict[str, Preset]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("presets file must be a JSON object.")
    if isinstance(raw.get("presets"), dict):
        src = raw["presets"]
    else:
        src = raw
    out: dict[str, Preset] = {}
    for name, value in src.items():
        if not isinstance(name, str):
            continue
        out[name] = _coerce_preset(name, value)
    return out


def _default_presets_path(repo_root: Path) -> Path:
    local = repo_root / "configs" / "presets.json"
    if local.exists():
        return local
    return repo_root / "configs" / "presets.example.json"


def build_next_action_alert_cmd(
    *,
    repo_root: Path,
    preset: Preset,
    mode: str | None,
    emit: str | None,
    state_file: str | None,
    state_key: str | None,
    log_csv: str | None,
    verbose: bool,
    dry_run: bool,
    no_lock: bool,
    lock_timeout_seconds: float | None,
    lock_stale_seconds: float | None,
) -> list[str]:
    py = sys.executable
    script = repo_root / "scripts" / "next_action_alert.py"

    final_mode = mode or preset.mode or "change_only"
    final_emit = emit or preset.emit or "text"
    final_state_file = state_file or preset.state_file or str(Path.home() / ".trading_codex" / "next_action_alert_state.json")
    final_state_key = state_key or preset.state_key or preset.name
    final_log_csv = log_csv or preset.log_csv

    cmd: list[str] = [
        py,
        str(script),
        "--mode",
        final_mode,
        "--emit",
        final_emit,
        "--state-file",
        _expand_user(final_state_file),
        "--state-key",
        final_state_key,
    ]

    if final_log_csv:
        cmd.extend(["--log-csv", _expand_user(final_log_csv)])
    if verbose:
        cmd.append("--verbose")
    if dry_run:
        cmd.append("--dry-run")
    if no_lock:
        cmd.append("--no-lock")
    if lock_timeout_seconds is not None:
        cmd.extend(["--lock-timeout-seconds", str(lock_timeout_seconds)])
    if lock_stale_seconds is not None:
        cmd.extend(["--lock-stale-seconds", str(lock_stale_seconds)])

    cmd.append("--")
    cmd.extend(_expand_known_path_args(preset.run_backtest_args))
    return cmd


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    parser = argparse.ArgumentParser(description="Daily runner: execute next_action_alert.py for a named preset.")
    parser.add_argument("--preset", required=True, help="Preset name from presets file.")
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Default: configs/presets.json then configs/presets.example.json.",
    )

    parser.add_argument("--mode", choices=["change_only", "change_or_rebalance_due"], default=None)
    parser.add_argument("--emit", choices=["text", "json"], default=None)
    parser.add_argument("--state-file", type=str, default=None)
    parser.add_argument("--state-key", type=str, default=None)
    parser.add_argument("--log-csv", type=str, default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-lock", action="store_true")
    parser.add_argument("--lock-timeout-seconds", type=float, default=None)
    parser.add_argument("--lock-stale-seconds", type=float, default=None)
    args = parser.parse_args(argv)

    presets_path = args.presets_file or _default_presets_path(repo_root)
    if not presets_path.exists():
        print(f"[daily_signal] ERROR: presets file not found: {presets_path}", file=sys.stderr)
        return 2
    try:
        presets = _load_presets_json(presets_path)
    except Exception as exc:
        print(f"[daily_signal] ERROR: failed to parse presets {presets_path}: {exc}", file=sys.stderr)
        return 2
    if args.preset not in presets:
        known = ", ".join(sorted(presets.keys()))
        print(f"[daily_signal] ERROR: unknown preset {args.preset!r}. Known: {known}", file=sys.stderr)
        return 2

    cmd = build_next_action_alert_cmd(
        repo_root=repo_root,
        preset=presets[args.preset],
        mode=args.mode,
        emit=args.emit,
        state_file=args.state_file,
        state_key=args.state_key,
        log_csv=args.log_csv,
        verbose=args.verbose,
        dry_run=args.dry_run,
        no_lock=args.no_lock,
        lock_timeout_seconds=args.lock_timeout_seconds,
        lock_stale_seconds=args.lock_stale_seconds,
    )
    # Preserve next_action_alert stdout behavior exactly.
    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
