#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import daily_signal
except ImportError:  # pragma: no cover - direct script execution path
    import daily_signal  # type: ignore[no-redef]

from trading_codex.execution.paper_lane import (
    DEFAULT_PAPER_STARTING_CASH,
    DEFAULT_PAPER_STATE_KEY,
    apply_paper_lane_signal,
    build_paper_lane_status,
    initialize_paper_lane,
    render_paper_apply_text,
    render_paper_status_text,
)


def _repo_root() -> Path:
    return REPO_ROOT


def _env_with_src(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return env


def _extract_flag_value(args: list[str], flag: str) -> str | None:
    for index, item in enumerate(args):
        if item == flag and index + 1 < len(args):
            return args[index + 1]
    return None


def _load_signal_from_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Signal JSON file must contain a JSON object.")
    return payload


def _data_dir_for_preset(*, repo_root: Path, preset: daily_signal.Preset) -> Path | None:
    expanded = daily_signal._expand_known_path_args(preset.run_backtest_args)
    data_dir = _extract_flag_value(expanded, "--data-dir")
    if data_dir:
        return Path(data_dir)
    candidate = repo_root / "data"
    return candidate if candidate.exists() else None


def _load_signal_from_preset(
    *,
    repo_root: Path,
    preset_name: str,
    presets_path: Path | None,
) -> tuple[dict[str, Any], daily_signal.Preset, Path]:
    resolved_presets_path = presets_path or daily_signal._default_presets_path(repo_root)
    presets = daily_signal._load_presets_json(resolved_presets_path)
    if preset_name not in presets:
        known = ", ".join(sorted(presets))
        raise ValueError(f"Unknown preset {preset_name!r}. Known: {known}")

    preset = presets[preset_name]
    expanded_args = daily_signal._expand_known_path_args(preset.run_backtest_args)
    cmd = [sys.executable, str(repo_root / "scripts" / "run_backtest.py"), *expanded_args, "--next-action-json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root), env=_env_with_src(repo_root))
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"run_backtest failed for preset {preset_name!r} ({proc.returncode}): {detail}")

    lines = proc.stdout.splitlines()
    if len(lines) != 1:
        raise RuntimeError(f"run_backtest --next-action-json must emit exactly one line. Got: {len(lines)}")
    payload = json.loads(lines[0])
    if not isinstance(payload, dict):
        raise RuntimeError("run_backtest --next-action-json did not return a JSON object.")
    return payload, preset, resolved_presets_path


def _resolve_signal_source(
    *,
    args: argparse.Namespace,
    repo_root: Path,
) -> tuple[dict[str, Any], str, str, str | None, Path | None]:
    if args.preset is not None:
        payload, preset, presets_path = _load_signal_from_preset(
            repo_root=repo_root,
            preset_name=args.preset,
            presets_path=args.presets_file,
        )
        data_dir = args.data_dir or _data_dir_for_preset(repo_root=repo_root, preset=preset)
        return payload, "preset", preset.name, str(presets_path), data_dir

    if args.signal_json_file is None:
        raise ValueError("Either --preset or --signal-json-file is required.")

    signal_path = args.signal_json_file.resolve()
    payload = _load_signal_from_file(signal_path)
    candidate_data_dir = args.data_dir
    if candidate_data_dir is None:
        default_data_dir = repo_root / "data"
        if default_data_dir.exists():
            candidate_data_dir = default_data_dir
    return payload, "signal_json_file", signal_path.stem, str(signal_path), candidate_data_dir


def _add_signal_source_args(parser: argparse.ArgumentParser) -> None:
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--preset", type=str, default=None, help="Preset name to run through run_backtest --next-action-json.")
    source_group.add_argument("--signal-json-file", type=Path, default=None, help="Existing next_action JSON payload file.")
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Defaults to configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Optional data dir used to value current paper holdings when reconciling or applying.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Operate the durable local paper-trading lane for the primary live candidate."
    )
    parser.add_argument("--base-dir", type=Path, default=None, help="Optional paper lane state directory override.")
    parser.add_argument("--state-key", type=str, default=DEFAULT_PAPER_STATE_KEY, help="Paper lane state key.")
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["json", "text"], default="text", help="Stdout format.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize or reset the durable paper lane state.")
    init_parser.add_argument(
        "--starting-cash",
        type=float,
        default=DEFAULT_PAPER_STARTING_CASH,
        help=f"Starting paper cash balance (default: {DEFAULT_PAPER_STARTING_CASH:.2f}).",
    )
    init_parser.add_argument("--reset", action="store_true", help="Reset existing state and clear duplicate event receipts.")

    status_parser = subparsers.add_parser("status", aliases=["reconcile"], help="Show paper state versus the latest target.")
    _add_signal_source_args(status_parser)

    apply_parser = subparsers.add_parser("apply", help="Apply the latest next_action payload to paper state.")
    _add_signal_source_args(apply_parser)

    return parser


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def _print_text(text: str, *, archive_manifest_path: str | None) -> None:
    print(text)
    if archive_manifest_path:
        print(f"Archive manifest: {archive_manifest_path}")


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "init":
            payload = initialize_paper_lane(
                state_key=args.state_key,
                base_dir=args.base_dir,
                starting_cash=float(args.starting_cash),
                timestamp=args.timestamp,
                reset=bool(args.reset),
            )
            if args.emit == "json":
                _print_json(payload)
            else:
                _print_text(
                    "\n".join(
                        [
                            f"Paper lane {payload['paper_state']['state_key']}",
                            f"Cash: {payload['paper_state']['cash']:.2f}",
                            f"State path: {payload['paths']['state_path']}",
                            f"Ledger path: {payload['paths']['ledger_path']}",
                        ]
                    ),
                    archive_manifest_path=payload.get("archive_manifest_path"),
                )
            return 0

        signal_raw, source_kind, source_label, source_ref, data_dir = _resolve_signal_source(
            args=args,
            repo_root=repo_root,
        )

        if args.command in {"status", "reconcile"}:
            payload = build_paper_lane_status(
                state_key=args.state_key,
                base_dir=args.base_dir,
                signal_raw=signal_raw,
                source_kind=source_kind,
                source_label=source_label,
                source_ref=source_ref,
                data_dir=data_dir,
                timestamp=args.timestamp,
            )
            if args.emit == "json":
                _print_json(payload)
            else:
                _print_text(render_paper_status_text(payload), archive_manifest_path=payload.get("archive_manifest_path"))
            return 0

        if args.command == "apply":
            payload = apply_paper_lane_signal(
                state_key=args.state_key,
                base_dir=args.base_dir,
                signal_raw=signal_raw,
                source_kind=source_kind,
                source_label=source_label,
                source_ref=source_ref,
                data_dir=data_dir,
                timestamp=args.timestamp,
            )
            if args.emit == "json":
                _print_json(payload)
            else:
                _print_text(render_paper_apply_text(payload), archive_manifest_path=payload.get("archive_manifest_path"))
            return 0
    except Exception as exc:
        print(f"[paper_lane] ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"[paper_lane] ERROR: unsupported command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
