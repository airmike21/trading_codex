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

from trading_codex.execution import (
    FileBrokerPositionAdapter,
    build_artifact_paths,
    build_execution_plan,
    parse_signal_payload,
    render_markdown,
    resolve_timestamp,
    write_artifacts,
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


def _load_signal_from_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Signal JSON file must contain a JSON object.")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a dry-run execution plan only. No live orders, broker writes, or auto-trading."
    )
    signal_group = parser.add_mutually_exclusive_group(required=True)
    signal_group.add_argument("--preset", help="Load the latest signal by running run_backtest for this preset.")
    signal_group.add_argument("--signal-json-file", type=Path, help="Plan from a precomputed next_action JSON file.")
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path when using --preset. Default: configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument("--positions-file", type=Path, required=True, help="Mock/file broker positions JSON.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.home() / ".trading_codex" / "execution_plans",
        help="Durable dry-run execution plan artifact directory.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format after writing artifacts.")
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    try:
        signal_raw: dict[str, Any]
        source_kind: str
        source_label: str
        source_ref: str | None
        data_dir: Path | None

        if args.preset:
            signal_raw, preset, resolved_presets_path = _load_signal_from_preset(
                repo_root=repo_root,
                preset_name=args.preset,
                presets_path=args.presets_file,
            )
            source_kind = "preset"
            source_label = preset.name
            source_ref = str(resolved_presets_path)
            data_dir = _data_dir_for_preset(repo_root=repo_root, preset=preset)
        else:
            signal_raw = _load_signal_from_file(args.signal_json_file)
            source_kind = "signal_json_file"
            source_label = args.signal_json_file.stem
            source_ref = str(args.signal_json_file)
            data_dir = None

        signal = parse_signal_payload(signal_raw)
        broker_adapter = FileBrokerPositionAdapter(args.positions_file)
        broker_snapshot = broker_adapter.load_snapshot()
        timestamp = resolve_timestamp(args.timestamp)
        plan = build_execution_plan(
            signal=signal,
            broker_snapshot=broker_snapshot,
            source_kind=source_kind,
            source_label=source_label,
            source_ref=source_ref,
            broker_source_ref=str(args.positions_file),
            data_dir=data_dir,
            generated_at=timestamp,
        )

        base_dir = Path(daily_signal._expand_user(str(args.base_dir)))
        artifact_paths = build_artifact_paths(base_dir, timestamp=timestamp, source_label=source_label)
        json_payload = write_artifacts(plan, artifacts=artifact_paths)

        if args.emit == "json":
            print(json.dumps(json_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        else:
            print(render_markdown(plan, artifacts=artifact_paths), end="")
        return 0
    except Exception as exc:
        print(f"[plan_execution] ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
