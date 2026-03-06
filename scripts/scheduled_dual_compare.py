#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

try:
    from scripts import daily_signal
except ImportError:  # pragma: no cover - direct script execution path
    import daily_signal  # type: ignore[no-redef]


DEFAULT_PRESET_NAMES = ("dual_mom_core", "dual_mom_core_vt")
WINDOW_CHOICES = ("morning_0825", "afternoon_1535")


@dataclass(frozen=True)
class Paths:
    base_dir: Path
    logs_dir: Path
    snapshots_dir: Path
    daily_reviews_dir: Path
    runtime_dir: Path
    state_dir: Path
    machine_log_path: Path
    runtime_presets_path: Path
    snapshot_path: Path
    daily_review_path: Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _chicago_zone() -> Any:
    if ZoneInfo is None:  # pragma: no cover
        return None
    return ZoneInfo("America/Chicago")


def _resolve_timestamp(value: str | None) -> datetime:
    chicago = _chicago_zone()
    if value:
        dt = datetime.fromisoformat(value)
        if chicago is not None:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=chicago)
            return dt.astimezone(chicago)
        return dt

    if chicago is not None:
        return datetime.now(chicago).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _timestamp_slug(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S%z")


def _read_presets_source(path: Path) -> dict[str, daily_signal.Preset]:
    return daily_signal._load_presets_json(path)


def _runtime_preset_payload(preset: daily_signal.Preset, *, state_file: Path, log_csv: Path) -> dict[str, Any]:
    return {
        "description": preset.description,
        "mode": preset.mode,
        "emit": preset.emit,
        "state_file": str(state_file),
        "state_key": preset.name,
        "log_csv": str(log_csv),
        "run_backtest_args": list(preset.run_backtest_args),
    }


def _build_paths(base_dir: Path, *, timestamp: datetime, window: str) -> Paths:
    day_slug = timestamp.date().isoformat()
    stamp = _timestamp_slug(timestamp)
    logs_dir = base_dir / "logs"
    snapshots_dir = base_dir / "snapshots" / day_slug
    daily_reviews_dir = base_dir / "daily_reviews"
    runtime_dir = base_dir / "runtime"
    state_dir = base_dir / "state"
    for path in (logs_dir, snapshots_dir, daily_reviews_dir, runtime_dir, state_dir):
        path.mkdir(parents=True, exist_ok=True)

    return Paths(
        base_dir=base_dir,
        logs_dir=logs_dir,
        snapshots_dir=snapshots_dir,
        daily_reviews_dir=daily_reviews_dir,
        runtime_dir=runtime_dir,
        state_dir=state_dir,
        machine_log_path=logs_dir / "scheduled_runs.jsonl",
        runtime_presets_path=runtime_dir / "dual_mom_compare_presets.json",
        snapshot_path=snapshots_dir / f"{stamp}_{window}_dual_compare.json",
        daily_review_path=daily_reviews_dir / f"{day_slug}_dual_compare.md",
    )


def _write_runtime_presets(
    path: Path,
    *,
    selected: list[daily_signal.Preset],
    paths: Paths,
) -> None:
    payload = {"presets": {}}
    for preset in selected:
        payload["presets"][preset.name] = _runtime_preset_payload(
            preset,
            state_file=paths.state_dir / f"{preset.name}.json",
            log_csv=paths.logs_dir / f"{preset.name}_alerts.csv",
        )
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _stdout_line(stdout: str) -> str:
    lines = stdout.splitlines()
    if not lines:
        return ""
    return lines[0]


def _run_command(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root), env=env)


def _append_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8", newline="") as fh:
        for record in records:
            fh.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
            fh.write("\n")


def _load_day_records(path: Path, *, day_slug: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if str(item.get("date_chicago", "")) == day_slug:
            out.append(item)
    return out


def _render_daily_review(
    *,
    records: list[dict[str, Any]],
    day_slug: str,
    updated_at: str,
    base_dir: Path,
) -> str:
    lines = [
        f"# Dual Momentum Scheduled Review {day_slug}",
        "",
        f"- Updated: `{updated_at}`",
        f"- Base dir: `{base_dir}`",
        f"- Machine log: `{base_dir / 'logs' / 'scheduled_runs.jsonl'}`",
        "",
    ]

    latest_by_job: dict[str, dict[str, Any]] = {}
    for record in records:
        job_name = str(record.get("job_name", ""))
        if job_name:
            latest_by_job[job_name] = record

    for window in WINDOW_CHOICES:
        lines.append(f"## {window}")
        lines.append("")
        lines.append("| Preset | Exit | Raw stdout line | Snapshot |")
        lines.append("| --- | ---: | --- | --- |")
        for preset_name in DEFAULT_PRESET_NAMES:
            signal_job = f"{window}_{preset_name}"
            signal_record = latest_by_job.get(signal_job)
            if signal_record is None:
                lines.append(f"| `{preset_name}` | - | - | - |")
                continue
            raw_stdout = str(signal_record.get("stdout_line") or "").replace("|", "\\|")
            snapshot_name = Path(str(signal_record.get("snapshot_path", "-"))).name
            lines.append(
                f"| `{preset_name}` | {signal_record.get('exit_code', '-')} | "
                f"`{raw_stdout}` | `{snapshot_name}` |"
            )
        lines.append("")
        lines.append("| Preset | Summary status | Action | Symbol | Next rebalance | Event ID |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        summary_job = f"{window}_daily_summary_dual_compare"
        summary_record = latest_by_job.get(summary_job)
        summary_rows: dict[str, dict[str, Any]] = {}
        if summary_record is not None and int(summary_record.get("exit_code", 1)) == 0:
            try:
                payload = json.loads(str(summary_record.get("stdout_line") or "[]"))
            except json.JSONDecodeError:
                payload = []
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict) and isinstance(item.get("preset"), str):
                        summary_rows[item["preset"]] = item
        for preset_name in DEFAULT_PRESET_NAMES:
            item = summary_rows.get(preset_name, {})
            lines.append(
                f"| `{preset_name}` | {item.get('status', '-')} | {item.get('action', '-')} | "
                f"{item.get('symbol', '-')} | {item.get('next_rebalance', '-')} | "
                f"`{item.get('event_id', '-')}` |"
            )
        lines.append("")

    return "\n".join(lines) + "\n"


def _job_specs(repo_root: Path, *, runtime_presets_path: Path, window: str) -> list[tuple[str, str | None, list[str]]]:
    py = sys.executable
    return [
        (
            f"{window}_dual_mom_core",
            "dual_mom_core",
            [
                py,
                str(repo_root / "scripts" / "daily_signal.py"),
                "--presets-file",
                str(runtime_presets_path),
                "--preset",
                "dual_mom_core",
            ],
        ),
        (
            f"{window}_dual_mom_core_vt",
            "dual_mom_core_vt",
            [
                py,
                str(repo_root / "scripts" / "daily_signal.py"),
                "--presets-file",
                str(runtime_presets_path),
                "--preset",
                "dual_mom_core_vt",
            ],
        ),
        (
            f"{window}_daily_summary_dual_compare",
            None,
            [
                py,
                str(repo_root / "scripts" / "daily_summary.py"),
                "--presets-file",
                str(runtime_presets_path),
                "--emit",
                "json",
                "--preset",
                "dual_mom_core",
                "--preset",
                "dual_mom_core_vt",
            ],
        ),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run scheduled dual momentum comparison jobs, capture snapshots, and refresh a same-day review artifact."
        )
    )
    parser.add_argument("--window", choices=WINDOW_CHOICES, required=True)
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.home() / ".trading_codex" / "scheduled_runs",
        help="Durable base directory for logs, snapshots, and review artifacts.",
    )
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional preset file override. Default: configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help="Optional ISO timestamp override for deterministic testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    presets_path = args.presets_file or daily_signal._default_presets_path(repo_root)
    if not presets_path.exists():
        print(f"[scheduled_dual_compare] ERROR: presets file not found: {presets_path}", file=sys.stderr)
        return 2

    try:
        presets = _read_presets_source(presets_path)
    except Exception as exc:
        print(f"[scheduled_dual_compare] ERROR: failed to parse presets {presets_path}: {exc}", file=sys.stderr)
        return 2

    missing = [name for name in DEFAULT_PRESET_NAMES if name not in presets]
    if missing:
        known = ", ".join(sorted(presets))
        print(
            f"[scheduled_dual_compare] ERROR: missing preset(s): {', '.join(missing)}. Known: {known}",
            file=sys.stderr,
        )
        return 2

    timestamp = _resolve_timestamp(args.timestamp)
    paths = _build_paths(Path(daily_signal._expand_user(str(args.base_dir))), timestamp=timestamp, window=args.window)
    selected = [presets[name] for name in DEFAULT_PRESET_NAMES]
    _write_runtime_presets(paths.runtime_presets_path, selected=selected, paths=paths)

    records: list[dict[str, Any]] = []
    had_failure = False
    for job_name, preset_name, cmd in _job_specs(repo_root, runtime_presets_path=paths.runtime_presets_path, window=args.window):
        proc = _run_command(cmd, repo_root=repo_root)
        record = {
            "timestamp_chicago": timestamp.isoformat(),
            "date_chicago": timestamp.date().isoformat(),
            "window": args.window,
            "job_name": job_name,
            "preset": preset_name,
            "command": cmd,
            "exit_code": proc.returncode,
            "stdout": proc.stdout,
            "stdout_line": _stdout_line(proc.stdout),
            "stderr": proc.stderr,
            "snapshot_path": str(paths.snapshot_path),
        }
        records.append(record)
        if proc.returncode != 0:
            had_failure = True

    snapshot_payload = {
        "timestamp_chicago": timestamp.isoformat(),
        "window": args.window,
        "presets_file": str(presets_path),
        "runtime_presets_file": str(paths.runtime_presets_path),
        "records": records,
    }
    paths.snapshot_path.write_text(json.dumps(snapshot_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _append_jsonl(paths.machine_log_path, records)

    day_records = _load_day_records(paths.machine_log_path, day_slug=timestamp.date().isoformat())
    review_text = _render_daily_review(
        records=day_records,
        day_slug=timestamp.date().isoformat(),
        updated_at=timestamp.isoformat(),
        base_dir=paths.base_dir,
    )
    paths.daily_review_path.write_text(review_text, encoding="utf-8")

    return 1 if had_failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
