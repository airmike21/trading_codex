#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from scripts import daily_signal, next_action_alert
except ImportError:  # pragma: no cover - direct script execution path
    import daily_signal  # type: ignore[no-redef]
    import next_action_alert  # type: ignore[no-redef]


DEFAULT_PRODUCTION_PRESET_NAMES = ("vm_core", "vm_core_due", "dual_mom_core")
STATUS_NEW = "NEW"
STATUS_UNCHANGED = "UNCHANGED"
STATUS_DUE = "DUE"
STATUS_MISSING_STATE = "MISSING_STATE"
STATUS_ERROR = "ERROR"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_state_file() -> str:
    return str(Path.home() / ".trading_codex" / "next_action_alert_state.json")


def _default_preset_names(presets: dict[str, daily_signal.Preset]) -> list[str]:
    preferred = [name for name in DEFAULT_PRODUCTION_PRESET_NAMES if name in presets]
    if preferred:
        return preferred

    fallback = [
        name
        for name in sorted(presets)
        if not any(token in name.lower() for token in ("demo", "example", "sample", "test"))
    ]
    return fallback or sorted(presets)


def _mode_label(mode: str) -> str:
    if mode == next_action_alert.MODE_CHANGE_OR_REBALANCE_DUE:
        return "due"
    return "change_only"


def _state_path_for_preset(preset: daily_signal.Preset) -> Path:
    final_state_file = preset.state_file or _default_state_file()
    return Path(daily_signal._expand_user(final_state_file))


def _run_next_action_payload(repo_root: Path, preset: daily_signal.Preset) -> tuple[dict[str, Any], str]:
    rb_args = daily_signal._expand_known_path_args(preset.run_backtest_args)
    cmd = [sys.executable, str(repo_root / "scripts" / "run_backtest.py"), *rb_args, "--next-action-json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))
    if proc.returncode != 0:
        msg = (proc.stderr or "") + (proc.stdout or "")
        raise RuntimeError(f"run_backtest failed ({proc.returncode}): {msg.strip()}")

    json_line = next_action_alert._expect_one_line(proc.stdout, "run_backtest --next-action-json")
    payload = json.loads(json_line)
    if not isinstance(payload, dict):
        raise RuntimeError("run_backtest payload must be a JSON object.")
    if "event_id" not in payload:
        raise RuntimeError("run_backtest payload missing event_id.")
    return payload, json_line


def summarize_preset(repo_root: Path, preset: daily_signal.Preset) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "preset": preset.name,
        "mode": preset.mode,
        "mode_label": _mode_label(preset.mode),
        "emit": preset.emit,
        "status": STATUS_ERROR,
        "would_emit": False,
        "action": None,
        "symbol": None,
        "next_rebalance": None,
        "event_id": None,
        "saved_event_id": None,
        "state_path": str(_state_path_for_preset(preset)),
        "error": None,
    }

    try:
        payload, _json_line = _run_next_action_payload(repo_root, preset)
        state_path = _state_path_for_preset(preset)
        state, state_kind = next_action_alert._load_state(state_path)
        prev_event_id = next_action_alert._state_event_id(state)
        prev_due_fingerprint = next_action_alert._state_due_fingerprint(state)

        next_rebalance_raw = payload.get("next_rebalance")
        next_rebalance_date = next_action_alert._parse_next_rebalance_date(next_rebalance_raw)
        due_fingerprint = (
            next_action_alert._due_fingerprint(payload, str(next_rebalance_raw))
            if next_rebalance_date is not None
            else None
        )
        due_now = (
            preset.mode == next_action_alert.MODE_CHANGE_OR_REBALANCE_DUE
            and next_rebalance_date is not None
            and next_action_alert._today_chicago() >= next_rebalance_date
        )
        due_already_emitted = due_fingerprint is not None and prev_due_fingerprint == due_fingerprint
        event_id = str(payload["event_id"])
        event_changed = prev_event_id != event_id
        would_emit = event_changed or (
            preset.mode == next_action_alert.MODE_CHANGE_OR_REBALANCE_DUE
            and due_now
            and not due_already_emitted
        )

        if state_kind == "missing":
            status = STATUS_MISSING_STATE
        elif event_changed:
            status = STATUS_NEW
        elif preset.mode == next_action_alert.MODE_CHANGE_OR_REBALANCE_DUE and due_now and not due_already_emitted:
            status = STATUS_DUE
        else:
            status = STATUS_UNCHANGED

        summary.update(
            {
                "strategy": payload.get("strategy"),
                "action": payload.get("action"),
                "symbol": payload.get("symbol"),
                "next_rebalance": payload.get("next_rebalance"),
                "event_id": event_id,
                "saved_event_id": prev_event_id,
                "state_kind": state_kind,
                "status": status,
                "would_emit": would_emit,
                "due_now": due_now,
                "due_already_emitted": due_already_emitted,
                "saved_due_fingerprint": prev_due_fingerprint,
                "due_fingerprint": due_fingerprint,
            }
        )
    except Exception as exc:
        summary["error"] = str(exc)

    return summary


def render_summary_line(summary: dict[str, Any], *, name_width: int, mode_width: int) -> str:
    preset = str(summary["preset"])
    mode_label = str(summary["mode_label"])
    action = str(summary.get("action") or (STATUS_ERROR if summary.get("status") == STATUS_ERROR else "-"))
    symbol = str(summary.get("symbol") or "-")
    next_rebalance = str(summary.get("next_rebalance") or "-")
    status = str(summary.get("status") or STATUS_ERROR)
    line = (
        f"{preset:<{name_width}} | {mode_label:<{mode_width}} | {action:<6} | "
        f"{symbol:<6} | next={next_rebalance} | status={status}"
    )
    if status == STATUS_ERROR and summary.get("error"):
        err = " ".join(str(summary["error"]).split())
        line += f" | error={err}"
    return line


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only daily summary across production presets without mutating alert state or CSV logs."
    )
    parser.add_argument(
        "--preset",
        action="append",
        default=[],
        help="Preset name to summarize. Repeat to select multiple presets.",
    )
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Default: configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument("--emit", choices=["text", "json"], default="text")
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    presets_path = args.presets_file or daily_signal._default_presets_path(repo_root)
    if not presets_path.exists():
        print(f"[daily_summary] ERROR: presets file not found: {presets_path}", file=sys.stderr)
        return 2

    try:
        presets = daily_signal._load_presets_json(presets_path)
    except Exception as exc:
        print(f"[daily_summary] ERROR: failed to parse presets {presets_path}: {exc}", file=sys.stderr)
        return 2

    selected_names = list(args.preset) if args.preset else _default_preset_names(presets)
    unknown = [name for name in selected_names if name not in presets]
    if unknown:
        known = ", ".join(sorted(presets))
        missing = ", ".join(unknown)
        print(f"[daily_summary] ERROR: unknown preset(s): {missing}. Known: {known}", file=sys.stderr)
        return 2

    summaries = [summarize_preset(repo_root, presets[name]) for name in selected_names]

    if args.emit == "json":
        print(json.dumps(summaries, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    else:
        name_width = max(len(str(item["preset"])) for item in summaries) if summaries else 1
        mode_width = max(len(str(item["mode_label"])) for item in summaries) if summaries else 1
        for item in summaries:
            print(render_summary_line(item, name_width=name_width, mode_width=mode_width))

    return 1 if any(item["status"] == STATUS_ERROR for item in summaries) else 0


if __name__ == "__main__":
    raise SystemExit(main())
