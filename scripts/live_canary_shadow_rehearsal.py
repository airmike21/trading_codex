#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import run_backtest as run_backtest_script
except ImportError:  # pragma: no cover - direct script execution path
    import run_backtest as run_backtest_script  # type: ignore[no-redef]

from trading_codex.execution import resolve_timestamp
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    DEFAULT_LIVE_CANARY_MAX_LONG_SHARES,
)


REHEARSAL_SCHEMA_NAME = "live_canary_shadow_rehearsal_bundle"
REHEARSAL_SCHEMA_VERSION = 1
REHEARSAL_DIR_NAME = "live_canary_shadow_rehearsals"
CURRENT_STRATEGY = "dual_mom_vol10_cash"
CURRENT_RISK_SYMBOLS = ("SPY", "QQQ", "IWM", "EFA")
CURRENT_DEFENSIVE_SYMBOL = "BIL"


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    src_text = str(SRC_PATH)
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = f"{src_text}:{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = src_text
    return env


def _dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _safe_path_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    safe = safe.strip("._")
    return safe or "unknown"


def _expected_event_id(payload: dict[str, Any]) -> str:
    def g(key: str) -> str:
        value = payload.get(key, "")
        return "" if value is None else str(value)

    return ":".join(
        [
            g("date"),
            g("strategy"),
            g("action"),
            g("symbol"),
            g("target_shares"),
            g("resize_new_shares"),
            g("next_rebalance"),
        ]
    )


def build_rehearsal_bundle_dir(
    base_dir: Path,
    *,
    broker: str,
    account_id: str,
    signal_payload: dict[str, Any],
) -> Path:
    signal_date = str(signal_payload.get("date") or "unknown-date")
    event_id = str(signal_payload.get("event_id") or "unknown-event")
    return (
        base_dir.expanduser().resolve()
        / REHEARSAL_DIR_NAME
        / _safe_path_component(broker)
        / _safe_path_component(account_id)
        / _safe_path_component(signal_date)
        / _safe_path_component(event_id)
    )


def _fsync_directory(path: Path) -> None:
    try:
        dir_fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with tmp_path.open("w", encoding="utf-8") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def _require_json_object(payload: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError(f"{label} must be a JSON object.")
    return payload


def _run_json_command(
    command: list[str],
    *,
    label: str,
    single_line_stdout: bool,
) -> tuple[int, dict[str, Any], str, str]:
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env=_subprocess_env(),
    )

    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if not stdout:
        detail = stderr or "no stdout emitted"
        raise RuntimeError(f"{label} failed to produce JSON output: {detail}")

    raw = stdout
    if single_line_stdout:
        lines = proc.stdout.splitlines()
        if len(lines) != 1:
            raise RuntimeError(
                f"{label} must emit exactly one stdout line of JSON (got {len(lines)} lines)."
            )
        raw = lines[0]

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{label} emitted invalid JSON: {exc}") from exc

    return proc.returncode, _require_json_object(payload, label=label), proc.stdout, proc.stderr


def _validate_candidate_signal(payload: dict[str, Any]) -> None:
    strategy = str(payload.get("strategy") or "")
    if strategy != CURRENT_STRATEGY:
        raise RuntimeError(f"run_backtest produced strategy {strategy!r}, expected {CURRENT_STRATEGY!r}.")

    symbol = str(payload.get("symbol") or "")
    if symbol not in set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS) | {"CASH"}:
        raise RuntimeError(
            f"run_backtest produced symbol {symbol!r}, which is outside the live-canary candidate universe."
        )

    target_shares = payload.get("target_shares")
    if isinstance(target_shares, bool) or not isinstance(target_shares, int):
        raise RuntimeError("run_backtest next_action target_shares must be an integer.")
    if target_shares < 0:
        raise RuntimeError("run_backtest next_action target_shares must not be negative.")

    event_id = str(payload.get("event_id") or "")
    expected_event_id = _expected_event_id(payload)
    if event_id != expected_event_id:
        raise RuntimeError(
            f"run_backtest next_action event_id drifted: expected {expected_event_id!r}, got {event_id!r}."
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only end-to-end live canary shadow rehearsal bundle for the current first-live candidate."
    )
    parser.add_argument(
        "--bundle-base-dir",
        type=Path,
        required=True,
        help="Required explicit base directory under which the deterministic rehearsal bundle will be written.",
    )
    parser.add_argument(
        "--account-id",
        "--live-canary-account",
        dest="account_id",
        type=str,
        required=True,
        help="Required explicit live-canary account binding for readiness/launch/reconcile rehearsal.",
    )
    parser.add_argument(
        "--broker",
        choices=["file", "tastytrade"],
        default="file",
        help="Broker truth source for readiness and reconciliation (default: file).",
    )
    parser.add_argument(
        "--positions-file",
        type=Path,
        default=None,
        help="Required with --broker file. Read-only broker positions snapshot JSON.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=REPO_ROOT / "data",
        help="Directory containing cached parquet bars for the candidate signal run (default: repo ./data).",
    )
    parser.add_argument("--start", default=None, help="Optional inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=None, help="Optional inclusive end date (YYYY-MM-DD).")
    parser.add_argument(
        "--config",
        type=Path,
        default=REPO_ROOT / "config.toml",
        help="Optional TOML config path for rebalance anchor lookup (default: repo config.toml).",
    )
    parser.add_argument(
        "--rebalance-anchor-date",
        default=None,
        help="Optional YYYY-MM-DD anchor passed through to run_backtest.",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help="Optional ISO timestamp override for deterministic readiness/launch/reconcile runs.",
    )
    parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help="Optional tastytrade secrets env file for read-only tastytrade broker truth.",
    )
    parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional device-challenge code override for read-only tastytrade broker truth.",
    )
    parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional device-challenge token override for read-only tastytrade broker truth.",
    )
    parser.add_argument(
        "--emit",
        choices=["json", "text"],
        default="text",
        help="Stdout format for the bundle result pointer.",
    )
    return parser


def _build_signal_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "run_backtest.py"),
        "--strategy",
        CURRENT_STRATEGY,
        "--symbols",
        *CURRENT_RISK_SYMBOLS,
        "--dmv-defensive-symbol",
        CURRENT_DEFENSIVE_SYMBOL,
        "--data-dir",
        str(args.data_dir),
        "--no-plot",
        "--next-action-json",
    ]
    if args.start:
        command.extend(["--start", args.start])
    if args.end:
        command.extend(["--end", args.end])
    if args.config is not None:
        command.extend(["--config", str(args.config)])
    if args.rebalance_anchor_date:
        command.extend(["--rebalance-anchor-date", args.rebalance_anchor_date])
    return command


def _build_state_ops_command(
    *,
    command_name: str,
    timestamp: str,
    signal_json_file: Path | None = None,
    launch_result_file: Path | None = None,
    broker: str,
    account_id: str,
    base_dir: Path,
    positions_file: Path | None,
    secrets_file: Path | None,
    tastytrade_challenge_code: str | None,
    tastytrade_challenge_token: str | None,
) -> list[str]:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "live_canary_state_ops.py"),
        "--emit",
        "json",
        "--timestamp",
        timestamp,
        command_name,
        "--broker",
        broker,
        "--account-id",
        account_id,
        "--base-dir",
        str(base_dir),
    ]
    if signal_json_file is not None:
        command.extend(["--signal-json-file", str(signal_json_file)])
        command.extend(["--arm-live-canary", account_id])
    if launch_result_file is not None:
        command.extend(["--launch-result-file", str(launch_result_file)])
    if positions_file is not None:
        command.extend(["--positions-file", str(positions_file)])
    if secrets_file is not None:
        command.extend(["--secrets-file", str(secrets_file)])
    if tastytrade_challenge_code is not None:
        command.extend(["--tastytrade-challenge-code", str(tastytrade_challenge_code)])
    if tastytrade_challenge_token is not None:
        command.extend(["--tastytrade-challenge-token", str(tastytrade_challenge_token)])
    return command


def _markdown_list(title: str, items: list[str]) -> list[str]:
    lines = [f"## {title}"]
    if items:
        lines.extend(f"- `{item}`" for item in items)
    else:
        lines.append("- none")
    return lines


def _render_summary_markdown(
    *,
    result: dict[str, Any],
    signal_payload: dict[str, Any],
    readiness_payload: dict[str, Any],
    launch_payload: dict[str, Any],
    reconcile_payload: dict[str, Any],
) -> str:
    artifact_paths = result["artifact_paths"]
    lines = [
        "# Live Canary Shadow Rehearsal",
        "",
        "## Overview",
        f"- Strategy: `{signal_payload.get('strategy')}`",
        f"- Account: `{result['account_id']}`",
        f"- Broker truth: `{result['broker']}`",
        f"- As-of date: `{signal_payload.get('date')}`",
        f"- Event ID: `{signal_payload.get('event_id')}`",
        f"- Symbol: `{signal_payload.get('symbol')}`",
        f"- Action: `{signal_payload.get('action')}`",
        f"- Target shares: `{signal_payload.get('target_shares')}`",
        (
            "- Resize shares: "
            f"`{signal_payload.get('resize_prev_shares')} -> {signal_payload.get('resize_new_shares')}`"
        ),
        f"- Next rebalance: `{signal_payload.get('next_rebalance')}`",
        f"- Preview only: `{str(result['preview_only']).lower()}`",
        "",
        "## Flow",
        f"- Readiness verdict: `{readiness_payload.get('verdict')}`",
        f"- Launch outcome: `{launch_payload.get('submit_outcome')}`",
        f"- Reconcile verdict: `{reconcile_payload.get('verdict')}`",
        f"- Readiness exit code: `{result['step_exit_codes']['readiness']}`",
        f"- Launch exit code: `{result['step_exit_codes']['launch']}`",
        f"- Reconcile exit code: `{result['step_exit_codes']['reconcile']}`",
        f"- Launch operator message: `{launch_payload.get('operator_message')}`",
        "",
    ]

    lines.extend(_markdown_list("Blockers", list(result["blocking_reasons"])))
    lines.append("")
    lines.extend(_markdown_list("Warnings", list(result["warnings"])))
    lines.append("")
    lines.extend(
        [
            "## Live Canary Assumptions",
            f"- Allowed symbols: `{', '.join(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS)}`",
            f"- Max long shares: `{DEFAULT_LIVE_CANARY_MAX_LONG_SHARES}`",
            "",
            "## Artifacts",
            f"- Bundle dir: `{result['bundle_dir']}`",
            f"- Signal copy: `{artifact_paths['signal_json']}`",
            f"- Readiness copy: `{artifact_paths['readiness_json']}`",
            f"- Launch copy: `{artifact_paths['launch_json']}`",
            f"- Reconcile copy: `{artifact_paths['reconcile_json']}`",
            f"- Summary: `{artifact_paths['summary_md']}`",
            f"- Live canary base dir: `{artifact_paths['live_canary_base_dir']}`",
            f"- Underlying launch artifact: `{artifact_paths['launch_result_path']}`",
            f"- Underlying reconcile artifact: `{artifact_paths['reconcile_result_path']}`",
        ]
    )
    if artifact_paths.get("positions_file") is not None:
        lines.append(f"- Positions file: `{artifact_paths['positions_file']}`")
    lines.extend(
        [
            "",
            "## Commands",
            f"- Signal: `{result['commands']['signal']}`",
            f"- Readiness: `{result['commands']['readiness']}`",
            f"- Launch: `{result['commands']['launch']}`",
            f"- Reconcile: `{result['commands']['reconcile']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_result_text(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"Bundle dir {result['bundle_dir']}",
            f"Summary {result['artifact_paths']['summary_md']}",
            (
                f"Signal {result['strategy']} {result['action']} {result['symbol']} "
                f"target={result['target_shares']} next={result['next_rebalance']}"
            ),
            (
                f"Readiness {result['readiness_verdict']} | "
                f"Launch {result['launch_outcome']} | "
                f"Reconcile {result['reconcile_verdict']}"
            ),
        ]
    )


def run_rehearsal(args: argparse.Namespace) -> dict[str, Any]:
    if args.broker == "file" and args.positions_file is None:
        raise ValueError("--positions-file is required when --broker file.")
    if args.broker == "tastytrade" and args.positions_file is not None:
        raise ValueError("--positions-file cannot be used with --broker tastytrade.")

    args.bundle_base_dir = args.bundle_base_dir.expanduser()
    args.data_dir = args.data_dir.expanduser()
    args.config = args.config.expanduser() if args.config is not None else None
    args.positions_file = args.positions_file.expanduser() if args.positions_file is not None else None
    args.secrets_file = args.secrets_file.expanduser() if args.secrets_file is not None else None

    cfg = run_backtest_script.load_run_backtest_config(args.config)
    if args.rebalance_anchor_date is None and cfg.rebalance_anchor_date is not None:
        args.rebalance_anchor_date = cfg.rebalance_anchor_date

    timestamp = resolve_timestamp(args.timestamp)
    timestamp_iso = timestamp.isoformat()

    signal_command = _build_signal_command(args)
    signal_exit_code, signal_payload, _signal_stdout, _signal_stderr = _run_json_command(
        signal_command,
        label="run_backtest --next-action-json",
        single_line_stdout=True,
    )
    if signal_exit_code != 0:
        raise RuntimeError(f"run_backtest exited with {signal_exit_code}.")
    _validate_candidate_signal(signal_payload)

    bundle_dir = build_rehearsal_bundle_dir(
        args.bundle_base_dir,
        broker=args.broker,
        account_id=args.account_id,
        signal_payload=signal_payload,
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)
    live_canary_base_dir = bundle_dir / "live_canary_state"

    signal_path = bundle_dir / "signal.json"
    readiness_path = bundle_dir / "readiness.json"
    launch_path = bundle_dir / "launch.json"
    reconcile_path = bundle_dir / "reconcile.json"
    summary_path = bundle_dir / "summary.md"

    _atomic_write_json(signal_path, signal_payload)

    readiness_command = _build_state_ops_command(
        command_name="readiness",
        timestamp=timestamp_iso,
        signal_json_file=signal_path,
        launch_result_file=None,
        broker=args.broker,
        account_id=args.account_id,
        base_dir=live_canary_base_dir,
        positions_file=args.positions_file,
        secrets_file=args.secrets_file,
        tastytrade_challenge_code=args.tastytrade_challenge_code,
        tastytrade_challenge_token=args.tastytrade_challenge_token,
    )
    readiness_exit_code, readiness_payload, _readiness_stdout, _readiness_stderr = _run_json_command(
        readiness_command,
        label="live_canary_state_ops readiness",
        single_line_stdout=False,
    )
    _atomic_write_json(readiness_path, readiness_payload)

    launch_command = _build_state_ops_command(
        command_name="launch",
        timestamp=timestamp_iso,
        signal_json_file=signal_path,
        launch_result_file=None,
        broker=args.broker,
        account_id=args.account_id,
        base_dir=live_canary_base_dir,
        positions_file=args.positions_file,
        secrets_file=args.secrets_file,
        tastytrade_challenge_code=args.tastytrade_challenge_code,
        tastytrade_challenge_token=args.tastytrade_challenge_token,
    )
    launch_exit_code, launch_payload, _launch_stdout, _launch_stderr = _run_json_command(
        launch_command,
        label="live_canary_state_ops launch",
        single_line_stdout=False,
    )
    _atomic_write_json(launch_path, launch_payload)

    launch_result_path = Path(
        _require_json_object(launch_payload.get("artifact_paths"), label="launch artifact_paths")["result_path"]
    )
    reconcile_command = _build_state_ops_command(
        command_name="reconcile",
        timestamp=timestamp_iso,
        signal_json_file=None,
        launch_result_file=launch_result_path,
        broker=args.broker,
        account_id=args.account_id,
        base_dir=live_canary_base_dir,
        positions_file=args.positions_file,
        secrets_file=args.secrets_file,
        tastytrade_challenge_code=args.tastytrade_challenge_code,
        tastytrade_challenge_token=args.tastytrade_challenge_token,
    )
    reconcile_exit_code, reconcile_payload, _reconcile_stdout, _reconcile_stderr = _run_json_command(
        reconcile_command,
        label="live_canary_state_ops reconcile",
        single_line_stdout=False,
    )
    _atomic_write_json(reconcile_path, reconcile_payload)

    launch_submit_result = launch_payload.get("submit_result")
    launch_submit_result = launch_submit_result if isinstance(launch_submit_result, dict) else {}
    launch_submit_blockers = launch_submit_result.get("blockers")
    launch_submit_blockers = (
        [str(item) for item in launch_submit_blockers if item]
        if isinstance(launch_submit_blockers, list)
        else []
    )
    launch_submit_warnings = launch_submit_result.get("warnings")
    launch_submit_warnings = (
        [str(item) for item in launch_submit_warnings if item]
        if isinstance(launch_submit_warnings, list)
        else []
    )

    blocking_reasons = _dedupe_preserve(
        [str(item) for item in readiness_payload.get("blocking_reasons", []) if item]
        + launch_submit_blockers
        + [str(item) for item in reconcile_payload.get("blocking_reasons", []) if item]
    )
    warnings = _dedupe_preserve(
        [str(item) for item in readiness_payload.get("warnings", []) if item]
        + launch_submit_warnings
        + [str(item) for item in reconcile_payload.get("warnings", []) if item]
    )

    result = {
        "schema_name": REHEARSAL_SCHEMA_NAME,
        "schema_version": REHEARSAL_SCHEMA_VERSION,
        "bundle_dir": str(bundle_dir),
        "account_id": args.account_id,
        "broker": args.broker,
        "strategy": str(signal_payload.get("strategy")),
        "as_of_date": str(signal_payload.get("date")),
        "event_id": str(signal_payload.get("event_id")),
        "symbol": str(signal_payload.get("symbol")),
        "action": str(signal_payload.get("action")),
        "target_shares": signal_payload.get("target_shares"),
        "next_rebalance": signal_payload.get("next_rebalance"),
        "preview_only": True,
        "readiness_verdict": readiness_payload.get("verdict"),
        "launch_outcome": launch_payload.get("submit_outcome"),
        "reconcile_verdict": reconcile_payload.get("verdict"),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "artifact_paths": {
            "signal_json": str(signal_path),
            "readiness_json": str(readiness_path),
            "launch_json": str(launch_path),
            "reconcile_json": str(reconcile_path),
            "summary_md": str(summary_path),
            "live_canary_base_dir": str(live_canary_base_dir),
            "launch_result_path": str(launch_result_path),
            "reconcile_result_path": str(
                _require_json_object(reconcile_payload.get("artifact_paths"), label="reconcile artifact_paths")["result_path"]
            ),
            "positions_file": None if args.positions_file is None else str(args.positions_file),
        },
        "step_exit_codes": {
            "signal": signal_exit_code,
            "readiness": readiness_exit_code,
            "launch": launch_exit_code,
            "reconcile": reconcile_exit_code,
        },
        "commands": {
            "signal": shlex.join(signal_command),
            "readiness": shlex.join(readiness_command),
            "launch": shlex.join(launch_command),
            "reconcile": shlex.join(reconcile_command),
        },
    }
    _atomic_write_text(
        summary_path,
        _render_summary_markdown(
            result=result,
            signal_payload=signal_payload,
            readiness_payload=readiness_payload,
            launch_payload=launch_payload,
            reconcile_payload=reconcile_payload,
        ),
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_rehearsal(args)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.emit == "json":
        print(json.dumps(result, separators=(",", ":"), ensure_ascii=False))
    else:
        print(_render_result_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
