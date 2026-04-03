#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import errno
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from trading_codex.execution.ibkr_paper_lane import DEFAULT_IBKR_PAPER_STATE_KEY
from trading_codex.run_archive import resolve_archive_root, write_run_archive

try:
    from scripts import paper_lane_daily_ops
except ImportError:  # pragma: no cover - direct script execution path
    import paper_lane_daily_ops  # type: ignore[no-redef]


DEFAULT_PRESET = paper_lane_daily_ops.DEFAULT_PRESET
DEFAULT_PROVIDER = paper_lane_daily_ops.DEFAULT_PROVIDER
STEP_SCHEMA_NAME = "ibkr_paper_lane_daily_ops_step"
STEP_SCHEMA_VERSION = 1
RUN_SCHEMA_NAME = "ibkr_paper_lane_daily_ops_run"
RUN_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_NAME = "ibkr_paper_lane_daily_ops_log_entry"
SUMMARY_SCHEMA_VERSION = 1
RUN_LOG_COLUMNS = (
    "schema_name",
    "schema_version",
    "run_id",
    "timestamp_chicago",
    "ops_date",
    "overall_result",
    "failed_step",
    "preset",
    "state_key",
    "provider",
    "presets_file",
    "data_dir",
    "ibkr_base_dir",
    "update_exit_code",
    "update_updated_symbols",
    "status_exit_code",
    "status_signal_date",
    "status_signal_action",
    "status_signal_symbol",
    "status_target_shares",
    "status_next_rebalance",
    "status_event_id",
    "status_submission_ready",
    "status_drift_present",
    "status_event_already_applied",
    "status_event_claim_pending",
    "status_pending_claim_result",
    "status_pending_claim_acknowledged_submit",
    "status_pending_claim_reply_required",
    "status_trade_required_count",
    "status_execution_blocker_count",
    "status_execution_blockers",
    "status_archive_manifest_path",
    "apply_exit_code",
    "apply_result",
    "apply_duplicate_event_blocked",
    "apply_event_claim_pending",
    "apply_event_claim_path",
    "apply_event_receipt_path",
    "apply_submitted_order_count",
    "apply_submitted_order_ids",
    "apply_archive_manifest_path",
    "ibkr_state_path",
    "ibkr_ledger_path",
    "ibkr_event_receipts_dir",
    "ibkr_pending_claims_dir",
    "daily_ops_manifest_path",
    "daily_ops_jsonl_path",
    "daily_ops_csv_path",
    "daily_ops_xlsx_path",
    "successful_signal_days_recorded",
)


class IbkrPaperDailyOpsRunLockedError(RuntimeError):
    pass


def _repo_root() -> Path:
    return REPO_ROOT


def _write_csv(path: Path, *, rows: list[dict[str, Any]]) -> None:
    from io import StringIO

    sio = StringIO()
    writer = csv.DictWriter(sio, fieldnames=list(RUN_LOG_COLUMNS), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in RUN_LOG_COLUMNS})
    paper_lane_daily_ops._atomic_write_text(path, sio.getvalue())


def _write_xlsx(path: Path, *, rows: list[dict[str, Any]], timestamp) -> None:  # type: ignore[no-untyped-def]
    payload = paper_lane_daily_ops._build_xlsx_bytes(
        headers=list(RUN_LOG_COLUMNS),
        rows=rows,
        timestamp=timestamp,
    )
    paper_lane_daily_ops._atomic_write_bytes(path, payload)


def resolve_ops_paths(
    *,
    state_key: str,
    archive_root: Path | None = None,
    create: bool,
) -> dict[str, Path]:
    resolved_archive_root = resolve_archive_root(
        preferred_root=paper_lane_daily_ops._expand_path(archive_root),
        create=create,
    )
    ops_root = resolved_archive_root / "stage2_ibkr_paper_ops" / paper_lane_daily_ops._safe_slug(
        state_key,
        fallback=DEFAULT_IBKR_PAPER_STATE_KEY,
    )
    if create:
        ops_root.mkdir(parents=True, exist_ok=True)
    return {
        "archive_root": resolved_archive_root,
        "ops_root": ops_root,
        "jsonl_path": ops_root / "ibkr_paper_lane_daily_ops_log.jsonl",
        "csv_path": ops_root / "ibkr_paper_lane_daily_ops_runs.csv",
        "xlsx_path": ops_root / "ibkr_paper_lane_daily_ops_runs.xlsx",
        "lock_path": ops_root / "ibkr_paper_lane_daily_ops.lock",
    }


@contextmanager
def _daily_ops_run_lock(*, lock_path: Path, state_key: str):
    if fcntl is None:  # pragma: no cover
        raise RuntimeError("ibkr_paper_lane_daily_ops single-instance locking requires a POSIX platform.")

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            lock_file.seek(0)
            holder = lock_file.read().strip()
            message = (
                f"another ibkr_paper_lane_daily_ops run is already active for state_key={state_key}; "
                f"lock_path={lock_path}"
            )
            if holder:
                message = f"{message}; holder={holder}"
            raise IbkrPaperDailyOpsRunLockedError(message) from exc

        lock_file.seek(0)
        lock_file.truncate(0)
        lock_file.write(
            f"pid={os.getpid()} state_key={state_key} acquired_at_chicago={paper_lane_daily_ops._chicago_now().isoformat()}\n"
        )
        lock_file.flush()
        os.fsync(lock_file.fileno())
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def build_update_data_eod_cmd(
    *,
    repo_root: Path,
    provider: str,
    data_dir: Path,
    symbols: list[str],
) -> list[str]:
    return paper_lane_daily_ops.build_update_data_eod_cmd(
        repo_root=repo_root,
        provider=provider,
        data_dir=data_dir,
        symbols=symbols,
    )


def build_ibkr_paper_lane_cmd(
    *,
    repo_root: Path,
    command: str,
    preset_name: str,
    presets_path: Path,
    state_key: str,
    data_dir: Path,
    ibkr_base_dir: Path | None,
    timestamp: str | None,
    ibkr_account_id: str | None,
    ibkr_base_url: str | None,
    ibkr_timeout_seconds: float | None,
    ibkr_verify_ssl: bool | None,
) -> list[str]:
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "ibkr_paper_lane.py"),
        "--emit",
        "json",
        "--state-key",
        state_key,
    ]
    if ibkr_base_dir is not None:
        cmd.extend(["--base-dir", str(ibkr_base_dir)])
    if timestamp is not None:
        cmd.extend(["--timestamp", timestamp])
    if ibkr_account_id:
        cmd.extend(["--ibkr-account-id", ibkr_account_id])
    if ibkr_base_url:
        cmd.extend(["--ibkr-base-url", ibkr_base_url])
    if ibkr_timeout_seconds is not None:
        cmd.extend(["--ibkr-timeout-seconds", str(ibkr_timeout_seconds)])
    if ibkr_verify_ssl is not None:
        cmd.append("--ibkr-verify-ssl" if ibkr_verify_ssl else "--no-ibkr-verify-ssl")
    cmd.extend(
        [
            command,
            "--preset",
            preset_name,
            "--presets-file",
            str(presets_path),
            "--data-dir",
            str(data_dir),
        ]
    )
    return cmd


def _run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=os.environ.copy(),
    )


def _run_step(
    *,
    repo_root: Path,
    step_name: str,
    cmd: list[str],
    expect_json_stdout: bool,
    timestamp,
) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    started = paper_lane_daily_ops._chicago_now()
    proc = _run_process(cmd, repo_root=repo_root)
    completed = paper_lane_daily_ops._chicago_now()
    parse_error: str | None = None
    stdout_json: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None

    if expect_json_stdout:
        if proc.returncode == 0:
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError as exc:
                parse_error = f"stdout JSON decode failed: {exc}"
            else:
                if isinstance(parsed, dict):
                    stdout_json = parsed
                else:
                    parse_error = "stdout JSON payload must be an object."
    else:
        metrics = paper_lane_daily_ops._parse_update_metrics(proc.stderr)

    success = proc.returncode == 0 and parse_error is None
    return {
        "schema_name": STEP_SCHEMA_NAME,
        "schema_version": STEP_SCHEMA_VERSION,
        "step": step_name,
        "timestamp_chicago": timestamp.isoformat(),
        "started_at_chicago": started.isoformat(),
        "completed_at_chicago": completed.isoformat(),
        "duration_seconds": round((completed - started).total_seconds(), 6),
        "command": cmd,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "stdout_json": stdout_json,
        "metrics": metrics,
        "parse_error": parse_error,
        "success": success,
    }


def _signal_payload_from_steps(step_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    status_json = (step_results.get("ibkr_paper_lane_status") or {}).get("stdout_json") or {}
    if isinstance(status_json.get("signal"), dict):
        return dict(status_json["signal"])

    apply_json = (step_results.get("ibkr_paper_lane_apply") or {}).get("stdout_json") or {}
    if isinstance(apply_json.get("signal"), dict):
        return dict(apply_json["signal"])

    return {}


def _build_summary_row(
    *,
    run_id: str,
    timestamp,
    preset_name: str,
    state_key: str,
    provider: str,
    presets_path: Path,
    data_dir: Path,
    ibkr_base_dir: Path | None,
    ops_paths: dict[str, Path],
    manifest_path: Path,
    step_results: dict[str, dict[str, Any]],
    overall_result: str,
    failed_step: str | None,
    successful_signal_days_recorded: int,
) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    update_metrics = (step_results.get("update_data_eod") or {}).get("metrics") or {}
    status_json = (step_results.get("ibkr_paper_lane_status") or {}).get("stdout_json") or {}
    apply_json = (step_results.get("ibkr_paper_lane_apply") or {}).get("stdout_json") or {}
    signal = _signal_payload_from_steps(step_results)
    status_paths = status_json.get("paths") if isinstance(status_json.get("paths"), dict) else {}
    apply_paths = apply_json.get("paths") if isinstance(apply_json.get("paths"), dict) else {}
    paths = status_paths or apply_paths
    pending_claim = (
        status_json.get("pending_event_claim")
        if isinstance(status_json.get("pending_event_claim"), dict)
        else {}
    )
    execution_plan = status_json.get("execution_plan") if isinstance(status_json.get("execution_plan"), dict) else {}
    blockers = execution_plan.get("blockers") if isinstance(execution_plan.get("blockers"), list) else []
    trade_required = status_json.get("trade_required") if isinstance(status_json.get("trade_required"), list) else []
    submitted_orders = apply_json.get("submitted_orders") if isinstance(apply_json.get("submitted_orders"), list) else []
    submitted_order_ids = [
        str(item.get("broker_order_id"))
        for item in submitted_orders
        if isinstance(item, dict) and item.get("broker_order_id")
    ]

    return {
        "schema_name": SUMMARY_SCHEMA_NAME,
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "run_id": run_id,
        "timestamp_chicago": timestamp.isoformat(),
        "ops_date": timestamp.date().isoformat(),
        "overall_result": overall_result,
        "failed_step": failed_step,
        "preset": preset_name,
        "state_key": state_key,
        "provider": provider,
        "presets_file": str(presets_path),
        "data_dir": str(data_dir),
        "ibkr_base_dir": "" if ibkr_base_dir is None else str(ibkr_base_dir),
        "update_exit_code": (step_results.get("update_data_eod") or {}).get("exit_code", ""),
        "update_updated_symbols": update_metrics.get("updated_symbols", ""),
        "status_exit_code": (step_results.get("ibkr_paper_lane_status") or {}).get("exit_code", ""),
        "status_signal_date": signal.get("date", ""),
        "status_signal_action": signal.get("action", ""),
        "status_signal_symbol": signal.get("symbol", ""),
        "status_target_shares": signal.get("target_shares", ""),
        "status_next_rebalance": signal.get("next_rebalance", ""),
        "status_event_id": signal.get("event_id", ""),
        "status_submission_ready": status_json.get("submission_ready", ""),
        "status_drift_present": status_json.get("drift_present", ""),
        "status_event_already_applied": status_json.get("event_already_applied", ""),
        "status_event_claim_pending": status_json.get("event_claim_pending", ""),
        "status_pending_claim_result": pending_claim.get("result", ""),
        "status_pending_claim_acknowledged_submit": pending_claim.get(
            "acknowledged_submit_may_have_reached_ibkr",
            "",
        ),
        "status_pending_claim_reply_required": pending_claim.get("reply_required", ""),
        "status_trade_required_count": len(trade_required) if trade_required else 0,
        "status_execution_blocker_count": len(blockers) if blockers else 0,
        "status_execution_blockers": "|".join(str(item) for item in blockers if str(item)),
        "status_archive_manifest_path": status_json.get("archive_manifest_path", ""),
        "apply_exit_code": (step_results.get("ibkr_paper_lane_apply") or {}).get("exit_code", ""),
        "apply_result": apply_json.get("result", ""),
        "apply_duplicate_event_blocked": apply_json.get("duplicate_event_blocked", ""),
        "apply_event_claim_pending": apply_json.get("event_claim_pending", ""),
        "apply_event_claim_path": apply_json.get("event_claim_path", ""),
        "apply_event_receipt_path": apply_json.get("event_receipt_path", ""),
        "apply_submitted_order_count": len(submitted_orders) if submitted_orders else 0,
        "apply_submitted_order_ids": "|".join(submitted_order_ids),
        "apply_archive_manifest_path": apply_json.get("archive_manifest_path", ""),
        "ibkr_state_path": paths.get("state_path", ""),
        "ibkr_ledger_path": paths.get("ledger_path", ""),
        "ibkr_event_receipts_dir": paths.get("event_receipts_dir", ""),
        "ibkr_pending_claims_dir": paths.get("pending_claims_dir", ""),
        "daily_ops_manifest_path": str(manifest_path),
        "daily_ops_jsonl_path": str(ops_paths["jsonl_path"]),
        "daily_ops_csv_path": str(ops_paths["csv_path"]),
        "daily_ops_xlsx_path": str(ops_paths["xlsx_path"]),
        "successful_signal_days_recorded": successful_signal_days_recorded,
    }


def _render_summary_text(*, run_id: str, summary_row: dict[str, Any]) -> str:
    lines = [
        f"Stage 2 IBKR paper daily ops run {run_id}",
        f"Result: {summary_row['overall_result']}",
        f"Preset: {summary_row['preset']}",
        (
            "Signal: "
            f"{summary_row['status_signal_date']} "
            f"{summary_row['status_signal_action']} "
            f"{summary_row['status_signal_symbol']}"
        ),
        f"Event ID: {summary_row['status_event_id']}",
        f"Update exit: {summary_row['update_exit_code']} (updated_symbols={summary_row['update_updated_symbols']})",
        (
            "IBKR status exit: "
            f"{summary_row['status_exit_code']} "
            f"drift_present={summary_row['status_drift_present']} "
            f"submission_ready={summary_row['status_submission_ready']}"
        ),
        (
            "IBKR apply exit: "
            f"{summary_row['apply_exit_code']} "
            f"result={summary_row['apply_result']} "
            f"duplicate_blocked={summary_row['apply_duplicate_event_blocked']} "
            f"claim_pending={summary_row['apply_event_claim_pending']}"
        ),
        f"Daily ops manifest: {summary_row['daily_ops_manifest_path']}",
        f"JSONL log: {summary_row['daily_ops_jsonl_path']}",
        f"CSV log: {summary_row['daily_ops_csv_path']}",
        f"XLSX workbook: {summary_row['daily_ops_xlsx_path']}",
        f"Successful signal days recorded: {summary_row['successful_signal_days_recorded']}",
    ]
    if summary_row["failed_step"]:
        lines.insert(2, f"Failed step: {summary_row['failed_step']}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the narrow Stage 2 IBKR PaperTrader daily ops routine: update data, "
            "check IBKR paper status, apply the IBKR paper action, and retain forward-evidence artifacts."
        )
    )
    parser.add_argument("--preset", default=DEFAULT_PRESET, help=f"IBKR paper preset name. Default: {DEFAULT_PRESET}")
    parser.add_argument(
        "--provider",
        choices=["stooq", "tiingo"],
        default=DEFAULT_PROVIDER,
        help=f"Data provider for update_data_eod. Default: {DEFAULT_PROVIDER}",
    )
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Defaults to configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument("--state-key", default=DEFAULT_IBKR_PAPER_STATE_KEY, help="IBKR paper lane state key.")
    parser.add_argument("--data-dir", type=Path, default=None, help="Optional data dir override.")
    parser.add_argument("--ibkr-base-dir", type=Path, default=None, help="Optional IBKR paper lane state dir override.")
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Defaults to ~/.trading_codex, then ~/.cache/trading_codex, then /tmp/trading_codex.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format.")
    parser.add_argument(
        "--ibkr-account-id",
        type=str,
        default=None,
        help="IBKR PaperTrader account id. Defaults to IBKR_PAPER_ACCOUNT_ID.",
    )
    parser.add_argument(
        "--ibkr-base-url",
        type=str,
        default=None,
        help="Optional IBKR Web API base URL override.",
    )
    parser.add_argument(
        "--ibkr-timeout-seconds",
        type=float,
        default=None,
        help="Optional IBKR Web API timeout override.",
    )
    parser.add_argument(
        "--ibkr-verify-ssl",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Optional IBKR Web API TLS verification override.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    try:
        timestamp = paper_lane_daily_ops._resolve_timestamp(args.timestamp)
        resolved_ibkr_base_dir = paper_lane_daily_ops._expand_path(args.ibkr_base_dir)
        resolved_presets_path, preset = paper_lane_daily_ops._resolve_preset(
            repo_root=repo_root,
            preset_name=args.preset,
            presets_path=args.presets_file,
        )
        data_dir = paper_lane_daily_ops._resolve_data_dir(repo_root=repo_root, preset=preset, explicit=args.data_dir)
        symbols = paper_lane_daily_ops._resolve_symbols_for_preset(preset)
        ops_paths = resolve_ops_paths(
            state_key=args.state_key,
            archive_root=args.archive_root,
            create=True,
        )
    except Exception as exc:
        print(f"[ibkr_paper_lane_daily_ops] ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        with _daily_ops_run_lock(lock_path=ops_paths["lock_path"], state_key=args.state_key):
            step_specs = [
                (
                    "update_data_eod",
                    build_update_data_eod_cmd(
                        repo_root=repo_root,
                        provider=args.provider,
                        data_dir=data_dir,
                        symbols=symbols,
                    ),
                    False,
                ),
                (
                    "ibkr_paper_lane_status",
                    build_ibkr_paper_lane_cmd(
                        repo_root=repo_root,
                        command="status",
                        preset_name=args.preset,
                        presets_path=resolved_presets_path,
                        state_key=args.state_key,
                        data_dir=data_dir,
                        ibkr_base_dir=resolved_ibkr_base_dir,
                        timestamp=timestamp.isoformat(),
                        ibkr_account_id=args.ibkr_account_id,
                        ibkr_base_url=args.ibkr_base_url,
                        ibkr_timeout_seconds=args.ibkr_timeout_seconds,
                        ibkr_verify_ssl=args.ibkr_verify_ssl,
                    ),
                    True,
                ),
                (
                    "ibkr_paper_lane_apply",
                    build_ibkr_paper_lane_cmd(
                        repo_root=repo_root,
                        command="apply",
                        preset_name=args.preset,
                        presets_path=resolved_presets_path,
                        state_key=args.state_key,
                        data_dir=data_dir,
                        ibkr_base_dir=resolved_ibkr_base_dir,
                        timestamp=timestamp.isoformat(),
                        ibkr_account_id=args.ibkr_account_id,
                        ibkr_base_url=args.ibkr_base_url,
                        ibkr_timeout_seconds=args.ibkr_timeout_seconds,
                        ibkr_verify_ssl=args.ibkr_verify_ssl,
                    ),
                    True,
                ),
            ]

            step_results: dict[str, dict[str, Any]] = {}
            failed_step: str | None = None
            failed_exit_code = 0

            for step_name, cmd, expect_json_stdout in step_specs:
                result = _run_step(
                    repo_root=repo_root,
                    step_name=step_name,
                    cmd=cmd,
                    expect_json_stdout=expect_json_stdout,
                    timestamp=timestamp,
                )
                step_results[step_name] = result
                if not result["success"]:
                    failed_step = step_name
                    failed_exit_code = int(result["exit_code"]) or 2
                    break

            overall_result = "failed" if failed_step else "ok"
            prior_rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
            signal_payload = _signal_payload_from_steps(step_results)
            provisional_summary = {
                "overall_result": overall_result,
                "status_signal_date": signal_payload.get("date"),
            }
            successful_signal_days_recorded = paper_lane_daily_ops._successful_signal_days(
                prior_rows + [provisional_summary]
            )

            manifest_fields = {
                "failed_step": failed_step,
                "preset": args.preset,
                "provider": args.provider,
                "state_key": args.state_key,
            }
            if signal_payload.get("event_id"):
                manifest_fields["event_id"] = signal_payload["event_id"]
            if signal_payload.get("date"):
                manifest_fields["signal_date"] = signal_payload["date"]
            if signal_payload.get("action"):
                manifest_fields["signal_action"] = signal_payload["action"]

            archive = write_run_archive(
                timestamp=timestamp,
                run_kind="ibkr_paper_lane_daily_ops",
                mode=overall_result,
                label=args.state_key,
                identity_parts=[args.state_key, args.preset, args.provider, timestamp.date().isoformat()],
                manifest_fields=manifest_fields,
                json_artifacts={
                    "ibkr_paper_lane_daily_ops_run": {
                        "schema_name": RUN_SCHEMA_NAME,
                        "schema_version": RUN_SCHEMA_VERSION,
                        "timestamp_chicago": timestamp.isoformat(),
                        "preset": args.preset,
                        "provider": args.provider,
                        "presets_file": str(resolved_presets_path),
                        "state_key": args.state_key,
                        "data_dir": str(data_dir),
                        "ibkr_base_dir": None if resolved_ibkr_base_dir is None else str(resolved_ibkr_base_dir),
                        "symbols": symbols,
                        "overall_result": overall_result,
                        "failed_step": failed_step,
                        "step_results": step_results,
                    },
                    **{step_name: payload for step_name, payload in step_results.items()},
                },
                text_artifacts={
                    "summary_text": "\n".join(
                        [
                            f"preset={args.preset}",
                            f"provider={args.provider}",
                            f"state_key={args.state_key}",
                            f"overall_result={overall_result}",
                            f"failed_step={failed_step or ''}",
                        ]
                    )
                },
                preferred_root=ops_paths["archive_root"],
            )

            summary_row = _build_summary_row(
                run_id=archive.manifest["run_id"],
                timestamp=timestamp,
                preset_name=args.preset,
                state_key=args.state_key,
                provider=args.provider,
                presets_path=resolved_presets_path,
                data_dir=data_dir,
                ibkr_base_dir=resolved_ibkr_base_dir,
                ops_paths=ops_paths,
                manifest_path=archive.paths.manifest_path,
                step_results=step_results,
                overall_result=overall_result,
                failed_step=failed_step,
                successful_signal_days_recorded=successful_signal_days_recorded,
            )

            paper_lane_daily_ops._append_jsonl_record(ops_paths["jsonl_path"], summary_row)
            all_rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
            _write_csv(ops_paths["csv_path"], rows=all_rows)
            _write_xlsx(ops_paths["xlsx_path"], rows=all_rows, timestamp=timestamp)

            text_summary = _render_summary_text(run_id=archive.manifest["run_id"], summary_row=summary_row)
            if args.emit == "json":
                print(
                    json.dumps(
                        {
                            "schema_name": RUN_SCHEMA_NAME,
                            "schema_version": RUN_SCHEMA_VERSION,
                            "archive_manifest_path": str(archive.paths.manifest_path),
                            "summary": summary_row,
                            "step_results": step_results,
                        },
                        indent=2,
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                )
            else:
                print(text_summary)

            if failed_step is not None:
                print(
                    f"[ibkr_paper_lane_daily_ops] ERROR: step {failed_step} failed; see {archive.paths.manifest_path}",
                    file=sys.stderr,
                )
                return failed_exit_code
    except IbkrPaperDailyOpsRunLockedError as exc:
        print(f"[ibkr_paper_lane_daily_ops] ERROR: {exc}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
