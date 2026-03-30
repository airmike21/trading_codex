#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

try:
    from scripts import ibkr_paper_lane
except ImportError:  # pragma: no cover - direct script execution path
    import ibkr_paper_lane  # type: ignore[no-redef]

from trading_codex.execution.ibkr_paper_lane import (
    DEFAULT_IBKR_PAPER_BASE_URL,
    DEFAULT_IBKR_PAPER_STATE_KEY,
    apply_ibkr_paper_signal,
    build_ibkr_paper_client,
    build_ibkr_paper_status,
    load_ibkr_paper_client_config,
)
from trading_codex.run_archive import write_run_archive


IBKR_PAPER_BRINGUP_SCHEMA_NAME = "ibkr_paper_bringup_acceptance"
IBKR_PAPER_BRINGUP_SCHEMA_VERSION = 1
BRINGUP_RUN_KIND = "ibkr_paper_bringup_acceptance"


def _repo_root() -> Path:
    return REPO_ROOT


def _resolve_timestamp(value: str | None) -> datetime:
    if value is not None:
        return datetime.fromisoformat(value)
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _exit_code_for_report(report: dict[str, Any]) -> int:
    return 0 if report.get("overall_status") == "ok" else 2


def _validate_mode_flags(args: argparse.Namespace) -> None:
    if args.mode == "apply" and not args.enable_ibkr_paper_apply:
        raise ValueError(
            "Refusing IBKR PaperTrader apply without --enable-ibkr-paper-apply. "
            "This bring-up command defaults fail-closed / no-write."
        )
    if args.mode != "apply" and args.enable_ibkr_paper_apply:
        raise ValueError("--enable-ibkr-paper-apply is only valid with --mode apply.")
    if args.mode != "apply" and args.confirm_replies:
        raise ValueError("--confirm-replies is only valid with --mode apply.")


def _classify_error(*, message: str) -> tuple[str, bool, bool, list[str], list[str]]:
    lowered = message.lower()
    if "verified as paper" in lowered:
        return "blocked", True, False, ["paper_verification_failed"], []
    if "reconciliation plan is blocked:" in lowered:
        suffix = message.split("reconciliation plan is blocked:", 1)[1]
        blockers = [item.strip() for item in suffix.split(",") if item.strip()]
        reasons = ["plan_blockers"] if blockers else ["plan_blockers_unknown"]
        return "blocked", True, True, reasons, blockers
    return "failed", False, False, ["operation_failed"], []


def _status_plan_blockers(status_payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(status_payload, dict):
        return []
    execution_plan = status_payload.get("execution_plan")
    if not isinstance(execution_plan, dict):
        return []
    blockers = execution_plan.get("blockers")
    if not isinstance(blockers, list):
        return []
    return [str(item) for item in blockers if str(item).strip()]


def _dedupe_reasons(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _build_report(
    *,
    generated_at: datetime,
    requested_mode: str,
    state_key: str,
    write_enabled: bool,
    signal_raw: dict[str, Any] | None,
    source_kind: str | None,
    source_label: str | None,
    source_ref: str | None,
    data_dir: Path | None,
    allowed_symbols: list[str] | None,
    config_account_id: str | None,
    config_base_url: str | None,
    config_timeout_seconds: float | None,
    config_verify_ssl: bool | None,
    status_payload: dict[str, Any] | None,
    apply_payload: dict[str, Any] | None,
    error_message: str | None,
) -> dict[str, Any]:
    active_status_payload = status_payload
    if active_status_payload is None and isinstance(apply_payload, dict):
        raw_status_before = apply_payload.get("status_before_apply")
        if isinstance(raw_status_before, dict):
            active_status_payload = raw_status_before

    signal = signal_raw if isinstance(signal_raw, dict) else None
    broker_account = None
    if isinstance(apply_payload, dict):
        raw_account = apply_payload.get("broker_account")
        if isinstance(raw_account, dict):
            broker_account = raw_account
    if broker_account is None and isinstance(active_status_payload, dict):
        raw_account = active_status_payload.get("broker_account")
        if isinstance(raw_account, dict):
            broker_account = raw_account

    drift_present = None
    event_already_applied = None
    event_claim_pending = None
    plan_blockers: list[str] = []
    blocking_reasons: list[str] = []
    lane_reachable = False
    paper_account_verified = False
    overall_status = "ok"
    apply_result = None

    if isinstance(active_status_payload, dict):
        drift_present = bool(active_status_payload.get("drift_present"))
        event_already_applied = bool(active_status_payload.get("event_already_applied"))
        event_claim_pending = bool(active_status_payload.get("event_claim_pending"))
        plan_blockers = _status_plan_blockers(active_status_payload)
        lane_reachable = True
        paper_account_verified = True
        if plan_blockers:
            blocking_reasons.append("plan_blockers")
        if event_already_applied:
            blocking_reasons.append("duplicate_event")
        if event_claim_pending:
            blocking_reasons.append("pending_claim")

    if isinstance(apply_payload, dict):
        apply_result = str(apply_payload.get("result") or "")
        if apply_result == "duplicate_event_refused":
            event_already_applied = True
            blocking_reasons.append("duplicate_event")
        elif apply_result == "claim_pending_manual_clearance_required":
            event_claim_pending = True
            blocking_reasons.append("pending_claim")
        elif apply_result not in {"", "applied", "applied_noop"}:
            blocking_reasons.append(f"apply_result:{apply_result}")

    if error_message is not None:
        classified_status, classified_lane_reachable, classified_paper_verified, reasons, parsed_blockers = _classify_error(
            message=error_message
        )
        overall_status = classified_status
        lane_reachable = lane_reachable or classified_lane_reachable
        paper_account_verified = paper_account_verified or classified_paper_verified
        if parsed_blockers and not plan_blockers:
            plan_blockers = parsed_blockers
        blocking_reasons.extend(reasons)

    blocking_reasons = _dedupe_reasons(blocking_reasons)
    lane_blocked = bool(blocking_reasons)
    if error_message is None and lane_blocked:
        overall_status = "blocked"

    return {
        "schema_name": IBKR_PAPER_BRINGUP_SCHEMA_NAME,
        "schema_version": IBKR_PAPER_BRINGUP_SCHEMA_VERSION,
        "generated_at_chicago": generated_at.isoformat(),
        "requested_mode": requested_mode,
        "execution_mode": "write_enabled" if write_enabled else "no_write",
        "write_enabled": write_enabled,
        "overall_status": overall_status,
        "lane_reachable": lane_reachable,
        "paper_account_verified": paper_account_verified,
        "lane_blocked": lane_blocked,
        "blocking_reasons": blocking_reasons,
        "drift_present": drift_present,
        "event_already_applied": event_already_applied,
        "event_claim_pending": event_claim_pending,
        "plan_blockers": plan_blockers,
        "apply_result": apply_result or None,
        "signal": None if signal is None else dict(signal),
        "source": {
            "kind": source_kind,
            "label": source_label,
            "ref": source_ref,
        },
        "allowed_symbols": [] if allowed_symbols is None else list(allowed_symbols),
        "data_dir": None if data_dir is None else str(data_dir),
        "state_key": state_key,
        "broker_account": (
            broker_account
            if broker_account is not None
            else {
                "account_id": config_account_id,
                "base_url": config_base_url,
                "timeout_seconds": config_timeout_seconds,
                "verify_ssl": config_verify_ssl,
            }
        ),
        "underlying_artifacts": {
            "status_archive_manifest_path": (
                None if not isinstance(active_status_payload, dict) else active_status_payload.get("archive_manifest_path")
            ),
            "apply_archive_manifest_path": (
                None if not isinstance(apply_payload, dict) else apply_payload.get("archive_manifest_path")
            ),
        },
        "status_payload": active_status_payload,
        "apply_payload": apply_payload,
        "error": None if error_message is None else {"message": error_message},
    }


def render_ibkr_paper_bringup_text(report: dict[str, Any]) -> str:
    signal = report.get("signal") or {}
    broker_account = report.get("broker_account") or {}
    blockers = report.get("plan_blockers") or []
    reasons = report.get("blocking_reasons") or []
    lines = [
        f"IBKR PaperTrader bring-up {report['state_key']}",
        f"Mode: {report['requested_mode']} ({report['execution_mode']})",
        f"Result: {report['overall_status']}",
        f"Lane reachable: {'yes' if report['lane_reachable'] else 'no'}",
        f"Paper account verified: {'yes' if report['paper_account_verified'] else 'no'}",
        f"Lane blocked: {'yes' if report['lane_blocked'] else 'no'}",
        f"Drift present: {_render_bool_or_unknown(report.get('drift_present'))}",
        f"Duplicate event state: {_render_bool_or_unknown(report.get('event_already_applied'))}",
        f"Pending claim state: {_render_bool_or_unknown(report.get('event_claim_pending'))}",
        f"Plan blockers: {', '.join(blockers) if blockers else 'none'}",
        f"Blocking reasons: {', '.join(reasons) if reasons else 'none'}",
        (
            f"Signal: {signal.get('date', '-')} {signal.get('strategy', '-')} "
            f"{signal.get('action', '-')} {signal.get('symbol', '-')} event_id={signal.get('event_id', '-')}"
        ),
        f"Allowed symbols: {', '.join(report.get('allowed_symbols') or []) or '-'}",
        f"IBKR account: {broker_account.get('account_id') or '-'}",
        f"IBKR base URL: {broker_account.get('base_url') or DEFAULT_IBKR_PAPER_BASE_URL}",
    ]
    if report.get("apply_result"):
        lines.append(f"Apply result: {report['apply_result']}")
    if report.get("error"):
        lines.append(f"Error: {report['error']['message']}")
    return "\n".join(lines)


def _render_bool_or_unknown(value: object) -> str:
    if value is None:
        return "unknown"
    return "yes" if bool(value) else "no"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the narrow Stage 2 IBKR PaperTrader bring-up / acceptance flow for the primary live candidate. "
            "Defaults to fail-closed no-write evidence gathering."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["preflight", "status", "apply"],
        default="preflight",
        help="Acceptance mode. preflight/status are no-write; apply requires --enable-ibkr-paper-apply.",
    )
    parser.add_argument(
        "--enable-ibkr-paper-apply",
        action="store_true",
        help="Required with --mode apply before this command may submit paper orders to IBKR PaperTrader.",
    )
    parser.add_argument(
        "--confirm-replies",
        action="store_true",
        help="Automatically confirm IBKR order warning replies during apply. Default is fail-closed into a pending claim.",
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Default follows the Trading Codex archive-root fallback chain.",
    )
    parser.add_argument("--base-dir", type=Path, default=None, help="Optional IBKR paper lane state directory override.")
    parser.add_argument("--state-key", type=str, default=DEFAULT_IBKR_PAPER_STATE_KEY, help="IBKR paper lane state key.")
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["json", "text"], default="text", help="Stdout format.")
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
        help=(
            "IBKR Web API base URL. Defaults to "
            f"{os.environ.get('IBKR_WEB_API_BASE_URL', DEFAULT_IBKR_PAPER_BASE_URL)}."
        ),
    )
    parser.add_argument(
        "--ibkr-timeout-seconds",
        type=float,
        default=None,
        help="IBKR Web API timeout. Defaults to IBKR_WEB_API_TIMEOUT_SECONDS or 15.0.",
    )
    parser.add_argument(
        "--ibkr-verify-ssl",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Verify TLS certificates for the IBKR Web API. Defaults to IBKR_WEB_API_VERIFY_SSL or false.",
    )
    ibkr_paper_lane._add_signal_source_args(parser)
    return parser


def main(
    argv: list[str] | None = None,
    *,
    client_factory=build_ibkr_paper_client,
) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)
    generated_at = _resolve_timestamp(args.timestamp)

    signal_raw: dict[str, Any] | None = None
    source_kind: str | None = None
    source_label: str | None = None
    source_ref: str | None = None
    data_dir: Path | None = None
    preset = None
    allowed_symbols: list[str] | None = None
    config = None
    status_payload: dict[str, Any] | None = None
    apply_payload: dict[str, Any] | None = None
    error_message: str | None = None

    try:
        _validate_mode_flags(args)
        (
            signal_raw,
            source_kind,
            source_label,
            source_ref,
            data_dir,
            preset,
        ) = ibkr_paper_lane._resolve_signal_source(
            args=args,
            repo_root=repo_root,
        )
        allowed_symbols = sorted(
            ibkr_paper_lane._resolve_allowed_symbols(raw_value=args.allowed_symbols, preset=preset)
        )
        config = load_ibkr_paper_client_config(
            account_id=args.ibkr_account_id,
            base_url=args.ibkr_base_url,
            verify_ssl=args.ibkr_verify_ssl,
            timeout_seconds=args.ibkr_timeout_seconds,
        )
        client = client_factory(config=config)

        if args.mode in {"preflight", "status"}:
            status_payload = build_ibkr_paper_status(
                client=client,
                config=config,
                allowed_symbols=allowed_symbols,
                state_key=args.state_key,
                base_dir=args.base_dir,
                signal_raw=signal_raw,
                source_kind=source_kind,
                source_label=source_label,
                source_ref=source_ref,
                data_dir=data_dir,
                timestamp=args.timestamp,
            )
        else:
            apply_payload = apply_ibkr_paper_signal(
                client=client,
                config=config,
                allowed_symbols=allowed_symbols,
                state_key=args.state_key,
                base_dir=args.base_dir,
                signal_raw=signal_raw,
                source_kind=source_kind,
                source_label=source_label,
                source_ref=source_ref,
                data_dir=data_dir,
                timestamp=args.timestamp,
                confirm_replies=bool(args.confirm_replies),
            )
    except Exception as exc:
        error_message = str(exc)

    report = _build_report(
        generated_at=generated_at,
        requested_mode=args.mode,
        state_key=args.state_key,
        write_enabled=bool(args.enable_ibkr_paper_apply),
        signal_raw=signal_raw,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        data_dir=data_dir,
        allowed_symbols=allowed_symbols,
        config_account_id=None if config is None else config.account_id,
        config_base_url=None if config is None else config.base_url,
        config_timeout_seconds=None if config is None else config.timeout_seconds,
        config_verify_ssl=None if config is None else config.verify_ssl,
        status_payload=status_payload,
        apply_payload=apply_payload,
        error_message=error_message,
    )

    archived = write_run_archive(
        timestamp=generated_at,
        run_kind=BRINGUP_RUN_KIND,
        mode=args.mode,
        label=args.state_key,
        identity_parts=[
            args.mode,
            report["execution_mode"],
            (signal_raw or {}).get("event_id"),
            None if config is None else config.account_id,
            report["overall_status"],
        ],
        manifest_fields={
            "state_key": args.state_key,
            "requested_mode": args.mode,
            "execution_mode": report["execution_mode"],
            "write_enabled": report["write_enabled"],
            "overall_status": report["overall_status"],
            "lane_reachable": report["lane_reachable"],
            "paper_account_verified": report["paper_account_verified"],
            "lane_blocked": report["lane_blocked"],
            "drift_present": report["drift_present"],
            "duplicate_event_state": report["event_already_applied"],
            "pending_claim_state": report["event_claim_pending"],
            "apply_result": report["apply_result"],
            "event_id": (signal_raw or {}).get("event_id"),
            "account_id": None if config is None else config.account_id,
            "source": {
                "script": "scripts/ibkr_paper_bringup.py",
                "kind": source_kind,
                "label": source_label,
            },
        },
        json_artifacts={"bringup_report": report},
        text_artifacts={"bringup_summary": render_ibkr_paper_bringup_text(report)},
        preferred_root=args.archive_root,
    )

    bringup_report_path = archived.paths.run_dir / archived.manifest["artifact_paths"]["bringup_report"]
    bringup_summary_path = archived.paths.run_dir / archived.manifest["artifact_paths"]["bringup_summary"]
    output = dict(report)
    output["archive"] = {
        "root_dir": str(archived.paths.root_dir),
        "run_dir": str(archived.paths.run_dir),
        "manifest_path": str(archived.paths.manifest_path),
        "bringup_report_path": str(bringup_report_path),
        "bringup_summary_path": str(bringup_summary_path),
    }

    exit_code = _exit_code_for_report(output)
    if args.emit == "json":
        print(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        lines = [
            render_ibkr_paper_bringup_text(output),
            "",
            f"Manifest: {archived.paths.manifest_path}",
            f"Bring-up report: {bringup_report_path}",
            f"Bring-up summary: {bringup_summary_path}",
        ]
        print("\n".join(lines))

    if exit_code != 0:
        print(
            f"[ibkr_paper_bringup] ERROR: acceptance {report['overall_status']}; see {archived.paths.manifest_path}",
            file=sys.stderr,
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
