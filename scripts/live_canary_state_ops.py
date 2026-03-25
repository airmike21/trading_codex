#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import live_canary_guardrails
except ImportError:  # pragma: no cover - direct script execution path
    import live_canary_guardrails  # type: ignore[no-redef]

from trading_codex.execution import resolve_timestamp
from trading_codex.execution.live_canary_reconcile import (
    LIVE_CANARY_RECONCILIATION_SCHEMA_NAME,
    LIVE_CANARY_RECONCILIATION_VERDICT_BLOCKED,
    build_live_canary_reconciliation,
)
from trading_codex.execution.live_canary_readiness import build_live_canary_readiness
from trading_codex.execution.live_canary_state_ops import (
    LIVE_CANARY_RELEASE_APPROVAL_OPERATION_SCHEMA_NAME,
    apply_live_canary_state_clear,
    apply_live_canary_release_approval,
    build_live_canary_state_status,
    preview_live_canary_release_approval,
    preview_live_canary_state_clear,
    resolve_live_canary_state_base_dir,
)
from trading_codex.execution.secrets import DEFAULT_TASTYTRADE_SECRETS_PATH
from trading_codex.run_archive import build_run_id


LIVE_CANARY_LAUNCH_SCHEMA_NAME = "live_canary_launch_result"
LIVE_CANARY_LAUNCH_SCHEMA_VERSION = 1
LIVE_CANARY_LAUNCH_DUPLICATE_ONLY_GATES = (
    "input_readiness",
    "account_binding",
    "manual_arming",
    "session_readiness",
    "canary_order_readiness",
    "affordability",
    "pre_live_approval",
    "operator_state_ops",
    "other_guardrails",
)


def _add_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--account-id", type=str, required=True, help="Explicit live-canary account binding to inspect.")
    parser.add_argument("--strategy", type=str, default=None, help="Optional strategy scope.")
    parser.add_argument("--signal-date", type=str, default=None, help="Optional signal date scope (YYYY-MM-DD).")
    parser.add_argument("--event-id", type=str, default=None, help="Optional exact live-canary event_id scope.")
    parser.add_argument(
        "--live-submission-fingerprint",
        type=str,
        default=None,
        help="Optional exact broker live-submit fingerprint scope.",
    )


def _add_base_dir_arg(parser: argparse.ArgumentParser, *, default: object = None) -> None:
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=default,
        help="Optional live-canary state directory. Default follows the Trading Codex archive-root fallback chain.",
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


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with tmp_path.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect live-canary readiness/state and explicitly archive scoped live-canary state when required."
    )
    _add_base_dir_arg(parser, default=None)
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["json", "text"], default="text", help="Stdout format.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser(
        "status",
        aliases=["inspect"],
        help="Read-only live-canary state inspection.",
    )
    _add_base_dir_arg(status_parser, default=argparse.SUPPRESS)
    _add_scope_args(status_parser)

    clear_parser = subparsers.add_parser(
        "clear",
        aliases=["reset"],
        help="Dry-run by default. Explicitly archive scoped state and append submit-tracking clear markers.",
    )
    _add_base_dir_arg(clear_parser, default=argparse.SUPPRESS)
    _add_scope_args(clear_parser)
    clear_parser.add_argument(
        "--clear",
        dest="clear_scopes",
        action="append",
        choices=["event", "session", "submit-tracking"],
        required=True,
        help="Repeatable explicit clear scope. No wildcard clear-all is supported.",
    )
    clear_parser.add_argument("--reason", type=str, default=None, help="Optional operator reason recorded in clear audit metadata.")
    clear_parser.add_argument("--apply", action="store_true", help="Apply the clear. Default is preview-only.")
    clear_parser.add_argument(
        "--confirm",
        type=str,
        default=None,
        help="Required with --apply. Must exactly match the preview confirmation token.",
    )

    approve_parser = subparsers.add_parser(
        "approve",
        aliases=["release"],
        help="Validate a shadow rehearsal bundle and write the durable pre-live approval artifact when --apply is supplied.",
    )
    _add_base_dir_arg(approve_parser, default=argparse.SUPPRESS)
    approve_parser.add_argument(
        "--account-id",
        type=str,
        required=True,
        help="Explicit live-canary account binding being approved for live submit.",
    )
    approve_parser.add_argument(
        "--bundle-dir",
        type=Path,
        required=True,
        help="Exact live-canary shadow rehearsal bundle directory to validate and bind into the approval artifact.",
    )
    approve_parser.add_argument("--reason", type=str, default=None, help="Optional approval note recorded in the durable artifact.")
    approve_parser.add_argument("--operator", type=str, default=None, help="Optional operator identity text recorded in the durable artifact.")
    approve_parser.add_argument("--apply", action="store_true", help="Write the approval artifact. Default is preview-only validation.")

    readiness_parser = subparsers.add_parser(
        "readiness",
        aliases=["preflight"],
        help="Read-only live-canary readiness/preflight verdict with exact blockers and next actions.",
    )
    _add_base_dir_arg(readiness_parser, default=argparse.SUPPRESS)
    readiness_parser.add_argument("--signal-json-file", type=Path, required=True, help="Existing next_action JSON payload.")
    readiness_parser.add_argument(
        "--broker",
        choices=["file", "tastytrade"],
        default="file",
        help="Broker snapshot source. Use 'file' for tests/reviews; 'tastytrade' for live-capable preflight.",
    )
    readiness_parser.add_argument("--positions-file", type=Path, default=None, help="Required with --broker file.")
    readiness_parser.add_argument(
        "--account-id",
        "--live-canary-account",
        dest="account_id",
        type=str,
        required=True,
        help="Required explicit live-canary account binding to evaluate.",
    )
    readiness_parser.add_argument(
        "--arm-live-canary",
        type=str,
        default=None,
        help="Manual arming token. Must exactly match the bound account for a ready verdict.",
    )
    readiness_parser.add_argument(
        "--ack-unmanaged-holdings",
        action="store_true",
        help="Allow managed-sleeve planning when unmanaged holdings exist. Readiness still remains fail-closed.",
    )
    readiness_parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Assess preview/read-only rehearsal readiness without requiring a durable pre-live approval artifact.",
    )
    readiness_parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional device-challenge code for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_CODE.",
    )
    readiness_parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional device-challenge token override for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_TOKEN.",
    )
    readiness_parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help=f"Optional tastytrade secrets env file. If omitted, auto-loads {DEFAULT_TASTYTRADE_SECRETS_PATH} when present.",
    )

    launch_parser = subparsers.add_parser(
        "launch",
        help="Single operator-facing live-canary workflow: run readiness first, then guarded live submit only when explicitly requested and ready.",
    )
    _add_base_dir_arg(launch_parser, default=argparse.SUPPRESS)
    launch_parser.add_argument("--signal-json-file", type=Path, required=True, help="Existing next_action JSON payload.")
    launch_parser.add_argument(
        "--broker",
        choices=["file", "tastytrade"],
        default="file",
        help="Broker snapshot source. Use 'file' for tests/reviews; 'tastytrade' for live-capable launch.",
    )
    launch_parser.add_argument("--positions-file", type=Path, default=None, help="Required with --broker file.")
    launch_parser.add_argument(
        "--account-id",
        "--live-canary-account",
        dest="account_id",
        type=str,
        required=True,
        help="Required explicit live-canary account binding to evaluate and, if ready, submit.",
    )
    launch_parser.add_argument(
        "--live-submit",
        action="store_true",
        help="Attempt the guarded live-submit path only when readiness is ready and the account is explicitly armed.",
    )
    launch_parser.add_argument(
        "--arm-live-canary",
        type=str,
        default=None,
        help="Manual arming token. Must exactly match the bound account for a live submit attempt.",
    )
    launch_parser.add_argument(
        "--ack-unmanaged-holdings",
        action="store_true",
        help="Allow managed-sleeve planning when unmanaged holdings exist. Launch still remains fail-closed.",
    )
    launch_parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional device-challenge code for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_CODE.",
    )
    launch_parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional device-challenge token override for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_TOKEN.",
    )
    launch_parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help=f"Optional tastytrade secrets env file. If omitted, auto-loads {DEFAULT_TASTYTRADE_SECRETS_PATH} when present.",
    )

    reconcile_parser = subparsers.add_parser(
        "reconcile",
        aliases=["closeout"],
        help="Read-only post-launch live-canary reconciliation against an existing launch result artifact.",
    )
    _add_base_dir_arg(reconcile_parser, default=argparse.SUPPRESS)
    reconcile_parser.add_argument(
        "--launch-result-file",
        type=Path,
        required=True,
        help="Existing live_canary_launch_result JSON artifact to reconcile.",
    )
    reconcile_parser.add_argument(
        "--broker",
        choices=["file", "tastytrade"],
        default="file",
        help="Broker truth source. Use 'file' for tests/reviews; 'tastytrade' for live-capable read-only reconciliation.",
    )
    reconcile_parser.add_argument("--positions-file", type=Path, default=None, help="Required with --broker file.")
    reconcile_parser.add_argument(
        "--orders-file",
        type=Path,
        default=None,
        help="Required with --broker file when the launch artifact requires broker order truth.",
    )
    reconcile_parser.add_argument(
        "--account-id",
        "--live-canary-account",
        dest="account_id",
        type=str,
        default=None,
        help="Optional explicit account assertion. Must match the launch artifact when provided.",
    )
    reconcile_parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional device-challenge code for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_CODE.",
    )
    reconcile_parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional device-challenge token override for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_TOKEN.",
    )
    reconcile_parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help=f"Optional tastytrade secrets env file. If omitted, auto-loads {DEFAULT_TASTYTRADE_SECRETS_PATH} when present.",
    )
    return parser


def _render_scope(scope: dict[str, Any]) -> str:
    return " ".join(
        [
            f"account={scope.get('account_id') or '-'}",
            f"strategy={scope.get('strategy') or '-'}",
            f"signal_date={scope.get('signal_date') or '-'}",
            f"event_id={scope.get('event_id') or '-'}",
            f"fingerprint={scope.get('live_submission_fingerprint') or '-'}",
        ]
    )


def _render_artifact_line(artifact: dict[str, Any]) -> str:
    bits = [
        artifact["artifact_kind"],
        f"path={artifact['path']}",
        f"clear_scope={artifact.get('clear_scope') or '-'}",
    ]
    fingerprint = artifact.get("live_submission_fingerprint")
    if fingerprint:
        bits.append(f"fingerprint={fingerprint}")
    scope_precision = artifact.get("scope_precision")
    if scope_precision:
        bits.append(f"scope_precision={scope_precision}")
    summary = artifact.get("summary") or {}
    result = summary.get("result")
    if result:
        bits.append(f"result={result}")
    response_text = summary.get("response_text")
    if response_text:
        bits.append(f"response={response_text}")
    blocking_reason = artifact.get("blocking_reason")
    if blocking_reason:
        bits.append(f"blocking_reason={blocking_reason}")
    recovery_hint = artifact.get("recovery_hint")
    if recovery_hint:
        bits.append(f"recovery_hint={recovery_hint}")
    return "- " + " | ".join(bits)


def _render_status_text(payload: dict[str, Any]) -> str:
    lines = [
        f"Scope { _render_scope(payload['scope']) }",
        f"Base dir {payload['base_dir']}",
        f"Blocking artifacts {payload['summary']['blocking_artifact_count']} of {payload['summary']['artifact_count']}",
    ]

    blocking_artifacts = payload.get("blocking_artifacts", [])
    if blocking_artifacts:
        lines.append("Blocking:")
        lines.extend(_render_artifact_line(artifact) for artifact in blocking_artifacts)
    else:
        lines.append("Blocking: none")

    artifacts = payload.get("artifacts", [])
    if artifacts:
        lines.append("Artifacts:")
        lines.extend(_render_artifact_line(artifact) for artifact in artifacts)
    else:
        lines.append("Artifacts: none")

    release_approval = payload.get("release_approval")
    if isinstance(release_approval, dict) and release_approval.get("assessed"):
        lines.append(
            "Release approval "
            f"present={str(bool(release_approval.get('present'))).lower()} "
            f"valid={str(bool(release_approval.get('valid'))).lower()} "
            f"path={release_approval.get('approval_path') or '-'}"
        )
        approval_blockers = release_approval.get("blocking_reasons") or []
        if approval_blockers:
            lines.append("Release approval blockers:")
            lines.extend(f"- {reason}" for reason in approval_blockers)
    return "\n".join(lines)


def _render_release_approval_text(payload: dict[str, Any]) -> str:
    approval = payload["approval"]
    lines = [
        f"{'Apply' if payload.get('apply') else 'Preview'} release approval",
        f"Scope { _render_scope(payload['scope']) }",
        f"Bundle dir {approval['bundle_dir']}",
        f"Approval path {payload['approval_path']}",
    ]
    if approval.get("reason"):
        lines.append(f"Reason {approval['reason']}")
    if approval.get("operator_id"):
        lines.append(f"Operator {approval['operator_id']}")
    return "\n".join(lines)


def _render_operation_line(operation: dict[str, Any]) -> str:
    bits = [operation["operation"], f"artifact_kind={operation['artifact_kind']}"]
    fingerprint = operation.get("live_submission_fingerprint")
    if fingerprint:
        bits.append(f"fingerprint={fingerprint}")
    source_path = operation.get("source_path")
    if source_path:
        bits.append(f"source={source_path}")
    archive_path = operation.get("archive_path")
    if archive_path:
        bits.append(f"archive={archive_path}")
    ledger_path = operation.get("ledger_path")
    if ledger_path:
        bits.append(f"ledger={ledger_path}")
    result = operation.get("result")
    if result:
        bits.append(f"result={result}")
    reason = operation.get("reason")
    if reason:
        bits.append(f"reason={reason}")
    return "- " + " | ".join(bits)


def _render_clear_text(payload: dict[str, Any]) -> str:
    operations = payload.get("applied_operations") if payload.get("apply") else payload.get("planned_operations")
    mode = "Apply" if payload.get("apply") else "Preview"
    lines = [
        f"{mode} clear {','.join(payload['clear_scopes'])}",
        f"Scope { _render_scope(payload['scope']) }",
        f"Confirmation token {payload['confirmation_token']}",
    ]
    if operations:
        lines.append("Operations:")
        lines.extend(_render_operation_line(operation) for operation in operations)
    else:
        lines.append("Operations: none")
    return "\n".join(lines)


def _render_readiness_gate_line(gate: dict[str, Any]) -> str:
    bits = [f"{gate['gate']}={gate['status']}"]
    if gate["blocking_reasons"]:
        bits.append("blockers=" + "; ".join(str(reason) for reason in gate["blocking_reasons"]))
    affordability = (gate.get("details") or {}).get("affordability")
    if isinstance(affordability, dict) and affordability.get("status") is not None:
        bits.append(f"affordability={affordability['status']}")
    orders = (gate.get("details") or {}).get("orders")
    if isinstance(orders, list) and orders:
        order_summaries = [f"{order['side']} {order['executable_qty']} {order['symbol']}" for order in orders]
        bits.append("orders=" + "; ".join(order_summaries))
    blocking_artifacts = (gate.get("details") or {}).get("blocking_artifacts")
    if isinstance(blocking_artifacts, list) and blocking_artifacts:
        artifact_kinds = [str(artifact.get("artifact_kind")) for artifact in blocking_artifacts]
        bits.append("artifacts=" + ",".join(artifact_kinds))
    return "- " + " | ".join(bits)


def _render_readiness_action_line(action: dict[str, Any]) -> str:
    summary = str(action.get("summary") or action.get("action_id") or "next_action")
    command = action.get("command")
    if isinstance(command, str) and command.strip():
        return f"- {summary} | command={command}"
    return f"- {summary}"


def _render_readiness_text(payload: dict[str, Any]) -> str:
    scope = payload["scope"]
    lines = [
        f"Verdict {payload['verdict']}",
        (
            "Scope "
            f"account={scope.get('account_id') or '-'} "
            f"strategy={scope.get('strategy') or '-'} "
            f"signal_date={scope.get('signal_date') or '-'} "
            f"event_id={scope.get('event_id') or '-'}"
        ),
        (
            "Summary "
            f"blocking_reasons={payload['summary']['blocking_reason_count']} "
            f"warnings={payload['summary']['warning_count']}"
        ),
    ]

    blocking_reasons = payload.get("blocking_reasons", [])
    if blocking_reasons:
        lines.append("Blocking reasons:")
        lines.extend(f"- {reason}" for reason in blocking_reasons)
    else:
        lines.append("Blocking reasons: none")

    warnings = payload.get("warnings", [])
    if warnings:
        lines.append("Warnings:")
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("Warnings: none")

    next_actions = payload.get("next_actions", [])
    if next_actions:
        lines.append("Next actions:")
        lines.extend(_render_readiness_action_line(action) for action in next_actions)
    else:
        lines.append("Next actions: none")

    lines.append("Gates:")
    lines.extend(_render_readiness_gate_line(gate) for gate in payload.get("gates", []))
    return "\n".join(lines)


def _render_launch_text(payload: dict[str, Any]) -> str:
    context = payload["event_context"]
    artifact_paths = payload["artifact_paths"]
    lines = [
        f"Readiness {payload['readiness_verdict']}",
        f"Submit outcome {payload['submit_outcome']}",
        f"Live submit requested {str(payload['requested_live_submit']).lower()}",
        f"Submit path invoked {str(payload['submit_path_invoked']).lower()}",
        (
            "Scope "
            f"account={context.get('account_id') or '-'} "
            f"strategy={context.get('strategy') or '-'} "
            f"signal_date={context.get('signal_date') or '-'} "
            f"event_id={context.get('event_id') or '-'} "
            f"fingerprint={context.get('live_submission_fingerprint') or '-'}"
        ),
        f"Operator message {payload['operator_message']}",
        f"Result path {artifact_paths['result_path']}",
    ]

    blocking_reasons = payload.get("readiness", {}).get("blocking_reasons", [])
    if blocking_reasons:
        lines.append("Readiness blockers:")
        lines.extend(f"- {reason}" for reason in blocking_reasons)
    else:
        lines.append("Readiness blockers: none")

    submit_result = payload.get("submit_result")
    if isinstance(submit_result, dict):
        lines.append(
            "Submit result "
            f"decision={submit_result.get('decision') or '-'} "
            f"response={submit_result.get('response_text') or '-'}"
        )
    else:
        lines.append("Submit result: not invoked")
    return "\n".join(lines)


def _render_reconciliation_order_line(order: dict[str, Any]) -> str:
    bits = [
        f"{order['symbol']} {order['side']} {order['requested_quantity']}",
        f"order_id={order['broker_order_id']}",
        f"status={order.get('broker_status') or '-'}",
        f"fill_state={order.get('fill_state') or '-'}",
    ]
    if order.get("filled_quantity") is not None:
        bits.append(f"filled={order['filled_quantity']}")
    if order.get("actual_position_shares") is not None:
        bits.append(f"actual_shares={order['actual_position_shares']}")
    if order.get("implied_position_shares") is not None:
        bits.append(f"expected_shares={order['implied_position_shares']}")
    blockers = order.get("blocking_reasons") or []
    if blockers:
        bits.append("blockers=" + "; ".join(str(reason) for reason in blockers))
    return "- " + " | ".join(bits)


def _render_reconciliation_action_line(action: dict[str, Any]) -> str:
    return f"- {action.get('summary') or action.get('action_id') or 'next_action'}"


def _render_reconciliation_text(payload: dict[str, Any]) -> str:
    context = payload["context"]
    launch = payload["launch"]
    snapshot = payload["broker_truth"]["snapshot"]
    lines = [
        f"Verdict {payload['verdict']}",
        f"Launch {launch['path']}",
        (
            "Scope "
            f"account={context.get('account_id') or '-'} "
            f"strategy={context.get('strategy') or '-'} "
            f"signal_date={context.get('signal_date') or '-'} "
            f"event_id={context.get('event_id') or '-'} "
            f"fingerprint={context.get('live_submission_fingerprint') or '-'}"
        ),
        (
            "Broker "
            f"source={payload['broker_truth']['source']} "
            f"account={snapshot.get('account_id') or '-'} "
            f"as_of={snapshot.get('as_of') or '-'}"
        ),
    ]

    blocking_reasons = payload.get("blocking_reasons", [])
    if blocking_reasons:
        lines.append("Blocking reasons:")
        lines.extend(f"- {reason}" for reason in blocking_reasons)
    else:
        lines.append("Blocking reasons: none")

    warnings = payload.get("warnings", [])
    if warnings:
        lines.append("Warnings:")
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("Warnings: none")

    next_actions = payload.get("next_actions", [])
    if next_actions:
        lines.append("Next actions:")
        lines.extend(_render_reconciliation_action_line(action) for action in next_actions)
    else:
        lines.append("Next actions: none")

    orders = payload.get("broker_truth", {}).get("orders", [])
    if orders:
        lines.append("Orders:")
        lines.extend(_render_reconciliation_order_line(order) for order in orders)
    else:
        lines.append("Orders: none")

    lines.append(f"Result path {payload['artifact_paths']['result_path']}")
    return "\n".join(lines)


def _launch_guardrails_argv(args: argparse.Namespace, *, resolved_base_dir: Path, timestamp: Any) -> list[str]:
    argv = [
        "--signal-json-file",
        str(args.signal_json_file),
        "--broker",
        args.broker,
        "--live-canary-account",
        str(args.account_id),
        "--timestamp",
        timestamp.isoformat(),
        "--base-dir",
        str(resolved_base_dir),
    ]
    if args.positions_file is not None:
        argv.extend(["--positions-file", str(args.positions_file)])
    if args.live_submit:
        argv.append("--live-submit")
    if args.arm_live_canary is not None:
        argv.extend(["--arm-live-canary", str(args.arm_live_canary)])
    if args.ack_unmanaged_holdings:
        argv.append("--ack-unmanaged-holdings")
    if args.tastytrade_challenge_code is not None:
        argv.extend(["--tastytrade-challenge-code", str(args.tastytrade_challenge_code)])
    if args.tastytrade_challenge_token is not None:
        argv.extend(["--tastytrade-challenge-token", str(args.tastytrade_challenge_token)])
    if args.secrets_file is not None:
        argv.extend(["--secrets-file", str(args.secrets_file)])
    return argv


def _copy_named_paths(
    *,
    target: dict[str, Any],
    payload: dict[str, Any] | None,
    key_names: tuple[str, ...],
    prefix: str,
) -> None:
    if not isinstance(payload, dict):
        return
    for key_name in key_names:
        value = payload.get(key_name)
        if isinstance(value, str) and value.strip():
            target[f"{prefix}{key_name}"] = value


def _live_submission_fingerprint(submit_payload: dict[str, Any] | None) -> str | None:
    if not isinstance(submit_payload, dict):
        return None
    live_submission = submit_payload.get("live_submission")
    if not isinstance(live_submission, dict):
        return None
    fingerprint = live_submission.get("live_submission_fingerprint")
    if not isinstance(fingerprint, str):
        return None
    stripped = fingerprint.strip()
    return stripped or None


def _collect_launch_artifact_paths(
    *,
    result_path: Path,
    resolved_base_dir: Path,
    signal_json_file: Path,
    positions_file: Path | None,
    readiness: dict[str, Any],
    submit_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    artifact_paths: dict[str, Any] = {
        "result_path": str(result_path),
        "live_canary_base_dir": str(resolved_base_dir),
        "signal_json_file": str(signal_json_file),
    }
    if positions_file is not None:
        artifact_paths["positions_file"] = str(positions_file)

    blocking_artifact_paths = sorted(
        {
            str(artifact.get("path"))
            for artifact in readiness.get("state_status", {}).get("blocking_artifacts", [])
            if isinstance(artifact, dict) and isinstance(artifact.get("path"), str) and artifact.get("path")
        }
    )
    if blocking_artifact_paths:
        artifact_paths["readiness_blocking_artifact_paths"] = blocking_artifact_paths

    release_approval = readiness.get("state_status", {}).get("release_approval")
    if isinstance(release_approval, dict):
        approval_path = release_approval.get("approval_path")
        if isinstance(approval_path, str) and approval_path.strip():
            artifact_paths["release_approval_path"] = approval_path

    _copy_named_paths(
        target=artifact_paths,
        payload=submit_payload,
        key_names=("audit_path", "event_state_path"),
        prefix="submit_",
    )
    submit_session_guard = None if not isinstance(submit_payload, dict) else submit_payload.get("session_guard")
    if isinstance(submit_session_guard, dict):
        state_path = submit_session_guard.get("state_path")
        if isinstance(state_path, str) and state_path.strip():
            artifact_paths["submit_session_state_path"] = state_path

    live_submission = None if not isinstance(submit_payload, dict) else submit_payload.get("live_submission")
    if isinstance(live_submission, dict):
        durable_state = live_submission.get("durable_state")
        _copy_named_paths(
            target=artifact_paths,
            payload=durable_state if isinstance(durable_state, dict) else None,
            key_names=("claim_path", "ledger_path", "lock_path", "state_dir"),
            prefix="submit_",
        )
        duplicate_submit_refusal = live_submission.get("duplicate_submit_refusal")
        _copy_named_paths(
            target=artifact_paths,
            payload=duplicate_submit_refusal if isinstance(duplicate_submit_refusal, dict) else None,
            key_names=("claim_path", "ledger_path", "lock_path", "state_dir"),
            prefix="submit_duplicate_refusal_",
        )
    return artifact_paths


def _build_launch_result_path(
    *,
    resolved_base_dir: Path,
    timestamp: Any,
    event_context: dict[str, Any],
    requested_live_submit: bool,
    submit_outcome: str,
) -> Path:
    run_id = build_run_id(
        timestamp.isoformat(),
        run_kind="live_canary_launch",
        label=f"{event_context.get('strategy') or 'live_canary'}_{event_context.get('account_id') or 'unbound'}",
        identity_parts=[
            event_context.get("event_id"),
            requested_live_submit,
            submit_outcome,
            event_context.get("live_submission_fingerprint"),
        ],
    )
    return resolved_base_dir / "launches" / timestamp.date().isoformat() / f"{run_id}.json"


def _launch_should_invoke_guardrails(*, requested_live_submit: bool, readiness: dict[str, Any]) -> bool:
    if not requested_live_submit:
        return False
    if readiness.get("verdict") == "ready":
        return True

    evaluation = readiness.get("evaluation")
    if not isinstance(evaluation, dict) or evaluation.get("decision") != "ready_live_submit":
        return False

    gates_by_name = {
        str(gate.get("gate")): gate
        for gate in readiness.get("gates", [])
        if isinstance(gate, dict) and gate.get("gate") is not None
    }
    duplicate_gate = gates_by_name.get("duplicate_state") or {}
    duplicate_blockers = duplicate_gate.get("blocking_reasons") or []
    if not duplicate_blockers:
        return False

    for gate_name in LIVE_CANARY_LAUNCH_DUPLICATE_ONLY_GATES:
        gate_payload = gates_by_name.get(gate_name) or {}
        if gate_payload.get("blocking_reasons"):
            return False
    return True


def _run_launch(args: argparse.Namespace, *, timestamp: Any) -> tuple[int, dict[str, Any]]:
    resolved_base_dir = resolve_live_canary_state_base_dir(args.base_dir, create=True)
    readiness = build_live_canary_readiness(
        signal_json_file=args.signal_json_file,
        broker=args.broker,
        positions_file=args.positions_file,
        account_id=args.account_id,
        arm_live_canary=args.arm_live_canary,
        ack_unmanaged_holdings=bool(args.ack_unmanaged_holdings),
        require_release_approval=bool(args.live_submit),
        base_dir=resolved_base_dir,
        timestamp=timestamp,
        tastytrade_challenge_code=args.tastytrade_challenge_code,
        tastytrade_challenge_token=args.tastytrade_challenge_token,
        secrets_file=args.secrets_file,
    )

    submit_exit_code: int | None = None
    submit_payload: dict[str, Any] | None = None
    invoke_guardrails = _launch_should_invoke_guardrails(
        requested_live_submit=bool(args.live_submit),
        readiness=readiness,
    )
    if invoke_guardrails:
        guardrails_args = live_canary_guardrails.build_parser().parse_args(
            _launch_guardrails_argv(args, resolved_base_dir=resolved_base_dir, timestamp=timestamp)
        )
        submit_exit_code, submit_payload = live_canary_guardrails.run_guardrails(guardrails_args)

    if not args.live_submit:
        submit_outcome = "not_requested"
    elif submit_payload is None:
        submit_outcome = "not_attempted_readiness_blocked"
    else:
        submit_outcome = str((submit_payload or {}).get("decision") or "submit_path_invoked_without_result")

    readiness_scope = readiness.get("scope", {})
    readiness_signal = readiness.get("signal", {})
    readiness_evaluation = readiness.get("evaluation", {})
    event_context = {
        "account_id": readiness_scope.get("account_id"),
        "action": readiness_signal.get("action"),
        "broker_account_id": None if not isinstance(readiness_evaluation, dict) else readiness_evaluation.get("broker_account_id"),
        "event_id": readiness_scope.get("event_id"),
        "live_submission_fingerprint": _live_submission_fingerprint(submit_payload),
        "signal_date": readiness_scope.get("signal_date"),
        "strategy": readiness_scope.get("strategy"),
        "symbol": readiness_signal.get("symbol"),
    }
    if isinstance(submit_payload, dict) and submit_payload.get("broker_account_id") is not None:
        event_context["broker_account_id"] = submit_payload.get("broker_account_id")

    result_path = _build_launch_result_path(
        resolved_base_dir=resolved_base_dir,
        timestamp=timestamp,
        event_context=event_context,
        requested_live_submit=bool(args.live_submit),
        submit_outcome=submit_outcome,
    )
    artifact_paths = _collect_launch_artifact_paths(
        result_path=result_path,
        resolved_base_dir=resolved_base_dir,
        signal_json_file=Path(args.signal_json_file),
        positions_file=None if args.positions_file is None else Path(args.positions_file),
        readiness=readiness,
        submit_payload=submit_payload,
    )

    operator_message = (
        str(submit_payload.get("response_text"))
        if isinstance(submit_payload, dict) and submit_payload.get("response_text") is not None
        else "; ".join(str(reason) for reason in readiness.get("blocking_reasons", []))
        if readiness.get("blocking_reasons")
        else "launch preview only"
    )
    payload = {
        "schema_name": LIVE_CANARY_LAUNCH_SCHEMA_NAME,
        "schema_version": LIVE_CANARY_LAUNCH_SCHEMA_VERSION,
        "timestamp_chicago": timestamp.isoformat(),
        "requested_live_submit": bool(args.live_submit),
        "submit_exit_code": submit_exit_code,
        "submit_outcome": submit_outcome,
        "submit_path_invoked": submit_payload is not None,
        "readiness_verdict": readiness["verdict"],
        "operator_message": operator_message,
        "event_context": event_context,
        "artifact_paths": artifact_paths,
        "readiness": readiness,
        "submit_result": submit_payload,
    }
    _atomic_write_json(result_path, payload)

    if submit_payload is not None:
        return int(submit_exit_code or 0), payload
    if readiness["verdict"] != "ready":
        return 2, payload
    return 0, payload


def _run_reconcile(args: argparse.Namespace, *, timestamp: Any) -> tuple[int, dict[str, Any]]:
    payload = build_live_canary_reconciliation(
        launch_result_file=args.launch_result_file,
        broker=args.broker,
        positions_file=args.positions_file,
        orders_file=args.orders_file,
        account_id=args.account_id,
        base_dir=args.base_dir,
        timestamp=timestamp,
        tastytrade_challenge_code=args.tastytrade_challenge_code,
        tastytrade_challenge_token=args.tastytrade_challenge_token,
        secrets_file=args.secrets_file,
    )
    result_path = Path(payload["artifact_paths"]["result_path"])
    _atomic_write_json(result_path, payload)
    if payload["verdict"] == LIVE_CANARY_RECONCILIATION_VERDICT_BLOCKED:
        return 2, payload
    return 0, payload


def _emit(payload: dict[str, Any], *, emit: str) -> None:
    if emit == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
        return
    if payload["schema_name"] == "live_canary_state_status":
        print(_render_status_text(payload))
        return
    if payload["schema_name"] == LIVE_CANARY_RELEASE_APPROVAL_OPERATION_SCHEMA_NAME:
        print(_render_release_approval_text(payload))
        return
    if payload["schema_name"] == "live_canary_readiness":
        print(_render_readiness_text(payload))
        return
    if payload["schema_name"] == LIVE_CANARY_LAUNCH_SCHEMA_NAME:
        print(_render_launch_text(payload))
        return
    if payload["schema_name"] == LIVE_CANARY_RECONCILIATION_SCHEMA_NAME:
        print(_render_reconciliation_text(payload))
        return
    print(_render_clear_text(payload))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timestamp = resolve_timestamp(args.timestamp)
    try:
        if args.command in {"status", "inspect"}:
            payload = build_live_canary_state_status(
                base_dir=args.base_dir,
                account_id=args.account_id,
                strategy=args.strategy,
                signal_date=args.signal_date,
                event_id=args.event_id,
                live_submission_fingerprint=args.live_submission_fingerprint,
            )
        elif args.command in {"readiness", "preflight"}:
            payload = build_live_canary_readiness(
                signal_json_file=args.signal_json_file,
                broker=args.broker,
                positions_file=args.positions_file,
                account_id=args.account_id,
                arm_live_canary=args.arm_live_canary,
                ack_unmanaged_holdings=bool(args.ack_unmanaged_holdings),
                require_release_approval=not bool(args.preview_only),
                base_dir=args.base_dir,
                timestamp=timestamp,
                tastytrade_challenge_code=args.tastytrade_challenge_code,
                tastytrade_challenge_token=args.tastytrade_challenge_token,
                secrets_file=args.secrets_file,
            )
        elif args.command == "launch":
            result_code, payload = _run_launch(args, timestamp=timestamp)
            _emit(payload, emit=args.emit)
            return result_code
        elif args.command in {"approve", "release"}:
            if args.apply:
                payload = apply_live_canary_release_approval(
                    base_dir=args.base_dir,
                    account_id=args.account_id,
                    bundle_dir=args.bundle_dir,
                    reason=args.reason,
                    operator=args.operator,
                    timestamp=timestamp,
                )
            else:
                payload = preview_live_canary_release_approval(
                    base_dir=args.base_dir,
                    account_id=args.account_id,
                    bundle_dir=args.bundle_dir,
                    reason=args.reason,
                    operator=args.operator,
                    timestamp=timestamp,
                )
        elif args.command in {"reconcile", "closeout"}:
            result_code, payload = _run_reconcile(args, timestamp=timestamp)
            _emit(payload, emit=args.emit)
            return result_code
        else:
            clear_scopes = set(args.clear_scopes or [])
            if args.confirm is not None and not args.apply:
                raise ValueError("--confirm can only be used together with --apply.")
            if args.apply and args.confirm is None:
                raise ValueError("--apply requires --confirm with the preview confirmation token.")
            if args.apply:
                payload = apply_live_canary_state_clear(
                    base_dir=args.base_dir,
                    account_id=args.account_id,
                    clear_scopes=clear_scopes,
                    confirm=args.confirm,
                    strategy=args.strategy,
                    signal_date=args.signal_date,
                    event_id=args.event_id,
                    live_submission_fingerprint=args.live_submission_fingerprint,
                    reason=args.reason,
                    timestamp=timestamp,
                )
            else:
                payload = preview_live_canary_state_clear(
                    base_dir=args.base_dir,
                    account_id=args.account_id,
                    clear_scopes=clear_scopes,
                    strategy=args.strategy,
                    signal_date=args.signal_date,
                    event_id=args.event_id,
                    live_submission_fingerprint=args.live_submission_fingerprint,
                    reason=args.reason,
                    timestamp=timestamp,
                )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2

    _emit(payload, emit=args.emit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
