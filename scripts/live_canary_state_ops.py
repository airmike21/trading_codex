#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from trading_codex.execution import resolve_timestamp
from trading_codex.execution.live_canary_readiness import build_live_canary_readiness
from trading_codex.execution.live_canary_state_ops import (
    apply_live_canary_state_clear,
    build_live_canary_state_status,
    preview_live_canary_state_clear,
)
from trading_codex.execution.secrets import DEFAULT_TASTYTRADE_SECRETS_PATH


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


def _emit(payload: dict[str, Any], *, emit: str) -> None:
    if emit == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
        return
    if payload["schema_name"] == "live_canary_state_status":
        print(_render_status_text(payload))
        return
    if payload["schema_name"] == "live_canary_readiness":
        print(_render_readiness_text(payload))
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
                base_dir=args.base_dir,
                timestamp=timestamp,
                tastytrade_challenge_code=args.tastytrade_challenge_code,
                tastytrade_challenge_token=args.tastytrade_challenge_token,
                secrets_file=args.secrets_file,
            )
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
