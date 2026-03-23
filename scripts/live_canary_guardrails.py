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

from trading_codex.execution import (
    FileBrokerPositionAdapter,
    RequestsTastytradeHttpClient,
    TastytradeBrokerExecutionAdapter,
    build_execution_plan,
    parse_signal_payload,
    resolve_timestamp,
)
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    LIVE_CANARY_STATE_PENDING,
    append_live_canary_audit,
    audit_rows_for_result,
    build_live_canary_submission_export,
    claim_live_canary_event,
    evaluate_live_canary,
    finalize_live_canary_event,
    live_canary_audit_path,
    live_canary_event_state_path,
    live_canary_live_submit_limits,
    normalize_live_canary_account,
    response_text_from_live_submission,
)
from trading_codex.execution.secrets import DEFAULT_TASTYTRADE_SECRETS_PATH, load_tastytrade_secrets


def _load_signal_from_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Signal JSON file must contain a JSON object.")
    return payload


def _signal_result(
    *,
    signal: Any,
    timestamp_chicago: str,
    account_id: str | None,
    decision: str,
    blockers: list[str],
    warnings: list[str],
    live_submit_requested: bool,
    armed: bool,
    duplicate: bool,
    response_text: str,
    audit_path: Path,
    event_state_path: Path | None,
    orders: list[dict[str, Any]] | None = None,
    live_submission: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rendered_orders = list(orders or [])
    return {
        "schema_name": "live_canary_guardrail_result",
        "schema_version": 1,
        "timestamp_chicago": timestamp_chicago,
        "account_id": account_id,
        "action": signal.action,
        "armed": armed,
        "audit_path": str(audit_path),
        "allowed_symbols": list(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        "blockers": list(blockers),
        "broker_account_id": None,
        "decision": decision,
        "duplicate": duplicate,
        "event_id": signal.event_id,
        "event_state_path": None if event_state_path is None else str(event_state_path),
        "executable_qty_total": sum(int(order["executable_qty"]) for order in rendered_orders),
        "live_submission": live_submission,
        "live_submit_requested": live_submit_requested,
        "orders": rendered_orders,
        "requested_qty_total": sum(int(order["requested_qty"]) for order in rendered_orders),
        "response_text": response_text,
        "symbol": signal.symbol,
        "warnings": list(warnings),
    }


def _blocked_rows_from_signal(
    *,
    timestamp_chicago: str,
    signal: Any,
    account_id: str | None,
    decision: str,
    response_text: str,
    live_submit_requested: bool,
) -> list[dict[str, Any]]:
    del live_submit_requested
    return [
        {
            "account": account_id,
            "action": signal.action,
            "armed": False,
            "classification": None,
            "current_broker_shares": None,
            "decision": decision,
            "desired_canary_shares": None,
            "desired_signal_shares": signal.desired_target_shares,
            "duplicate": False,
            "event_id": signal.event_id,
            "executable_qty": 0,
            "reference_price": signal.price,
            "requested_qty": 0,
            "response_text": response_text,
            "side": None,
            "symbol": signal.symbol,
            "ts_chicago": timestamp_chicago,
        }
    ]


def _render_text_result(payload: dict[str, Any]) -> str:
    parts = [
        payload["decision"],
        f"event_id={payload['event_id']}",
        f"account={payload['account_id'] or '-'}",
        f"duplicate={str(payload['duplicate']).lower()}",
    ]
    if payload["orders"]:
        order_bits = [
            f"{order['side']} {order['executable_qty']} {order['symbol']} (requested={order['requested_qty']})"
            for order in payload["orders"]
        ]
        parts.append("orders=" + "; ".join(order_bits))
    parts.append(f"response={payload['response_text']}")
    return " | ".join(parts)


def _emit_result(*, payload: dict[str, Any], emit: str) -> None:
    if emit == "json":
        print(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return
    print(_render_text_result(payload))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed live-canary guardrails around next_action JSON. Dry-run is the default."
    )
    parser.add_argument("--signal-json-file", type=Path, required=True, help="Existing next_action JSON payload.")
    parser.add_argument(
        "--broker",
        choices=["file", "tastytrade"],
        default="file",
        help="Broker snapshot source. Use 'file' for tests/reviews; 'tastytrade' for live-capable dry-run review.",
    )
    parser.add_argument("--positions-file", type=Path, default=None, help="Required with --broker file.")
    parser.add_argument(
        "--live-canary-account",
        type=str,
        required=True,
        help="Required explicit account binding for this canary path. No env fallback.",
    )
    parser.add_argument(
        "--live-submit",
        action="store_true",
        help="Attempt real tastytrade submission only when also armed with --arm-live-canary.",
    )
    parser.add_argument(
        "--arm-live-canary",
        type=str,
        default=None,
        help="Manual arming token. Must exactly match --live-canary-account.",
    )
    parser.add_argument(
        "--ack-unmanaged-holdings",
        action="store_true",
        help="Allow managed-sleeve planning when unmanaged holdings exist. Still fail-closed on canary blockers.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Optional local audit/state directory. Default uses the archive root fallback chain plus /live_canary.",
    )
    parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional device-challenge code for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_CODE.",
    )
    parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional device-challenge token override for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_TOKEN.",
    )
    parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help=f"Optional tastytrade secrets env file. If omitted, auto-loads {DEFAULT_TASTYTRADE_SECRETS_PATH} when present.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["json", "text"], default="json", help="Stdout format.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timestamp = resolve_timestamp(args.timestamp)
    audit_path = live_canary_audit_path(args.base_dir)

    raw_signal = _load_signal_from_file(args.signal_json_file)
    signal = parse_signal_payload(raw_signal)
    source_label = args.signal_json_file.stem
    source_ref = str(args.signal_json_file)
    account_id = normalize_live_canary_account(args.live_canary_account)

    if account_id is None:
        timestamp_chicago = timestamp.isoformat()
        response_text = "live_canary_requires_account_binding"
        rows = _blocked_rows_from_signal(
            timestamp_chicago=timestamp_chicago,
            signal=signal,
            account_id=None,
            decision="blocked",
            response_text=response_text,
            live_submit_requested=bool(args.live_submit),
        )
        append_live_canary_audit(audit_path=audit_path, rows=rows)
        payload = _signal_result(
            signal=signal,
            timestamp_chicago=timestamp_chicago,
            account_id=None,
            decision="blocked",
            blockers=["live_canary_requires_account_binding"],
            warnings=[],
            live_submit_requested=bool(args.live_submit),
            armed=False,
            duplicate=False,
            response_text=response_text,
            audit_path=audit_path,
            event_state_path=None,
        )
        _emit_result(payload=payload, emit=args.emit)
        return 2

    try:
        if args.broker == "file":
            if args.positions_file is None:
                raise ValueError("--positions-file is required when --broker file.")
            broker_adapter = FileBrokerPositionAdapter(args.positions_file)
            broker_source_ref = str(args.positions_file)
        else:
            load_tastytrade_secrets(secrets_file=args.secrets_file)
            if args.positions_file is not None:
                raise ValueError("--positions-file cannot be used with --broker tastytrade.")
            broker_adapter = TastytradeBrokerExecutionAdapter(
                account_id=account_id,
                client=RequestsTastytradeHttpClient(
                    challenge_code=args.tastytrade_challenge_code,
                    challenge_token=args.tastytrade_challenge_token,
                ),
            )
            broker_source_ref = f"tastytrade:{account_id}"

        broker_snapshot = broker_adapter.load_snapshot()
        plan = build_execution_plan(
            signal=signal,
            broker_snapshot=broker_snapshot,
            account_scope="managed_sleeve",
            managed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
            ack_unmanaged_holdings=args.ack_unmanaged_holdings,
            source_kind="signal_json_file",
            source_label=source_label,
            source_ref=source_ref,
            broker_source_ref=broker_source_ref,
            data_dir=None,
        )
        evaluation = evaluate_live_canary(
            plan=plan,
            live_canary_account=account_id,
            live_submit_requested=bool(args.live_submit),
            arm_live_canary=args.arm_live_canary,
            allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
            timestamp=timestamp,
        )
    except Exception as exc:
        timestamp_chicago = timestamp.isoformat()
        response_text = str(exc)
        rows = _blocked_rows_from_signal(
            timestamp_chicago=timestamp_chicago,
            signal=signal,
            account_id=account_id,
            decision="blocked",
            response_text=response_text,
            live_submit_requested=bool(args.live_submit),
        )
        append_live_canary_audit(audit_path=audit_path, rows=rows)
        payload = _signal_result(
            signal=signal,
            timestamp_chicago=timestamp_chicago,
            account_id=account_id,
            decision="blocked",
            blockers=[response_text],
            warnings=[],
            live_submit_requested=bool(args.live_submit),
            armed=False,
            duplicate=False,
            response_text=response_text,
            audit_path=audit_path,
            event_state_path=None,
        )
        _emit_result(payload=payload, emit=args.emit)
        return 2

    final_decision = evaluation.decision
    final_blockers = list(evaluation.blockers)
    final_warnings = list(evaluation.warnings)
    duplicate = False
    event_state_path: Path | None = None
    live_submission_payload: dict[str, Any] | None = None
    response_text = "dry-run only"

    if final_blockers:
        response_text = "; ".join(final_blockers)
    elif final_decision in {"noop_hold", "noop_cash", "noop"}:
        response_text = "no executable live-canary order"
    elif not args.live_submit:
        response_text = "dry-run only"

    if (
        args.live_submit
        and not final_blockers
        and evaluation.orders
        and final_decision == "ready_live_submit"
    ):
        if args.broker != "tastytrade":
            final_decision = "blocked"
            final_blockers.append("live_canary_live_submit_requires_tastytrade_broker")
            response_text = "live_canary_live_submit_requires_tastytrade_broker"
        else:
            claim_record = {
                "account_id": account_id,
                "decision": "pending_live_submit",
                "event_id": signal.event_id,
                "generated_at_chicago": evaluation.timestamp_chicago,
                "manual_clearance_required": True,
                "response_text": LIVE_CANARY_STATE_PENDING,
                "result": LIVE_CANARY_STATE_PENDING,
            }
            claimed, prior_record, event_state_path = claim_live_canary_event(
                base_dir=args.base_dir,
                account_id=account_id,
                event_id=signal.event_id,
                record=claim_record,
            )
            if not claimed:
                duplicate = True
                final_decision = "blocked_duplicate"
                final_blockers.append("live_canary_duplicate_event")
                response_text = (
                    str(prior_record.get("response_text"))
                    if isinstance(prior_record, dict) and prior_record.get("response_text") is not None
                    else "live_canary_duplicate_event"
                )
            else:
                simulated_export = build_live_canary_submission_export(plan=plan, evaluation=evaluation)
                live_max_order_notional, live_max_order_qty = live_canary_live_submit_limits(simulated_export)
                try:
                    live_submission = broker_adapter.submit_live_orders(
                        export=simulated_export,
                        confirm_account_id=account_id,
                        live_allowed_account=account_id,
                        confirm_plan_sha256=simulated_export.plan_sha256,
                        allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
                        live_max_order_notional=live_max_order_notional,
                        live_max_order_qty=live_max_order_qty,
                        ledger_path=Path(audit_path.parent) / "broker_live_submission_fingerprints.jsonl",
                        live_submission_artifact_path=None,
                    )
                    response_text = response_text_from_live_submission(live_submission)
                    final_decision = (
                        "live_submitted"
                        if live_submission.live_submit_attempted and live_submission.submission_succeeded
                        else "live_submit_refused"
                    )
                    live_submission_payload = {
                        "live_submit_attempted": live_submission.live_submit_attempted,
                        "manual_clearance_required": live_submission.manual_clearance_required,
                        "orders": [
                            {
                                "attempted": order.attempted,
                                "broker_order_id": order.broker_order_id,
                                "broker_status": order.broker_status,
                                "error": order.error,
                                "quantity": order.quantity,
                                "side": order.side,
                                "succeeded": order.succeeded,
                                "symbol": order.symbol,
                            }
                            for order in live_submission.orders
                        ],
                        "refusal_reasons": list(live_submission.refusal_reasons),
                        "submission_result": live_submission.submission_result,
                        "submission_succeeded": live_submission.submission_succeeded,
                    }
                    finalize_live_canary_event(
                        state_path=event_state_path,
                        record={
                            "account_id": account_id,
                            "decision": final_decision,
                            "event_id": signal.event_id,
                            "generated_at_chicago": evaluation.timestamp_chicago,
                            "manual_clearance_required": bool(
                                live_submission.manual_clearance_required or not live_submission.submission_succeeded
                            ),
                            "response_text": response_text,
                            "result": live_submission.submission_result,
                        },
                    )
                except Exception as exc:
                    final_decision = "live_submit_error"
                    response_text = str(exc)
                    finalize_live_canary_event(
                        state_path=event_state_path,
                        record={
                            "account_id": account_id,
                            "decision": final_decision,
                            "event_id": signal.event_id,
                            "generated_at_chicago": evaluation.timestamp_chicago,
                            "manual_clearance_required": True,
                            "response_text": response_text,
                            "result": LIVE_CANARY_STATE_PENDING,
                        },
                    )

    final_blockers = sorted(set(final_blockers))
    final_warnings = sorted(set(final_warnings))
    rows = audit_rows_for_result(
        evaluation=evaluation,
        decision=final_decision,
        duplicate=duplicate,
        response_text=response_text,
    )
    append_live_canary_audit(audit_path=audit_path, rows=rows)
    if event_state_path is None and account_id:
        event_state_path = live_canary_event_state_path(
            base_dir=args.base_dir,
            account_id=account_id,
            event_id=signal.event_id,
        )
        if not event_state_path.exists():
            event_state_path = None

    payload = _signal_result(
        signal=signal,
        timestamp_chicago=evaluation.timestamp_chicago,
        account_id=account_id,
        decision=final_decision,
        blockers=final_blockers,
        warnings=final_warnings,
        live_submit_requested=bool(args.live_submit),
        armed=evaluation.armed,
        duplicate=duplicate,
        response_text=response_text,
        audit_path=audit_path,
        event_state_path=event_state_path,
        orders=[
            {
                "cap_applied": order.cap_applied,
                "classification": order.classification,
                "current_broker_shares": order.current_broker_shares,
                "desired_canary_shares": order.desired_canary_shares,
                "desired_signal_shares": order.desired_signal_shares,
                "estimated_notional": order.estimated_notional,
                "executable_qty": order.executable_qty,
                "reference_price": order.reference_price,
                "requested_qty": order.requested_qty,
                "side": order.side,
                "symbol": order.symbol,
            }
            for order in evaluation.orders
        ],
        live_submission=live_submission_payload,
    )
    payload["broker_account_id"] = evaluation.broker_account_id
    _emit_result(payload=payload, emit=args.emit)

    if final_decision in {"blocked", "blocked_duplicate", "live_submit_error", "live_submit_refused"}:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
