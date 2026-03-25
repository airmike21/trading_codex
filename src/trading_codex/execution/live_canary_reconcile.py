from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.execution import (
    FileBrokerOrderStatusAdapter,
    FileBrokerPositionAdapter,
    RequestsTastytradeHttpClient,
    TastytradeBrokerPositionAdapter,
)
from trading_codex.execution.broker import BrokerOrderStatus
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_BROKER_SNAPSHOT_MAX_AGE,
    normalize_live_canary_account,
)
from trading_codex.execution.live_canary_state_ops import (
    build_live_canary_state_status,
    parse_live_canary_event_scope,
    resolve_live_canary_state_base_dir,
)
from trading_codex.execution.secrets import load_tastytrade_secrets
from trading_codex.run_archive import build_run_id


LIVE_CANARY_RECONCILIATION_SCHEMA_NAME = "live_canary_reconciliation_result"
LIVE_CANARY_RECONCILIATION_SCHEMA_VERSION = 1
LIVE_CANARY_LAUNCH_SCHEMA_NAME = "live_canary_launch_result"
LIVE_CANARY_LAUNCH_SCHEMA_VERSION = 1
LIVE_CANARY_RECONCILIATION_VERDICT_RECONCILED = "reconciled"
LIVE_CANARY_RECONCILIATION_VERDICT_NOT_APPLICABLE = "not_applicable"
LIVE_CANARY_RECONCILIATION_VERDICT_BLOCKED = "blocked"

_FINAL_FILLED_STATUSES = frozenset({"filled", "executed"})
_FINAL_UNFILLED_STATUSES = frozenset({"cancelled", "canceled", "rejected", "expired"})
_OPEN_STATUSES = frozenset(
    {
        "accepted",
        "in flight",
        "live",
        "open",
        "pending",
        "received",
        "routing",
        "working",
    }
)


@dataclass(frozen=True)
class LaunchOrderIntent:
    symbol: str
    side: str
    requested_quantity: int
    current_broker_shares: int
    desired_canary_shares: int


def _normalize_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _coerce_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean.")
    return value


def _required_bool_field(payload: dict[str, Any], *, field_name: str, prefix: str) -> bool:
    if field_name not in payload:
        raise ValueError(f"{prefix}.{field_name} must be a boolean.")
    return _coerce_bool(payload.get(field_name), field_name=f"{prefix}.{field_name}")


def _coerce_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer.")
    return value


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8").strip()
    if raw == "":
        raise ValueError(f"{label} at {path} is empty.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} at {path} is malformed: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} at {path} must be a JSON object.")
    return payload


def _parse_iso_timestamp(value: str, *, field_name: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO timestamp.") from exc


def _dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _launch_orders_from_payload(payload: object, *, field_name: str) -> list[LaunchOrderIntent]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise ValueError(f"{field_name} must be a list.")
    orders: list[LaunchOrderIntent] = []
    seen_symbols: set[str] = set()
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{field_name}[{index}] must be an object.")
        symbol = _normalize_text(item.get("symbol"), field_name=f"{field_name}[{index}].symbol").upper()
        if symbol in seen_symbols:
            raise ValueError(f"{field_name} contains duplicate symbol {symbol!r}.")
        seen_symbols.add(symbol)
        side = _normalize_text(item.get("side"), field_name=f"{field_name}[{index}].side").upper()
        requested_quantity = _coerce_int(item.get("requested_qty"), field_name=f"{field_name}[{index}].requested_qty")
        current_broker_shares = _coerce_int(
            item.get("current_broker_shares"),
            field_name=f"{field_name}[{index}].current_broker_shares",
        )
        desired_canary_shares = _coerce_int(
            item.get("desired_canary_shares"),
            field_name=f"{field_name}[{index}].desired_canary_shares",
        )
        orders.append(
            LaunchOrderIntent(
                symbol=symbol,
                side=side,
                requested_quantity=requested_quantity,
                current_broker_shares=current_broker_shares,
                desired_canary_shares=desired_canary_shares,
            )
        )
    return orders


def _receipt_orders_from_payload(payload: object) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise ValueError("launch.submit_result.live_submission.orders must be a list.")
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"launch.submit_result.live_submission.orders[{index}] must be an object.")
        symbol = _normalize_text(
            item.get("symbol"),
            field_name=f"launch.submit_result.live_submission.orders[{index}].symbol",
        ).upper()
        side = _normalize_text(
            item.get("side"),
            field_name=f"launch.submit_result.live_submission.orders[{index}].side",
        ).upper()
        quantity = _coerce_int(
            item.get("quantity"),
            field_name=f"launch.submit_result.live_submission.orders[{index}].quantity",
        )
        normalized.append(
            {
                "attempted": _required_bool_field(
                    item,
                    field_name="attempted",
                    prefix=f"launch.submit_result.live_submission.orders[{index}]",
                ),
                "broker_order_id": _optional_text(item.get("broker_order_id")),
                "broker_status": _optional_text(item.get("broker_status")),
                "error": _optional_text(item.get("error")),
                "quantity": quantity,
                "side": side,
                "symbol": symbol,
                "succeeded": _required_bool_field(
                    item,
                    field_name="succeeded",
                    prefix=f"launch.submit_result.live_submission.orders[{index}]",
                ),
            }
        )
    return normalized


def _build_reconciliation_result_path(
    *,
    resolved_base_dir: Path,
    launch_result_file: Path,
    timestamp: datetime,
    account_id: str | None,
    strategy: str | None,
    event_id: str | None,
    live_submission_fingerprint: str | None,
    verdict: str,
) -> Path:
    run_id = build_run_id(
        timestamp.isoformat(),
        run_kind="live_canary_reconcile",
        label=f"{strategy or 'live_canary'}_{account_id or 'unbound'}",
        identity_parts=[
            str(launch_result_file),
            event_id,
            live_submission_fingerprint,
            verdict,
        ],
    )
    return resolved_base_dir / "reconciliations" / timestamp.date().isoformat() / f"{run_id}.json"


def _state_artifacts_by_kind(state_status: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    artifacts_by_kind: dict[str, list[dict[str, Any]]] = {}
    for artifact in state_status.get("artifacts", []):
        if not isinstance(artifact, dict):
            continue
        artifact_kind = str(artifact.get("artifact_kind") or "")
        artifacts_by_kind.setdefault(artifact_kind, []).append(artifact)
    return artifacts_by_kind


def _assert_single_artifact(
    *,
    artifacts_by_kind: dict[str, list[dict[str, Any]]],
    artifact_kind: str,
    blocking_reasons: list[str],
) -> dict[str, Any] | None:
    matches = artifacts_by_kind.get(artifact_kind, [])
    if len(matches) > 1:
        blocking_reasons.append(f"conflicting_durable_state:{artifact_kind}:{len(matches)}")
        return None
    return matches[0] if matches else None


def _normalize_order_status(status: str | None) -> str | None:
    if status is None:
        return None
    return " ".join(status.strip().lower().split())


def _classify_order_truth(
    *,
    launch_order: LaunchOrderIntent,
    receipt_order: dict[str, Any],
    broker_order_status: BrokerOrderStatus,
    actual_position_shares: int,
    expected_account_id: str | None,
) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    normalized_status = _normalize_order_status(broker_order_status.status)
    filled_quantity = broker_order_status.filled_quantity
    remaining_quantity = broker_order_status.remaining_quantity
    requested_quantity = launch_order.requested_quantity

    if filled_quantity is not None and (filled_quantity < 0 or filled_quantity > requested_quantity):
        blocking_reasons.append(
            "broker_order_filled_quantity_out_of_range:"
            f"{broker_order_status.order_id}:{filled_quantity}:{requested_quantity}"
        )
    if remaining_quantity is not None and (remaining_quantity < 0 or remaining_quantity > requested_quantity):
        blocking_reasons.append(
            "broker_order_remaining_quantity_out_of_range:"
            f"{broker_order_status.order_id}:{remaining_quantity}:{requested_quantity}"
        )
    if (
        filled_quantity is not None
        and remaining_quantity is not None
        and filled_quantity + remaining_quantity != requested_quantity
    ):
        blocking_reasons.append(
            "broker_order_fill_quantity_mismatch:"
            f"{broker_order_status.order_id}:{filled_quantity}:{remaining_quantity}:{requested_quantity}"
        )

    if (
        expected_account_id is not None
        and broker_order_status.account_id is not None
        and broker_order_status.account_id != expected_account_id
    ):
        blocking_reasons.append(
            "broker_order_account_mismatch:"
            f"{broker_order_status.order_id}:{broker_order_status.account_id}:{expected_account_id}"
        )

    fill_state = "unknown"
    finality = "ambiguous"
    implied_position_shares = None

    if normalized_status in _FINAL_FILLED_STATUSES:
        fill_state = "filled"
        finality = "final"
        implied_position_shares = launch_order.desired_canary_shares
    elif normalized_status in _FINAL_UNFILLED_STATUSES:
        fill_state = "unfilled"
        finality = "final"
        implied_position_shares = launch_order.current_broker_shares
    elif normalized_status in _OPEN_STATUSES:
        finality = "open"
        if filled_quantity is None:
            fill_state = "ambiguous_open"
            blocking_reasons.append(
                f"broker_order_open_without_fill_quantity:{broker_order_status.order_id}:{normalized_status}"
            )
        elif filled_quantity == 0:
            fill_state = "open_unfilled"
            implied_position_shares = launch_order.current_broker_shares
        else:
            fill_state = "partially_filled"
    elif normalized_status is None and filled_quantity is not None:
        if filled_quantity == requested_quantity:
            fill_state = "filled"
            finality = "final"
            implied_position_shares = launch_order.desired_canary_shares
        elif filled_quantity == 0:
            fill_state = "unknown_zero_fill"
        else:
            fill_state = "partially_filled"
    else:
        blocking_reasons.append(
            f"broker_order_status_ambiguous:{broker_order_status.order_id}:{broker_order_status.status or 'missing'}"
        )

    if filled_quantity is not None and 0 < filled_quantity < requested_quantity:
        fill_state = "partially_filled"
        if normalized_status in _FINAL_FILLED_STATUSES:
            blocking_reasons.append(
                f"broker_order_status_fill_conflict:{broker_order_status.order_id}:{broker_order_status.status}"
            )
        finality = "open" if normalized_status in _OPEN_STATUSES else finality
        delta = filled_quantity if launch_order.side == "BUY" else -filled_quantity
        implied_position_shares = launch_order.current_broker_shares + delta

    if fill_state == "unfilled":
        blocking_reasons.append(
            f"broker_order_not_filled:{broker_order_status.order_id}:{broker_order_status.status or 'unknown'}"
        )
    elif fill_state == "partially_filled":
        blocking_reasons.append(
            f"broker_order_partially_filled:{broker_order_status.order_id}:{filled_quantity}:{requested_quantity}"
        )
    elif finality == "open":
        blocking_reasons.append(
            f"broker_order_still_open:{broker_order_status.order_id}:{broker_order_status.status or 'unknown'}"
        )

    position_truth_matched = implied_position_shares is not None and actual_position_shares == implied_position_shares
    if implied_position_shares is not None and not position_truth_matched:
        blocking_reasons.append(
            "broker_position_truth_mismatch:"
            f"{launch_order.symbol}:{actual_position_shares}:{implied_position_shares}:{broker_order_status.order_id}"
        )

    return {
        "account_id": broker_order_status.account_id,
        "actual_position_shares": actual_position_shares,
        "blocking_reasons": _dedupe_preserve(blocking_reasons),
        "broker_order_id": broker_order_status.order_id,
        "broker_status": broker_order_status.status,
        "desired_canary_shares": launch_order.desired_canary_shares,
        "fill_state": fill_state,
        "filled_quantity": filled_quantity,
        "finality": finality,
        "implied_position_shares": implied_position_shares,
        "position_truth_matched": position_truth_matched,
        "receipt": dict(receipt_order),
        "remaining_quantity": remaining_quantity,
        "requested_quantity": requested_quantity,
        "side": launch_order.side,
        "symbol": launch_order.symbol,
        "updated_at": broker_order_status.updated_at,
    }


def _reconcile_next_actions(
    *,
    mode: str,
    verdict: str,
    blocking_reasons: list[str],
) -> list[dict[str, str]]:
    if verdict == LIVE_CANARY_RECONCILIATION_VERDICT_RECONCILED:
        return [
            {
                "action_id": "no_action_required",
                "summary": "Launch artifact, broker order truth, durable local state, and resulting position truth agree.",
            }
        ]

    if verdict == LIVE_CANARY_RECONCILIATION_VERDICT_NOT_APPLICABLE:
        if mode == "preview_only":
            return [
                {
                    "action_id": "no_closeout_required",
                    "summary": "No live submit was requested for this launch artifact. Use it as preview evidence only.",
                }
            ]
        return [
            {
                "action_id": "review_launch_blockers",
                "summary": "No broker order was submitted for this launch artifact. Review the recorded launch blockers or refusal reasons before any retry.",
            }
        ]

    actions: list[dict[str, str]] = []
    for reason in blocking_reasons:
        if reason.startswith("launch_artifact_missing_broker_order_id:"):
            actions.append(
                {
                    "action_id": "manual_broker_lookup_required",
                    "summary": "The launch receipt does not contain a durable broker order id. Verify the broker account manually before clearing any local state.",
                }
            )
            continue
        if reason.startswith("broker_order_still_open:"):
            actions.append(
                {
                    "action_id": "wait_for_final_order_status",
                    "summary": "The broker order is still open. Wait for a final broker status, then rerun reconciliation.",
                }
            )
            continue
        if reason.startswith("broker_order_partially_filled:"):
            actions.append(
                {
                    "action_id": "manual_partial_fill_review",
                    "summary": "The broker order is partially filled. Review the broker order manually and do not clear submit-tracking state until the final position is understood.",
                }
            )
            continue
        if reason.startswith("broker_position_truth_mismatch:"):
            actions.append(
                {
                    "action_id": "investigate_position_mismatch",
                    "summary": "Broker position truth does not match the broker order truth. Investigate fills and account state manually before any retry or clear.",
                }
            )
            continue
        if reason.startswith("live_canary_reconcile_broker_snapshot_stale:") or reason.startswith(
            "live_canary_reconcile_broker_snapshot_predates_launch:"
        ):
            actions.append(
                {
                    "action_id": "refresh_broker_snapshot",
                    "summary": "Refresh broker positions and balances, then rerun reconciliation with fresh broker/account truth.",
                }
            )
            continue
        if reason.startswith("conflicting_durable_state:") or reason.startswith("launch_state_mismatch:"):
            actions.append(
                {
                    "action_id": "inspect_local_state",
                    "summary": "Local live-canary state does not match the launch artifact. Inspect the event/session/submit-tracking records before any operator clear.",
                }
            )
            continue
        if reason.startswith("broker_order_account_mismatch:") or reason.startswith("broker_snapshot_account_mismatch:"):
            actions.append(
                {
                    "action_id": "verify_account_binding",
                    "summary": "Broker/account truth does not match the launch artifact account. Verify the exact account binding before proceeding.",
                }
            )
            continue
    if not actions:
        actions.append(
            {
                "action_id": "manual_review_required",
                "summary": "Reconciliation failed closed. Review the blocking reasons and verify broker truth manually before changing local state.",
            }
        )
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for action in actions:
        action_id = action["action_id"]
        if action_id in seen:
            continue
        seen.add(action_id)
        deduped.append(action)
    return deduped


def build_live_canary_reconciliation(
    *,
    launch_result_file: Path,
    broker: str,
    positions_file: Path | None,
    orders_file: Path | None,
    account_id: object = None,
    base_dir: Path | None = None,
    timestamp: datetime,
    tastytrade_challenge_code: str | None = None,
    tastytrade_challenge_token: str | None = None,
    secrets_file: Path | None = None,
) -> dict[str, Any]:
    resolved_launch_result_file = Path(launch_result_file)
    if not resolved_launch_result_file.exists():
        raise ValueError(f"Launch result artifact {resolved_launch_result_file} does not exist.")

    launch_payload = _read_json_object(resolved_launch_result_file, label="Launch result artifact")
    if launch_payload.get("schema_name") != LIVE_CANARY_LAUNCH_SCHEMA_NAME:
        raise ValueError("Launch result artifact schema_name is not live_canary_launch_result.")
    if launch_payload.get("schema_version") != LIVE_CANARY_LAUNCH_SCHEMA_VERSION:
        raise ValueError("Launch result artifact schema_version is unsupported.")

    launch_timestamp = _parse_iso_timestamp(
        _normalize_text(launch_payload.get("timestamp_chicago"), field_name="launch.timestamp_chicago"),
        field_name="launch.timestamp_chicago",
    )
    requested_live_submit = _coerce_bool(
        launch_payload.get("requested_live_submit"),
        field_name="launch.requested_live_submit",
    )
    submit_path_invoked = _coerce_bool(
        launch_payload.get("submit_path_invoked"),
        field_name="launch.submit_path_invoked",
    )
    submit_outcome = _normalize_text(launch_payload.get("submit_outcome"), field_name="launch.submit_outcome")
    readiness_verdict = _normalize_text(
        launch_payload.get("readiness_verdict"),
        field_name="launch.readiness_verdict",
    )

    event_context = launch_payload.get("event_context")
    if not isinstance(event_context, dict):
        raise ValueError("Launch result artifact must include event_context.")
    readiness_payload = launch_payload.get("readiness")
    readiness_payload = readiness_payload if isinstance(readiness_payload, dict) else {}
    readiness_scope = readiness_payload.get("scope")
    readiness_scope = readiness_scope if isinstance(readiness_scope, dict) else {}
    readiness_signal = readiness_payload.get("signal")
    readiness_signal = readiness_signal if isinstance(readiness_signal, dict) else {}

    event_id = _optional_text(event_context.get("event_id")) or _optional_text(readiness_scope.get("event_id")) or _optional_text(
        readiness_signal.get("event_id")
    )
    if event_id is None:
        raise ValueError("Launch result artifact is missing event_id context.")
    signal_date, strategy = parse_live_canary_event_scope(event_id)
    if signal_date is None or strategy is None:
        raise ValueError("Launch result artifact event_id does not use the Trading Codex event_id format.")

    launch_account_id = normalize_live_canary_account(event_context.get("account_id")) or normalize_live_canary_account(
        readiness_scope.get("account_id")
    )
    if launch_account_id is None:
        raise ValueError("Launch result artifact is missing account_id context.")
    configured_account_id = normalize_live_canary_account(account_id)
    if configured_account_id is not None and configured_account_id != launch_account_id:
        raise ValueError(
            f"--account-id {configured_account_id} does not match the launch artifact account_id {launch_account_id}."
        )
    broker_account_id = normalize_live_canary_account(event_context.get("broker_account_id"))
    symbol = _optional_text(event_context.get("symbol")) or _optional_text(readiness_signal.get("symbol"))
    action = _optional_text(event_context.get("action")) or _optional_text(readiness_signal.get("action"))

    launch_artifact_paths = launch_payload.get("artifact_paths")
    launch_artifact_paths = launch_artifact_paths if isinstance(launch_artifact_paths, dict) else {}
    launch_base_dir = _optional_text(launch_artifact_paths.get("live_canary_base_dir"))
    if launch_base_dir is not None and base_dir is not None and str(Path(base_dir)) != launch_base_dir:
        raise ValueError("--base-dir does not match the launch artifact live_canary_base_dir.")
    resolved_base_dir = resolve_live_canary_state_base_dir(
        Path(launch_base_dir) if launch_base_dir is not None else base_dir,
        create=True,
    )

    submit_result = launch_payload.get("submit_result")
    submit_result = submit_result if isinstance(submit_result, dict) else None
    submit_decision = None if submit_result is None else _optional_text(submit_result.get("decision"))
    live_submission = None if submit_result is None else submit_result.get("live_submission")
    live_submission = live_submission if isinstance(live_submission, dict) else None
    live_submit_attempted = None
    live_submission_manual_clearance_required = None
    if live_submission is not None:
        live_submit_attempted = _required_bool_field(
            live_submission,
            field_name="live_submit_attempted",
            prefix="launch.submit_result.live_submission",
        )
        live_submission_manual_clearance_required = _required_bool_field(
            live_submission,
            field_name="manual_clearance_required",
            prefix="launch.submit_result.live_submission",
        )
        _required_bool_field(
            live_submission,
            field_name="submission_succeeded",
            prefix="launch.submit_result.live_submission",
        )
    live_submission_fingerprint = _optional_text(event_context.get("live_submission_fingerprint")) or (
        None if live_submission is None else _optional_text(live_submission.get("live_submission_fingerprint"))
    )

    launch_orders_source = None
    if submit_result is not None and submit_result.get("orders") is not None:
        launch_orders_source = submit_result.get("orders")
    else:
        evaluation = readiness_payload.get("evaluation")
        evaluation = evaluation if isinstance(evaluation, dict) else {}
        launch_orders_source = evaluation.get("orders")
    launch_orders = _launch_orders_from_payload(launch_orders_source, field_name="launch.orders")
    launch_order_by_symbol = {order.symbol: order for order in launch_orders}

    if not requested_live_submit:
        mode = "preview_only"
    elif not submit_path_invoked:
        mode = "requested_but_not_invoked"
    elif live_submit_attempted is True:
        mode = "submit_attempted"
    elif submit_decision in {"blocked", "blocked_duplicate"} or live_submission is not None:
        mode = "submit_not_attempted"
    else:
        mode = "submit_receipt_missing"

    blocking_reasons: list[str] = []
    warnings: list[str] = []

    if broker_account_id is not None and broker_account_id != launch_account_id:
        blocking_reasons.append(f"launch_broker_account_mismatch:{broker_account_id}:{launch_account_id}")

    state_status = build_live_canary_state_status(
        base_dir=resolved_base_dir,
        account_id=launch_account_id,
        strategy=strategy,
        signal_date=signal_date,
        event_id=event_id,
        live_submission_fingerprint=live_submission_fingerprint,
    )
    artifacts_by_kind = _state_artifacts_by_kind(state_status)
    event_state_artifact = _assert_single_artifact(
        artifacts_by_kind=artifacts_by_kind,
        artifact_kind="event_state",
        blocking_reasons=blocking_reasons,
    )
    session_state_artifact = _assert_single_artifact(
        artifacts_by_kind=artifacts_by_kind,
        artifact_kind="session_state",
        blocking_reasons=blocking_reasons,
    )
    claim_artifact = _assert_single_artifact(
        artifacts_by_kind=artifacts_by_kind,
        artifact_kind="submit_tracking_claim",
        blocking_reasons=blocking_reasons,
    )
    ledger_artifact = _assert_single_artifact(
        artifacts_by_kind=artifacts_by_kind,
        artifact_kind="submit_tracking_ledger",
        blocking_reasons=blocking_reasons,
    )

    expected_event_state_path = None if submit_result is None else _optional_text(submit_result.get("event_state_path"))
    session_guard = None if submit_result is None else submit_result.get("session_guard")
    session_guard = session_guard if isinstance(session_guard, dict) else None
    expected_session_state_path = None if session_guard is None else _optional_text(session_guard.get("state_path"))
    expected_claim_path = _optional_text(launch_artifact_paths.get("submit_claim_path"))
    expected_ledger_path = _optional_text(launch_artifact_paths.get("submit_ledger_path"))

    if expected_event_state_path is not None:
        if event_state_artifact is None or event_state_artifact.get("path") != expected_event_state_path:
            blocking_reasons.append("launch_state_mismatch:event_state_path")
    if expected_session_state_path is not None:
        if session_state_artifact is None or session_state_artifact.get("path") != expected_session_state_path:
            blocking_reasons.append("launch_state_mismatch:session_state_path")
    if expected_ledger_path is not None:
        if ledger_artifact is None or ledger_artifact.get("path") != expected_ledger_path:
            blocking_reasons.append("launch_state_mismatch:submit_ledger_path")
    if expected_claim_path is not None and live_submission is not None and live_submission_manual_clearance_required:
        if claim_artifact is None or claim_artifact.get("path") != expected_claim_path:
            blocking_reasons.append("launch_state_mismatch:submit_claim_path")

    for artifact, artifact_name in (
        (event_state_artifact, "event_state"),
        (session_state_artifact, "session_state"),
    ):
        if artifact is None:
            continue
        record = artifact.get("record")
        if not isinstance(record, dict):
            blocking_reasons.append(f"launch_state_mismatch:{artifact_name}_record_missing")
            continue
        if _optional_text(record.get("account_id")) != launch_account_id:
            blocking_reasons.append(f"launch_state_mismatch:{artifact_name}_account_id")
        if artifact_name == "event_state":
            if _optional_text(record.get("event_id")) != event_id:
                blocking_reasons.append("launch_state_mismatch:event_state_event_id")
        else:
            if _optional_text(record.get("event_id")) != event_id:
                blocking_reasons.append("launch_state_mismatch:session_state_event_id")
            if _optional_text(record.get("signal_date")) != signal_date:
                blocking_reasons.append("launch_state_mismatch:session_state_signal_date")
            if _optional_text(record.get("strategy")) != strategy:
                blocking_reasons.append("launch_state_mismatch:session_state_strategy")
        if submit_result is not None and _optional_text(record.get("decision")) != _optional_text(submit_result.get("decision")):
            blocking_reasons.append(f"launch_state_mismatch:{artifact_name}_decision")
        record_live_submission = record.get("live_submission")
        if live_submission is not None:
            if not isinstance(record_live_submission, dict):
                blocking_reasons.append(f"launch_state_mismatch:{artifact_name}_live_submission_missing")
            elif _optional_text(record_live_submission.get("live_submission_fingerprint")) != live_submission_fingerprint:
                blocking_reasons.append(f"launch_state_mismatch:{artifact_name}_live_submission_fingerprint")

    if live_submission_fingerprint is not None and ledger_artifact is not None:
        latest_entry = ledger_artifact.get("latest_entry")
        if not isinstance(latest_entry, dict):
            blocking_reasons.append("launch_state_mismatch:submit_ledger_latest_entry_missing")
        else:
            if _optional_text(latest_entry.get("live_submission_fingerprint")) != live_submission_fingerprint:
                blocking_reasons.append("launch_state_mismatch:submit_ledger_fingerprint")
            expected_ledger_result = _optional_text(
                live_submission.get("submission_result") if live_submission is not None else None
            )
            latest_result = _optional_text(latest_entry.get("result"))
            if expected_ledger_result is not None and latest_result is not None and latest_result != expected_ledger_result:
                blocking_reasons.append(
                    f"launch_state_mismatch:submit_ledger_result:{latest_result}:{expected_ledger_result}"
                )

    broker_snapshot = None
    broker_truth_source = broker
    order_statuses: dict[str, BrokerOrderStatus] = {}
    order_adapter: Any | None = None
    if broker == "file":
        if positions_file is None:
            raise ValueError("--positions-file is required when --broker file.")
        position_adapter = FileBrokerPositionAdapter(Path(positions_file))
        broker_snapshot = position_adapter.load_snapshot()
        if mode == "submit_attempted":
            if orders_file is None:
                blocking_reasons.append("launch_broker_truth_missing:orders_file_required")
            else:
                order_adapter = FileBrokerOrderStatusAdapter(Path(orders_file))
    elif broker == "tastytrade":
        if positions_file is not None:
            raise ValueError("--positions-file cannot be used with --broker tastytrade.")
        if orders_file is not None:
            raise ValueError("--orders-file cannot be used with --broker tastytrade.")
        account_for_broker = broker_account_id or launch_account_id
        load_tastytrade_secrets(secrets_file=secrets_file)
        position_adapter = TastytradeBrokerPositionAdapter(
            account_id=account_for_broker,
            client=RequestsTastytradeHttpClient(
                challenge_code=tastytrade_challenge_code,
                challenge_token=tastytrade_challenge_token,
            ),
        )
        broker_truth_source = f"tastytrade:{account_for_broker}"
        broker_snapshot = position_adapter.load_snapshot()
        order_adapter = position_adapter
    else:
        raise ValueError(f"Unsupported broker: {broker}")

    assert broker_snapshot is not None
    if broker_snapshot.account_id is None:
        blocking_reasons.append("live_canary_reconcile_broker_snapshot_missing_account")
    elif broker_snapshot.account_id != launch_account_id:
        blocking_reasons.append(
            f"broker_snapshot_account_mismatch:{broker_snapshot.account_id}:{launch_account_id}"
        )

    if broker_snapshot.as_of is None:
        blocking_reasons.append("live_canary_reconcile_broker_snapshot_as_of_missing")
        broker_snapshot_as_of = None
    else:
        broker_snapshot_as_of = _parse_iso_timestamp(
            broker_snapshot.as_of,
            field_name="broker snapshot as_of",
        )
        if broker_snapshot_as_of < launch_timestamp:
            blocking_reasons.append(
                "live_canary_reconcile_broker_snapshot_predates_launch:"
                f"{broker_snapshot.as_of}:{launch_timestamp.isoformat()}"
            )
        snapshot_age = timestamp - broker_snapshot_as_of.astimezone(timestamp.tzinfo) if timestamp.tzinfo else timestamp - broker_snapshot_as_of
        if snapshot_age > DEFAULT_LIVE_CANARY_BROKER_SNAPSHOT_MAX_AGE:
            blocking_reasons.append(
                "live_canary_reconcile_broker_snapshot_stale:"
                f"{int(snapshot_age.total_seconds())}:"
                f"{int(DEFAULT_LIVE_CANARY_BROKER_SNAPSHOT_MAX_AGE.total_seconds())}"
            )

    order_truth: list[dict[str, Any]] = []
    if mode == "submit_attempted":
        if live_submission is None:
            blocking_reasons.append("launch_broker_truth_missing:live_submission_receipt")
        else:
            receipt_orders = _receipt_orders_from_payload(live_submission.get("orders"))
            if not receipt_orders:
                blocking_reasons.append("launch_broker_truth_missing:receipt_orders")
            for receipt_order in receipt_orders:
                symbol = receipt_order["symbol"]
                launch_order = launch_order_by_symbol.get(symbol)
                if launch_order is None:
                    blocking_reasons.append(f"launch_receipt_order_not_in_launch_orders:{symbol}")
                    continue
                if launch_order.side != receipt_order["side"]:
                    blocking_reasons.append(
                        f"launch_receipt_order_side_mismatch:{symbol}:{receipt_order['side']}:{launch_order.side}"
                    )
                if launch_order.requested_quantity != receipt_order["quantity"]:
                    blocking_reasons.append(
                        f"launch_receipt_order_quantity_mismatch:{symbol}:{receipt_order['quantity']}:{launch_order.requested_quantity}"
                    )
                broker_order_id = receipt_order["broker_order_id"]
                if broker_order_id is None:
                    blocking_reasons.append(f"launch_artifact_missing_broker_order_id:{symbol}")
                    continue
                if order_adapter is not None:
                    order_statuses[broker_order_id] = order_adapter.load_order_statuses(order_ids=[broker_order_id])[
                        broker_order_id
                    ]

            for receipt_order in receipt_orders:
                broker_order_id = receipt_order["broker_order_id"]
                launch_order = launch_order_by_symbol.get(receipt_order["symbol"])
                if broker_order_id is None or launch_order is None or broker_order_id not in order_statuses:
                    continue
                actual_position_shares = broker_snapshot.positions.get(launch_order.symbol).shares if launch_order.symbol in broker_snapshot.positions else 0
                order_truth.append(
                    _classify_order_truth(
                        launch_order=launch_order,
                        receipt_order=receipt_order,
                        broker_order_status=order_statuses[broker_order_id],
                        actual_position_shares=actual_position_shares,
                        expected_account_id=launch_account_id,
                    )
                )
        if live_submission is not None and live_submission.get("submission_result") == "ambiguous_attempted_submit_manual_clearance_required":
            warnings.append("Launch recorded an ambiguous attempted submit and required manual clearance.")

    if mode == "submit_receipt_missing":
        blocking_reasons.append("launch_broker_truth_missing:submit_path_without_receipt")

    if submit_decision == "live_submit_error":
        blocking_reasons.append("launch_broker_truth_missing:submit_live_orders_error")

    for order in order_truth:
        blocking_reasons.extend(order["blocking_reasons"])

    if mode == "submit_attempted" and not blocking_reasons:
        if not order_truth:
            blocking_reasons.append("launch_broker_truth_missing:no_reconciled_orders")
        elif any(order["fill_state"] != "filled" or not order["position_truth_matched"] for order in order_truth):
            blocking_reasons.append("launch_closeout_incomplete")

    verdict = LIVE_CANARY_RECONCILIATION_VERDICT_BLOCKED
    if not blocking_reasons:
        if mode == "submit_attempted":
            verdict = LIVE_CANARY_RECONCILIATION_VERDICT_RECONCILED
        else:
            verdict = LIVE_CANARY_RECONCILIATION_VERDICT_NOT_APPLICABLE

    result_path = _build_reconciliation_result_path(
        resolved_base_dir=resolved_base_dir,
        launch_result_file=resolved_launch_result_file,
        timestamp=timestamp,
        account_id=launch_account_id,
        strategy=strategy,
        event_id=event_id,
        live_submission_fingerprint=live_submission_fingerprint,
        verdict=verdict,
    )

    relevant_symbols = sorted(
        {order.symbol for order in launch_orders}
        | ({symbol} if symbol is not None else set())
    )
    broker_positions = [
        {
            "actual_shares": broker_snapshot.positions.get(symbol_name).shares if symbol_name in broker_snapshot.positions else 0,
            "instrument_type": (
                broker_snapshot.positions[symbol_name].instrument_type if symbol_name in broker_snapshot.positions else None
            ),
            "price": broker_snapshot.positions[symbol_name].price if symbol_name in broker_snapshot.positions else None,
            "symbol": symbol_name,
        }
        for symbol_name in relevant_symbols
    ]

    next_actions = _reconcile_next_actions(
        mode=mode,
        verdict=verdict,
        blocking_reasons=_dedupe_preserve(blocking_reasons),
    )

    return {
        "schema_name": LIVE_CANARY_RECONCILIATION_SCHEMA_NAME,
        "schema_version": LIVE_CANARY_RECONCILIATION_SCHEMA_VERSION,
        "timestamp_chicago": timestamp.isoformat(),
        "verdict": verdict,
        "mode": mode,
        "blocking_reasons": _dedupe_preserve(blocking_reasons),
        "warnings": _dedupe_preserve(warnings),
        "next_actions": next_actions,
        "summary": {
            "blocking_reason_count": len(_dedupe_preserve(blocking_reasons)),
            "warning_count": len(_dedupe_preserve(warnings)),
            "launch_order_count": len(launch_orders),
            "reconciled_order_count": len(order_truth),
        },
        "launch": {
            "path": str(resolved_launch_result_file),
            "timestamp_chicago": launch_payload["timestamp_chicago"],
            "requested_live_submit": requested_live_submit,
            "submit_outcome": submit_outcome,
            "submit_path_invoked": submit_path_invoked,
            "readiness_verdict": readiness_verdict,
            "operator_message": _optional_text(launch_payload.get("operator_message")),
        },
        "context": {
            "account_id": launch_account_id,
            "action": action,
            "broker_account_id": broker_account_id,
            "event_id": event_id,
            "live_submission_fingerprint": live_submission_fingerprint,
            "signal_date": signal_date,
            "strategy": strategy,
            "symbol": symbol,
        },
        "artifact_paths": {
            "launch_result_path": str(resolved_launch_result_file),
            "result_path": str(result_path),
            "live_canary_base_dir": str(resolved_base_dir),
            "positions_file": None if positions_file is None else str(Path(positions_file)),
            "orders_file": None if orders_file is None else str(Path(orders_file)),
            "event_state_path": expected_event_state_path,
            "session_state_path": expected_session_state_path,
            "submit_claim_path": expected_claim_path,
            "submit_ledger_path": expected_ledger_path,
            "operator_state_ops_audit_path": state_status.get("operator_state_ops_audit_path"),
        },
        "launch_artifact_paths": dict(launch_artifact_paths),
        "durable_state": {
            "state_status": {
                "scope": state_status.get("scope"),
                "summary": state_status.get("summary"),
            },
            "event_state": None
            if event_state_artifact is None
            else {
                "path": event_state_artifact.get("path"),
                "record": event_state_artifact.get("record"),
            },
            "session_state": None
            if session_state_artifact is None
            else {
                "path": session_state_artifact.get("path"),
                "record": session_state_artifact.get("record"),
            },
            "submit_tracking_claim": None
            if claim_artifact is None
            else {
                "path": claim_artifact.get("path"),
                "record": claim_artifact.get("record"),
            },
            "submit_tracking_ledger": None
            if ledger_artifact is None
            else {
                "path": ledger_artifact.get("path"),
                "latest_entry": ledger_artifact.get("latest_entry"),
                "latest_result": ledger_artifact.get("latest_result"),
                "summary": ledger_artifact.get("summary"),
            },
        },
        "broker_truth": {
            "source": broker_truth_source,
            "snapshot": {
                "account_id": broker_snapshot.account_id,
                "as_of": broker_snapshot.as_of,
                "broker_name": broker_snapshot.broker_name,
                "buying_power": broker_snapshot.buying_power,
                "cash": broker_snapshot.cash,
                "relevant_positions": broker_positions,
            },
            "orders": order_truth,
        },
    }
