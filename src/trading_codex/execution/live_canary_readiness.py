from __future__ import annotations

import json
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.execution import (
    FileBrokerPositionAdapter,
    RequestsTastytradeHttpClient,
    TastytradeBrokerPositionAdapter,
    build_execution_plan,
    parse_signal_payload,
)
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_MISSING,
    LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_UNPARSEABLE,
    LIVE_CANARY_MARKET_HOLIDAY_BLOCKER_PREFIX,
    LIVE_CANARY_REGULAR_SESSION_BLOCKER,
    LIVE_CANARY_SIGNAL_DATE_MISMATCH_PREFIX,
    LIVE_CANARY_SIGNAL_DATE_UNPARSEABLE,
    LIVE_CANARY_SUBMISSION_CAP_BLOCKER,
    evaluate_live_canary,
    normalize_live_canary_account,
    render_live_canary_affordability,
)
from trading_codex.execution.live_canary_state_ops import build_live_canary_state_status
from trading_codex.execution.secrets import load_tastytrade_secrets


LIVE_CANARY_READINESS_SCHEMA_NAME = "live_canary_readiness"
LIVE_CANARY_READINESS_SCHEMA_VERSION = 1
LIVE_CANARY_NO_EXECUTABLE_ORDER = "live_canary_no_executable_order"
LIVE_CANARY_SIGNAL_LOAD_ERROR_PREFIX = "live_canary_signal_load_error"
LIVE_CANARY_BROKER_LOAD_ERROR_PREFIX = "live_canary_broker_snapshot_load_error"
LIVE_CANARY_PLAN_BUILD_ERROR_PREFIX = "live_canary_plan_build_error"
LIVE_CANARY_STATE_STATUS_ERROR_PREFIX = "live_canary_state_status_error"

READINESS_GATE_ORDER = (
    "input_readiness",
    "account_binding",
    "manual_arming",
    "session_readiness",
    "canary_order_readiness",
    "affordability",
    "duplicate_state",
    "operator_state_ops",
    "other_guardrails",
)


def _dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _load_signal_from_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Signal JSON file must contain a JSON object.")
    return payload


def _render_live_canary_order(order: Any) -> dict[str, Any]:
    return {
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


def _gate_payload(
    *,
    gate: str,
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    details: dict[str, Any] | None = None,
    assessed: bool,
) -> dict[str, Any]:
    rendered_blockers = _dedupe_preserve(list(blockers or []))
    rendered_warnings = _dedupe_preserve(list(warnings or []))
    status = "fail" if rendered_blockers else ("pass" if assessed else "not_assessed")
    return {
        "gate": gate,
        "status": status,
        "blocking_reasons": rendered_blockers,
        "warnings": rendered_warnings,
        "details": details or {},
    }


def _reason_has_prefix(reason: str, prefix: str) -> bool:
    return reason == prefix or reason.startswith(f"{prefix}:")


def _is_session_readiness_blocker(reason: str) -> bool:
    return (
        reason == LIVE_CANARY_REGULAR_SESSION_BLOCKER
        or _reason_has_prefix(reason, LIVE_CANARY_MARKET_HOLIDAY_BLOCKER_PREFIX)
        or _reason_has_prefix(reason, LIVE_CANARY_SIGNAL_DATE_MISMATCH_PREFIX)
        or reason in {
            LIVE_CANARY_SIGNAL_DATE_UNPARSEABLE,
            LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_MISSING,
            LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_UNPARSEABLE,
        }
        or reason.startswith("live_canary_broker_snapshot_stale:")
    )


def _is_affordability_blocker(reason: str) -> bool:
    return any(
        reason.startswith(prefix)
        for prefix in (
            "live_canary_affordability_unavailable:",
            "live_canary_buy_order_notional_exceeds_available:",
            "live_canary_total_buy_notional_exceeds_available:",
            "live_canary_missing_estimated_notional:",
        )
    )


def _is_canary_order_blocker(reason: str) -> bool:
    return (
        reason.startswith(f"{LIVE_CANARY_SUBMISSION_CAP_BLOCKER}:")
        or reason.startswith("live_canary_missing_reference_price:")
        or reason.startswith("live_canary_symbol_not_allowed:")
        or reason.startswith("live_canary_unsupported_action:")
        or reason == LIVE_CANARY_NO_EXECUTABLE_ORDER
    )


def _base_command_parts(
    *,
    program: str,
    signal_json_file: Path,
    broker: str,
    positions_file: Path | None,
    account_id: str | None,
    arm_live_canary: str | None,
    ack_unmanaged_holdings: bool,
    base_dir: Path | None,
    secrets_file: Path | None,
) -> list[str]:
    parts = [
        ".venv/bin/python",
        program,
        "--signal-json-file",
        str(signal_json_file),
        "--broker",
        broker,
    ]
    if broker == "file" and positions_file is not None:
        parts.extend(["--positions-file", str(positions_file)])
    if account_id is not None:
        account_flag = "--account-id" if program.endswith("live_canary_state_ops.py") else "--live-canary-account"
        parts.extend([account_flag, account_id])
    if arm_live_canary is not None:
        parts.extend(["--arm-live-canary", arm_live_canary])
    if ack_unmanaged_holdings:
        parts.append("--ack-unmanaged-holdings")
    if base_dir is not None:
        parts.extend(["--base-dir", str(base_dir)])
    if broker == "tastytrade" and secrets_file is not None:
        parts.extend(["--secrets-file", str(secrets_file)])
    return parts


def _readiness_command(
    *,
    signal_json_file: Path,
    broker: str,
    positions_file: Path | None,
    account_id: str | None,
    arm_live_canary: str | None,
    ack_unmanaged_holdings: bool,
    base_dir: Path | None,
    secrets_file: Path | None,
) -> str:
    parts = _base_command_parts(
        program="scripts/live_canary_state_ops.py",
        signal_json_file=signal_json_file,
        broker=broker,
        positions_file=positions_file,
        account_id=account_id,
        arm_live_canary=arm_live_canary,
        ack_unmanaged_holdings=ack_unmanaged_holdings,
        base_dir=base_dir,
        secrets_file=secrets_file,
    )
    parts.insert(2, "readiness")
    return shlex.join(parts)


def _submit_command(
    *,
    signal_json_file: Path,
    broker: str,
    positions_file: Path | None,
    account_id: str | None,
    ack_unmanaged_holdings: bool,
    base_dir: Path | None,
    secrets_file: Path | None,
) -> str | None:
    if account_id is None:
        return None
    parts = _base_command_parts(
        program="scripts/live_canary_guardrails.py",
        signal_json_file=signal_json_file,
        broker=broker,
        positions_file=positions_file,
        account_id=account_id,
        arm_live_canary=account_id,
        ack_unmanaged_holdings=ack_unmanaged_holdings,
        base_dir=base_dir,
        secrets_file=secrets_file,
    )
    parts.extend(["--live-submit", "--emit", "json"])
    return shlex.join(parts)


def _state_clear_command(
    *,
    base_dir: Path | None,
    account_id: str,
    clear_scope: str,
    strategy: str | None = None,
    signal_date: str | None = None,
    event_id: str | None = None,
    live_submission_fingerprint: str | None = None,
) -> str:
    parts = [
        ".venv/bin/python",
        "scripts/live_canary_state_ops.py",
        "clear",
        "--account-id",
        account_id,
    ]
    if strategy is not None:
        parts.extend(["--strategy", strategy])
    if signal_date is not None:
        parts.extend(["--signal-date", signal_date])
    if event_id is not None:
        parts.extend(["--event-id", event_id])
    if live_submission_fingerprint is not None:
        parts.extend(["--live-submission-fingerprint", live_submission_fingerprint])
    parts.extend(["--clear", clear_scope])
    if base_dir is not None:
        parts.extend(["--base-dir", str(base_dir)])
    return shlex.join(parts)


def _state_status_command(
    *,
    base_dir: Path | None,
    account_id: str,
    live_submission_fingerprint: str,
) -> str:
    parts = [
        ".venv/bin/python",
        "scripts/live_canary_state_ops.py",
        "status",
        "--account-id",
        account_id,
        "--live-submission-fingerprint",
        live_submission_fingerprint,
    ]
    if base_dir is not None:
        parts.extend(["--base-dir", str(base_dir)])
    return shlex.join(parts)


def _build_next_actions(
    *,
    signal_json_file: Path,
    broker: str,
    positions_file: Path | None,
    configured_account_id: str | None,
    broker_account_id: str | None,
    arm_live_canary: str | None,
    ack_unmanaged_holdings: bool,
    base_dir: Path | None,
    secrets_file: Path | None,
    blocking_reasons: list[str],
    duplicate_artifacts: list[dict[str, Any]],
    operator_artifacts: list[dict[str, Any]],
    verdict: str,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []

    def add_action(
        *,
        action_id: str,
        reason: str,
        summary: str,
        command: str | None = None,
    ) -> None:
        actions.append(
            {
                "action_id": action_id,
                "reason": reason,
                "summary": summary,
                "command": command,
            }
        )

    if verdict == "ready":
        submit_command = _submit_command(
            signal_json_file=signal_json_file,
            broker=broker,
            positions_file=positions_file,
            account_id=configured_account_id,
            ack_unmanaged_holdings=ack_unmanaged_holdings,
            base_dir=base_dir,
            secrets_file=secrets_file,
        )
        if submit_command is not None:
            add_action(
                action_id="run_live_canary_submit",
                reason="ready",
                summary="Run the live-canary submit workflow with the same inputs.",
                command=submit_command,
            )
        return actions

    for reason in blocking_reasons:
        if reason.startswith(f"{LIVE_CANARY_SIGNAL_LOAD_ERROR_PREFIX}:"):
            add_action(
                action_id="fix_signal_input",
                reason=reason,
                summary="Fix or regenerate the signal JSON file, then rerun readiness.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith(f"{LIVE_CANARY_BROKER_LOAD_ERROR_PREFIX}:"):
            add_action(
                action_id="refresh_broker_input",
                reason=reason,
                summary="Fix or refresh the broker snapshot input, then rerun readiness.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith(f"{LIVE_CANARY_PLAN_BUILD_ERROR_PREFIX}:"):
            add_action(
                action_id="fix_plan_inputs",
                reason=reason,
                summary="Fix the signal or broker input mismatch that prevented plan construction, then rerun readiness.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith(f"{LIVE_CANARY_STATE_STATUS_ERROR_PREFIX}:"):
            add_action(
                action_id="repair_live_canary_state",
                reason=reason,
                summary="Inspect and repair the unreadable live-canary state or submit-tracking files before attempting live canary.",
            )
            continue
        if reason == "live_canary_requires_account_binding":
            target_account = broker_account_id or "<LIVE_ACCOUNT_ID>"
            add_action(
                action_id="set_account_binding",
                reason=reason,
                summary="Set the live-canary account binding to the exact broker account id, then rerun readiness.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=target_account,
                    arm_live_canary=None,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason == "live_canary_account_binding_mismatch":
            target_account = broker_account_id or "<LIVE_ACCOUNT_ID>"
            add_action(
                action_id="fix_account_binding_mismatch",
                reason=reason,
                summary="Use the exact broker snapshot account id for the live-canary binding, then rerun readiness.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=target_account,
                    arm_live_canary=target_account if target_account == broker_account_id else None,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason == "live_canary_broker_snapshot_missing_account":
            add_action(
                action_id="refresh_broker_snapshot_account",
                reason=reason,
                summary="Refresh the broker snapshot so the broker account id is present before live canary.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason == "live_canary_not_armed":
            if configured_account_id is not None:
                add_action(
                    action_id="arm_live_canary",
                    reason=reason,
                    summary="Arm the live canary with the bound account id and rerun readiness.",
                    command=_readiness_command(
                        signal_json_file=signal_json_file,
                        broker=broker,
                        positions_file=positions_file,
                        account_id=configured_account_id,
                        arm_live_canary=configured_account_id,
                        ack_unmanaged_holdings=ack_unmanaged_holdings,
                        base_dir=base_dir,
                        secrets_file=secrets_file,
                    ),
                )
            continue
        if reason == LIVE_CANARY_REGULAR_SESSION_BLOCKER:
            add_action(
                action_id="wait_for_regular_session",
                reason=reason,
                summary="Wait for the next NYSE regular session window, then rerun readiness with a fresh broker snapshot.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith(f"{LIVE_CANARY_MARKET_HOLIDAY_BLOCKER_PREFIX}:"):
            add_action(
                action_id="wait_for_market_open_day",
                reason=reason,
                summary="Wait until the next NYSE trading day after the holiday, then rerun readiness with fresh broker data.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith(f"{LIVE_CANARY_SIGNAL_DATE_MISMATCH_PREFIX}:") or reason == LIVE_CANARY_SIGNAL_DATE_UNPARSEABLE:
            add_action(
                action_id="regenerate_signal",
                reason=reason,
                summary="Regenerate the next_action signal so the signal date matches the latest completed regular session, then rerun readiness.",
            )
            continue
        if reason in {
            LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_MISSING,
            LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_UNPARSEABLE,
        } or reason.startswith("live_canary_broker_snapshot_stale:"):
            add_action(
                action_id="refresh_broker_snapshot",
                reason=reason,
                summary="Refresh the broker snapshot and balances so the live-canary data is present, parseable, and within the freshness limit.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith("live_canary_affordability_unavailable:"):
            add_action(
                action_id="restore_affordability_inputs",
                reason=reason,
                summary="Refresh buying power or cash data so affordability can be proven safe before live canary.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if _is_affordability_blocker(reason):
            add_action(
                action_id="fix_affordability",
                reason=reason,
                summary="Increase available buying power or cash, or reduce conflicting exposure, until the canary buy notional fits the existing guardrails.",
            )
            continue
        if reason.startswith(f"{LIVE_CANARY_SUBMISSION_CAP_BLOCKER}:"):
            add_action(
                action_id="reduce_existing_position_below_cap",
                reason=reason,
                summary="Reduce the existing live position below the canary cap before attempting this live canary.",
            )
            continue
        if reason.startswith("live_canary_missing_reference_price:"):
            add_action(
                action_id="refresh_reference_price",
                reason=reason,
                summary="Refresh the broker snapshot so every required live-canary order has a reference price.",
                command=_readiness_command(
                    signal_json_file=signal_json_file,
                    broker=broker,
                    positions_file=positions_file,
                    account_id=configured_account_id,
                    arm_live_canary=arm_live_canary,
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    base_dir=base_dir,
                    secrets_file=secrets_file,
                ),
            )
            continue
        if reason.startswith("live_canary_symbol_not_allowed:"):
            add_action(
                action_id="use_supported_live_canary_symbol",
                reason=reason,
                summary="Do not run live canary for an unsupported symbol. Regenerate the signal or skip live canary.",
            )
            continue
        if reason.startswith("live_canary_unsupported_action:"):
            add_action(
                action_id="use_supported_live_canary_action",
                reason=reason,
                summary="Regenerate the signal with a supported live-canary action before attempting live submit.",
            )
            continue
        if reason == LIVE_CANARY_NO_EXECUTABLE_ORDER:
            add_action(
                action_id="no_submit_required",
                reason=reason,
                summary="No executable live-canary order is pending for this signal. Do not run live submit.",
            )
            continue
        add_action(
            action_id=f"review_blocker:{reason}",
            reason=reason,
            summary="Resolve the reported blocker and rerun readiness before live canary.",
        )

    for artifact in duplicate_artifacts:
        artifact_kind = str(artifact.get("artifact_kind"))
        scope = artifact.get("scope") or {}
        blocking_reason = str(artifact.get("blocking_reason") or artifact_kind)
        if artifact_kind == "event_state" and configured_account_id is not None:
            add_action(
                action_id=f"clear_event_state:{scope.get('event_id')}",
                reason=blocking_reason,
                summary="If this exact-event blocker is stale and the retry is intentional, preview a narrow event-state clear.",
                command=_state_clear_command(
                    base_dir=base_dir,
                    account_id=configured_account_id,
                    clear_scope="event",
                    event_id=scope.get("event_id"),
                ),
            )
        if artifact_kind == "session_state" and configured_account_id is not None:
            add_action(
                action_id=f"clear_session_state:{scope.get('strategy')}:{scope.get('signal_date')}",
                reason=blocking_reason,
                summary="If this same-session blocker is stale and the retry is intentional, preview a narrow session-state clear.",
                command=_state_clear_command(
                    base_dir=base_dir,
                    account_id=configured_account_id,
                    clear_scope="session",
                    strategy=scope.get("strategy"),
                    signal_date=scope.get("signal_date"),
                ),
            )

    for artifact in operator_artifacts:
        fingerprint = artifact.get("live_submission_fingerprint")
        if configured_account_id is None or not isinstance(fingerprint, str) or not fingerprint.strip():
            continue
        blocking_reason = str(artifact.get("blocking_reason") or artifact.get("artifact_kind") or "operator_state_ops")
        scope_precision = artifact.get("scope_precision")
        if scope_precision == "legacy_unscoped":
            add_action(
                action_id=f"inspect_legacy_submit_tracking:{fingerprint}",
                reason=blocking_reason,
                summary="Inspect the exact legacy submit-tracking fingerprint scope before deciding whether a narrow clear is safe.",
                command=_state_status_command(
                    base_dir=base_dir,
                    account_id=configured_account_id,
                    live_submission_fingerprint=fingerprint,
                ),
            )
        else:
            add_action(
                action_id=f"clear_submit_tracking:{fingerprint}",
                reason=blocking_reason,
                summary="If this duplicate-protection blocker is stale and the retry is intentional, preview a narrow submit-tracking clear.",
                command=_state_clear_command(
                    base_dir=base_dir,
                    account_id=configured_account_id,
                    clear_scope="submit-tracking",
                    live_submission_fingerprint=fingerprint,
                ),
            )

    deduped_actions: list[dict[str, Any]] = []
    seen_action_ids: set[str] = set()
    for action in actions:
        action_id = str(action["action_id"])
        if action_id in seen_action_ids:
            continue
        seen_action_ids.add(action_id)
        deduped_actions.append(action)
    return deduped_actions


def build_live_canary_readiness(
    *,
    signal_json_file: Path,
    broker: str,
    positions_file: Path | None,
    account_id: object,
    arm_live_canary: object = None,
    ack_unmanaged_holdings: bool = False,
    base_dir: Path | None = None,
    timestamp: datetime,
    tastytrade_challenge_code: str | None = None,
    tastytrade_challenge_token: str | None = None,
    secrets_file: Path | None = None,
) -> dict[str, Any]:
    resolved_signal_json_file = Path(signal_json_file)
    resolved_positions_file = None if positions_file is None else Path(positions_file)
    resolved_base_dir = None if base_dir is None else Path(base_dir)
    configured_account_id = normalize_live_canary_account(account_id)
    arm_value = normalize_live_canary_account(arm_live_canary)

    signal = None
    raw_signal = None
    broker_snapshot = None
    plan = None
    evaluation = None
    state_status = None
    input_errors: list[str] = []
    state_status_error: str | None = None

    try:
        raw_signal = _load_signal_from_file(resolved_signal_json_file)
        signal = parse_signal_payload(raw_signal)
    except Exception as exc:
        input_errors.append(f"{LIVE_CANARY_SIGNAL_LOAD_ERROR_PREFIX}:{exc}")

    if signal is not None:
        try:
            if broker == "file":
                if resolved_positions_file is None:
                    raise ValueError("--positions-file is required when --broker file.")
                broker_adapter = FileBrokerPositionAdapter(resolved_positions_file)
                broker_source_ref = str(resolved_positions_file)
            elif broker == "tastytrade":
                if configured_account_id is None:
                    raise ValueError("live_canary_requires_account_binding")
                load_tastytrade_secrets(secrets_file=secrets_file)
                broker_adapter = TastytradeBrokerPositionAdapter(
                    account_id=configured_account_id,
                    client=RequestsTastytradeHttpClient(
                        challenge_code=tastytrade_challenge_code,
                        challenge_token=tastytrade_challenge_token,
                    ),
                )
                broker_source_ref = f"tastytrade:{configured_account_id}"
            else:
                raise ValueError(f"Unsupported broker: {broker}")
            broker_snapshot = broker_adapter.load_snapshot()
        except Exception as exc:
            rendered_error = str(exc)
            if rendered_error == "live_canary_requires_account_binding":
                input_errors.append(rendered_error)
            else:
                input_errors.append(f"{LIVE_CANARY_BROKER_LOAD_ERROR_PREFIX}:{exc}")
        else:
            try:
                plan = build_execution_plan(
                    signal=signal,
                    broker_snapshot=broker_snapshot,
                    account_scope="managed_sleeve",
                    managed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
                    ack_unmanaged_holdings=ack_unmanaged_holdings,
                    source_kind="signal_json_file",
                    source_label=resolved_signal_json_file.stem,
                    source_ref=str(resolved_signal_json_file),
                    broker_source_ref=broker_source_ref,
                    data_dir=None,
                )
            except Exception as exc:
                input_errors.append(f"{LIVE_CANARY_PLAN_BUILD_ERROR_PREFIX}:{exc}")
            else:
                evaluation = evaluate_live_canary(
                    plan=plan,
                    live_canary_account=configured_account_id,
                    live_submit_requested=True,
                    arm_live_canary=arm_value,
                    allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
                    timestamp=timestamp,
                )

    if signal is not None and configured_account_id is not None:
        try:
            state_status = build_live_canary_state_status(
                base_dir=resolved_base_dir,
                account_id=configured_account_id,
                strategy=signal.strategy,
                signal_date=signal.date,
                event_id=signal.event_id,
            )
        except Exception as exc:
            state_status_error = f"{LIVE_CANARY_STATE_STATUS_ERROR_PREFIX}:{exc}"

    input_gate = _gate_payload(
        gate="input_readiness",
        blockers=input_errors,
        assessed=True,
        details={
            "base_dir": None if resolved_base_dir is None else str(resolved_base_dir),
            "broker": broker,
            "positions_file": None if resolved_positions_file is None else str(resolved_positions_file),
            "signal_json_file": str(resolved_signal_json_file),
        },
    )

    if evaluation is not None:
        account_blockers = [
            blocker
            for blocker in evaluation.blockers
            if blocker in {
                "live_canary_requires_account_binding",
                "live_canary_broker_snapshot_missing_account",
                "live_canary_account_binding_mismatch",
            }
        ]
        account_gate = _gate_payload(
            gate="account_binding",
            blockers=account_blockers,
            assessed=True,
            details={
                "configured_account_id": configured_account_id,
                "broker_account_id": evaluation.broker_account_id,
            },
        )
        manual_arming_gate = _gate_payload(
            gate="manual_arming",
            blockers=[blocker for blocker in evaluation.blockers if blocker == "live_canary_not_armed"],
            assessed=True,
            details={
                "armed": evaluation.armed,
                "configured_account_id": configured_account_id,
                "requested_arm_value": arm_value,
            },
        )
    else:
        account_gate = _gate_payload(
            gate="account_binding",
            blockers=[] if configured_account_id is not None else ["live_canary_requires_account_binding"],
            assessed=configured_account_id is None,
            details={
                "configured_account_id": configured_account_id,
                "broker_account_id": None if broker_snapshot is None else broker_snapshot.account_id,
            },
        )
        manual_arming_blockers = []
        if configured_account_id is not None and arm_value != configured_account_id:
            manual_arming_blockers.append("live_canary_not_armed")
        manual_arming_gate = _gate_payload(
            gate="manual_arming",
            blockers=manual_arming_blockers,
            assessed=configured_account_id is not None,
            details={
                "armed": bool(configured_account_id is not None and arm_value == configured_account_id),
                "configured_account_id": configured_account_id,
                "requested_arm_value": arm_value,
            },
        )

    if evaluation is not None:
        session_blockers = [blocker for blocker in evaluation.blockers if _is_session_readiness_blocker(blocker)]
        session_warnings = [warning for warning in evaluation.warnings if _is_session_readiness_blocker(warning)]
        rendered_affordability = render_live_canary_affordability(evaluation.affordability)
        rendered_orders = [_render_live_canary_order(order) for order in evaluation.orders]
        canary_order_blockers = [blocker for blocker in evaluation.blockers if _is_canary_order_blocker(blocker)]
        if evaluation.decision in {"noop", "noop_cash", "noop_hold"}:
            canary_order_blockers.append(LIVE_CANARY_NO_EXECUTABLE_ORDER)
        affordability_blockers = [blocker for blocker in evaluation.blockers if _is_affordability_blocker(blocker)]
        other_guardrail_blockers = [
            blocker
            for blocker in evaluation.blockers
            if blocker
            not in (
                set(account_gate["blocking_reasons"])
                | set(manual_arming_gate["blocking_reasons"])
                | set(session_blockers)
                | set(canary_order_blockers)
                | set(affordability_blockers)
            )
        ]
        session_gate = _gate_payload(
            gate="session_readiness",
            blockers=session_blockers,
            warnings=session_warnings,
            assessed=True,
            details={
                "broker_snapshot_as_of": plan.broker_snapshot.as_of if plan is not None else None,
                "signal_date": signal.date if signal is not None else None,
                "timestamp_chicago": timestamp.isoformat(),
            },
        )
        canary_order_gate = _gate_payload(
            gate="canary_order_readiness",
            blockers=canary_order_blockers,
            warnings=[
                warning
                for warning in evaluation.warnings
                if warning.startswith("live_canary_qty_capped:")
                or warning.startswith("live_canary_missing_reference_price:")
            ],
            assessed=True,
            details={
                "decision": evaluation.decision,
                "orders": rendered_orders,
            },
        )
        affordability_gate = _gate_payload(
            gate="affordability",
            blockers=affordability_blockers,
            assessed=True,
            details={
                "affordability": rendered_affordability,
            },
        )
        other_guardrails_gate = _gate_payload(
            gate="other_guardrails",
            blockers=other_guardrail_blockers,
            warnings=[
                warning
                for warning in evaluation.warnings
                if warning
                not in set(session_gate["warnings"]) | set(canary_order_gate["warnings"])
            ],
            assessed=True,
            details={
                "decision": evaluation.decision,
            },
        )
    else:
        session_gate = _gate_payload(gate="session_readiness", assessed=False)
        canary_order_gate = _gate_payload(gate="canary_order_readiness", assessed=False)
        affordability_gate = _gate_payload(gate="affordability", assessed=False)
        other_guardrails_gate = _gate_payload(gate="other_guardrails", assessed=False)
        rendered_affordability = None
        rendered_orders = []

    duplicate_artifacts: list[dict[str, Any]] = []
    operator_artifacts: list[dict[str, Any]] = []
    if state_status is not None:
        duplicate_artifacts = [
            artifact
            for artifact in state_status.get("blocking_artifacts", [])
            if artifact.get("artifact_kind") in {"event_state", "session_state"}
        ]
        operator_artifacts = [
            artifact
            for artifact in state_status.get("blocking_artifacts", [])
            if artifact.get("artifact_kind") in {"submit_tracking_claim", "submit_tracking_ledger"}
        ]

    duplicate_gate_blockers = [
        str(artifact.get("blocking_reason"))
        for artifact in duplicate_artifacts
        if artifact.get("blocking_reason")
    ]
    operator_gate_blockers = [
        str(artifact.get("blocking_reason"))
        for artifact in operator_artifacts
        if artifact.get("blocking_reason")
    ]
    if state_status_error is not None:
        operator_gate_blockers.append(state_status_error)

    duplicate_gate = _gate_payload(
        gate="duplicate_state",
        blockers=duplicate_gate_blockers,
        assessed=state_status is not None,
        details={
            "blocking_artifacts": duplicate_artifacts,
            "summary": None if state_status is None else state_status.get("summary"),
        },
    )
    operator_state_ops_gate = _gate_payload(
        gate="operator_state_ops",
        blockers=operator_gate_blockers,
        assessed=state_status is not None or state_status_error is not None,
        details={
            "blocking_artifacts": operator_artifacts,
            "summary": None if state_status is None else state_status.get("summary"),
        },
    )

    gates_by_name = {
        gate_payload["gate"]: gate_payload
        for gate_payload in (
            input_gate,
            account_gate,
            manual_arming_gate,
            session_gate,
            canary_order_gate,
            affordability_gate,
            duplicate_gate,
            operator_state_ops_gate,
            other_guardrails_gate,
        )
    }
    gates = [gates_by_name[gate_name] for gate_name in READINESS_GATE_ORDER]

    blocking_reasons = _dedupe_preserve(
        [
            blocker
            for gate_payload in gates
            for blocker in gate_payload["blocking_reasons"]
        ]
    )
    warnings = _dedupe_preserve(
        [
            warning
            for gate_payload in gates
            for warning in gate_payload["warnings"]
        ]
    )

    verdict = (
        "ready"
        if evaluation is not None
        and evaluation.decision == "ready_live_submit"
        and not blocking_reasons
        else "not_ready"
    )
    next_actions = _build_next_actions(
        signal_json_file=resolved_signal_json_file,
        broker=broker,
        positions_file=resolved_positions_file,
        configured_account_id=configured_account_id,
        broker_account_id=None if evaluation is None else evaluation.broker_account_id,
        arm_live_canary=arm_value,
        ack_unmanaged_holdings=ack_unmanaged_holdings,
        base_dir=resolved_base_dir,
        secrets_file=secrets_file,
        blocking_reasons=blocking_reasons,
        duplicate_artifacts=duplicate_artifacts,
        operator_artifacts=operator_artifacts,
        verdict=verdict,
    )

    return {
        "schema_name": LIVE_CANARY_READINESS_SCHEMA_NAME,
        "schema_version": LIVE_CANARY_READINESS_SCHEMA_VERSION,
        "verdict": verdict,
        "timestamp_chicago": timestamp.isoformat(),
        "source": {
            "base_dir": None if resolved_base_dir is None else str(resolved_base_dir),
            "broker": broker,
            "positions_file": None if resolved_positions_file is None else str(resolved_positions_file),
            "signal_json_file": str(resolved_signal_json_file),
        },
        "scope": {
            "account_id": configured_account_id,
            "event_id": None if signal is None else signal.event_id,
            "signal_date": None if signal is None else signal.date,
            "strategy": None if signal is None else signal.strategy,
        },
        "summary": {
            "blocking_reason_count": len(blocking_reasons),
            "gate_count": len(gates),
            "pass_gate_count": sum(1 for gate in gates if gate["status"] == "pass"),
            "warning_count": len(warnings),
        },
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "next_actions": next_actions,
        "gates": gates,
        "signal": None
        if signal is None
        else {
            "action": signal.action,
            "event_id": signal.event_id,
            "price": signal.price,
            "raw": raw_signal,
            "signal_date": signal.date,
            "strategy": signal.strategy,
            "symbol": signal.symbol,
            "target_shares": signal.target_shares,
        },
        "evaluation": None
        if evaluation is None
        else {
            "account_id": evaluation.account_id,
            "affordability": rendered_affordability,
            "armed": evaluation.armed,
            "blockers": list(evaluation.blockers),
            "broker_account_id": evaluation.broker_account_id,
            "decision": evaluation.decision,
            "orders": rendered_orders,
            "warnings": list(evaluation.warnings),
        },
        "state_status": {
            "blocking_artifacts": []
            if state_status is None
            else list(state_status.get("blocking_artifacts", [])),
            "error": state_status_error,
            "summary": None if state_status is None else state_status.get("summary"),
        },
    }
