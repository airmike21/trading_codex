from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

from trading_codex.execution.models import (
    ExecutionPlan,
    LiveSubmissionExport,
    SignalPayload,
    SimulatedOrderRequest,
    SimulatedSubmissionExport,
)
from trading_codex.execution.planner import (
    SIMULATED_INSTRUMENT_TYPE,
    SIMULATED_ORDER_TYPE,
    SIMULATED_TIME_IN_FORCE,
    plan_sha256_for_preview,
)
from trading_codex.run_archive import resolve_archive_root

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS = ("BIL", "EFA", "IWM", "QQQ", "SPY")
DEFAULT_LIVE_CANARY_MAX_LONG_SHARES = 1
LIVE_CANARY_SUPPORTED_ENTER_ACTIONS = {"BUY", "ENTER", "RESIZE", "ROTATE"}
LIVE_CANARY_SUPPORTED_EXIT_ACTIONS = {"EXIT", "SELL"}
LIVE_CANARY_NOOP_ACTIONS = {"HOLD"}
LIVE_CANARY_STATE_PENDING = "claim_pending_manual_clearance_required"
LIVE_CANARY_SUBMISSION_CAP_BLOCKER = "live_canary_existing_position_exceeds_cap"


@dataclass(frozen=True)
class LiveCanaryOrder:
    symbol: str
    side: str
    requested_qty: int
    executable_qty: int
    current_broker_shares: int
    desired_signal_shares: int
    desired_canary_shares: int
    classification: str
    reference_price: float | None
    estimated_notional: float | None
    cap_applied: bool


@dataclass(frozen=True)
class LiveCanaryEvaluation:
    timestamp_chicago: str
    account_id: str | None
    broker_account_id: str | None
    signal: SignalPayload
    live_submit_requested: bool
    armed: bool
    decision: str
    blockers: list[str]
    warnings: list[str]
    orders: list[LiveCanaryOrder]

    @property
    def duplicate(self) -> bool:
        return False


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _dedupe_strings(items: list[str]) -> list[str]:
    return sorted({item for item in items if item})


def _coerce_optional_non_empty_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected a string or None.")
    stripped = value.strip()
    return stripped or None


def normalize_live_canary_account(value: object) -> str | None:
    return _coerce_optional_non_empty_string(value)


def live_canary_state_dir(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        path = Path(base_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path
    path = resolve_archive_root(create=True) / "live_canary"
    path.mkdir(parents=True, exist_ok=True)
    return path


def live_canary_audit_path(base_dir: Path | None = None) -> Path:
    return live_canary_state_dir(base_dir) / "audit.jsonl"


def _live_canary_event_key(*, account_id: str, event_id: str) -> str:
    payload = json.dumps(
        {
            "account_id": account_id,
            "event_id": event_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def live_canary_event_state_path(*, base_dir: Path | None = None, account_id: str, event_id: str) -> Path:
    state_dir = live_canary_state_dir(base_dir)
    return state_dir / "events" / f"{_live_canary_event_key(account_id=account_id, event_id=event_id)}.json"


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
            fh.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8").strip()
    if raw == "":
        raise ValueError(f"Live canary state file {path} is empty.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Live canary state file {path} is malformed: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Live canary state file {path} must be a JSON object.")
    return payload


def _live_canary_lock_path(base_dir: Path | None = None) -> Path:
    return live_canary_state_dir(base_dir) / "live_canary.lock"


@contextmanager
def live_canary_state_lock(base_dir: Path | None = None):
    if fcntl is None:  # pragma: no cover
        yield
        return
    lock_path = _live_canary_lock_path(base_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def claim_live_canary_event(
    *,
    base_dir: Path | None,
    account_id: str,
    event_id: str,
    record: dict[str, Any],
) -> tuple[bool, dict[str, Any] | None, Path]:
    state_path = live_canary_event_state_path(base_dir=base_dir, account_id=account_id, event_id=event_id)
    lock_context = live_canary_state_lock(base_dir) if base_dir is not None or fcntl is not None else nullcontext()
    with lock_context:
        existing = _read_json_file(state_path)
        if existing is not None:
            return False, existing, state_path
        state_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(state_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            _fsync_directory(state_path.parent)
        except Exception:
            try:
                state_path.unlink()
            except FileNotFoundError:
                pass
            raise
    return True, None, state_path


def finalize_live_canary_event(
    *,
    state_path: Path,
    record: dict[str, Any],
) -> None:
    _atomic_write_json(state_path, record)


def _classify_order(
    *,
    signal: SignalPayload,
    current_shares: int,
    desired_shares: int,
) -> str:
    if desired_shares == current_shares:
        return "HOLD"
    if desired_shares <= 0 and current_shares > 0:
        if signal.symbol.upper() == "CASH" or signal.action.upper() in LIVE_CANARY_SUPPORTED_EXIT_ACTIONS:
            return "EXIT"
        return "SELL"
    if desired_shares > 0 and current_shares <= 0:
        return "BUY"
    if desired_shares > current_shares:
        return "RESIZE_BUY"
    return "RESIZE_SELL"


def _action_support_mode(signal: SignalPayload, *, allowed_symbols: set[str]) -> tuple[str, list[str]]:
    symbol = signal.symbol.upper()
    action = signal.action.upper()
    blockers: list[str] = []

    if action in LIVE_CANARY_NOOP_ACTIONS:
        if symbol == "CASH":
            return "noop_cash", blockers
        if symbol not in allowed_symbols:
            blockers.append(f"live_canary_symbol_not_allowed:{symbol}")
            return "blocked", blockers
        return "noop_hold", blockers

    if symbol == "CASH":
        return "cash_exit", blockers

    if symbol not in allowed_symbols:
        blockers.append(f"live_canary_symbol_not_allowed:{symbol}")
        return "blocked", blockers

    if action in LIVE_CANARY_SUPPORTED_EXIT_ACTIONS:
        return "cash_exit", blockers

    if action in LIVE_CANARY_SUPPORTED_ENTER_ACTIONS and signal.desired_target_shares > 0:
        return "target_long", blockers

    blockers.append(f"live_canary_unsupported_action:{signal.action}")
    return "blocked", blockers


def evaluate_live_canary(
    *,
    plan: ExecutionPlan,
    live_canary_account: str | None,
    live_submit_requested: bool,
    arm_live_canary: str | None,
    allowed_symbols: set[str] | None = None,
    max_long_shares: int = DEFAULT_LIVE_CANARY_MAX_LONG_SHARES,
    timestamp: datetime | None = None,
) -> LiveCanaryEvaluation:
    if max_long_shares <= 0:
        raise ValueError("max_long_shares must be > 0.")

    resolved_allowed_symbols = {
        symbol.strip().upper()
        for symbol in (allowed_symbols or set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS))
        if symbol.strip()
    }
    if not resolved_allowed_symbols:
        raise ValueError("allowed_symbols must not be empty.")

    signal = plan.signal
    account_id = normalize_live_canary_account(live_canary_account)
    broker_account_id = normalize_live_canary_account(plan.broker_snapshot.account_id)
    arm_value = normalize_live_canary_account(arm_live_canary)
    blockers: list[str] = list(plan.blockers)
    warnings: list[str] = list(plan.warnings)
    orders: list[LiveCanaryOrder] = []

    if account_id is None:
        blockers.append("live_canary_requires_account_binding")
    if broker_account_id is None:
        blockers.append("live_canary_broker_snapshot_missing_account")
    elif account_id is not None and broker_account_id != account_id:
        blockers.append("live_canary_account_binding_mismatch")

    armed = bool(live_submit_requested and account_id is not None and arm_value == account_id)
    if live_submit_requested and not armed:
        blockers.append("live_canary_not_armed")

    mode, action_blockers = _action_support_mode(signal, allowed_symbols=resolved_allowed_symbols)
    blockers.extend(action_blockers)

    if mode in {"target_long", "cash_exit"}:
        for item in plan.items:
            if item.current_broker_shares > max_long_shares:
                blockers.append(
                    f"{LIVE_CANARY_SUBMISSION_CAP_BLOCKER}:{item.symbol.upper()}:"
                    f"{item.current_broker_shares}:{max_long_shares}"
                )

    if not blockers and mode in {"target_long", "cash_exit"}:
        desired_positions = {}
        if mode == "target_long":
            desired_positions[signal.symbol.upper()] = max_long_shares

        item_by_symbol = {item.symbol.upper(): item for item in plan.items}
        symbols = sorted(
            set(desired_positions)
            | {item.symbol.upper() for item in plan.items if item.current_broker_shares != 0}
        )
        for symbol in symbols:
            item = item_by_symbol.get(symbol)
            current_shares = 0 if item is None else item.current_broker_shares
            desired_canary_shares = desired_positions.get(symbol, 0)
            executable_delta = desired_canary_shares - current_shares
            if executable_delta == 0:
                continue
            requested_qty = abs(executable_delta) if item is None else abs(item.delta_shares)
            classification = _classify_order(
                signal=signal,
                current_shares=current_shares,
                desired_shares=desired_canary_shares,
            )
            reference_price = None if item is None else item.reference_price
            if reference_price is None:
                blockers.append(f"live_canary_missing_reference_price:{symbol}")
                continue
            executable_qty = abs(executable_delta)
            cap_applied = item is not None and executable_qty < abs(item.delta_shares)
            estimated_notional = round(reference_price * executable_qty, 2)
            if cap_applied:
                warnings.append(f"live_canary_qty_capped:{symbol}:{requested_qty}:{executable_qty}")
            orders.append(
                LiveCanaryOrder(
                    symbol=symbol,
                    side="BUY" if executable_delta > 0 else "SELL",
                    requested_qty=requested_qty,
                    executable_qty=executable_qty,
                    current_broker_shares=current_shares,
                    desired_signal_shares=signal.desired_target_shares if symbol == signal.symbol.upper() else 0,
                    desired_canary_shares=desired_canary_shares,
                    classification=classification,
                    reference_price=reference_price,
                    estimated_notional=estimated_notional,
                    cap_applied=cap_applied,
                )
            )

    blockers = _dedupe_strings(blockers)
    warnings = _dedupe_strings(warnings)
    if blockers:
        decision = "blocked"
    elif mode == "noop_hold":
        decision = "noop_hold"
    elif mode == "noop_cash":
        decision = "noop_cash"
    elif not orders:
        decision = "noop"
    elif live_submit_requested:
        decision = "ready_live_submit"
    else:
        decision = "dry_run_ready"

    return LiveCanaryEvaluation(
        timestamp_chicago=(timestamp or _chicago_now()).isoformat(),
        account_id=account_id,
        broker_account_id=broker_account_id,
        signal=signal,
        live_submit_requested=live_submit_requested,
        armed=armed,
        decision=decision,
        blockers=blockers,
        warnings=warnings,
        orders=orders,
    )


def build_live_canary_submission_export(
    *,
    plan: ExecutionPlan,
    evaluation: LiveCanaryEvaluation,
) -> SimulatedSubmissionExport:
    if evaluation.account_id is None:
        raise ValueError("evaluation.account_id is required.")
    preview_orders = sorted(
        [
            {
                "order_type": SIMULATED_ORDER_TYPE,
                "qty": order.executable_qty,
                "side": order.side,
                "symbol": order.symbol,
                "tif": SIMULATED_TIME_IN_FORCE,
            }
            for order in evaluation.orders
        ],
        key=lambda item: (item["symbol"], item["side"], item["qty"]),
    )
    preview = {
        "account_scope": plan.account_scope,
        "allowed_symbols": sorted(plan.managed_symbols_universe),
        "broker": plan.broker_snapshot.broker_name,
        "broker_account_id": evaluation.account_id,
        "candidate_orders": preview_orders,
        "event_id": plan.signal.event_id,
        "guardrail": "live_canary_v1",
        "max_long_shares": DEFAULT_LIVE_CANARY_MAX_LONG_SHARES,
        "signal_action": plan.signal.action,
        "signal_symbol": plan.signal.symbol,
        "strategy": plan.signal.strategy,
    }
    plan_sha256 = plan_sha256_for_preview(preview)
    orders = [
        SimulatedOrderRequest(
            account_id=evaluation.account_id,
            broker_name=plan.broker_snapshot.broker_name,
            symbol=order.symbol,
            side=order.side,
            quantity=order.executable_qty,
            instrument_type=SIMULATED_INSTRUMENT_TYPE,
            order_type=SIMULATED_ORDER_TYPE,
            time_in_force=SIMULATED_TIME_IN_FORCE,
            strategy=plan.signal.strategy,
            event_id=plan.signal.event_id,
            reference_price=order.reference_price,
            estimated_notional=order.estimated_notional,
            classification=order.classification,
            blockers=[],
            warnings=list(evaluation.warnings),
        )
        for order in evaluation.orders
    ]
    return SimulatedSubmissionExport(
        generated_at_chicago=evaluation.timestamp_chicago,
        dry_run=not evaluation.live_submit_requested,
        source_kind=plan.source_kind,
        source_label=plan.source_label,
        source_ref=plan.source_ref,
        broker_name=plan.broker_snapshot.broker_name,
        account_id=evaluation.account_id,
        broker_source_ref=plan.broker_source_ref,
        account_scope=plan.account_scope,
        plan_math_scope=plan.plan_math_scope,
        sizing=plan.sizing,
        managed_symbols_universe=list(plan.managed_symbols_universe),
        blockers=[],
        warnings=list(evaluation.warnings),
        unmanaged_holdings_acknowledged=plan.unmanaged_holdings_acknowledged,
        unmanaged_positions_count=len(plan.unmanaged_positions),
        unmanaged_positions_summary=list(plan.unmanaged_positions),
        plan_preview=preview,
        plan_sha256=plan_sha256,
        orders=orders,
    )


def audit_rows_for_result(
    *,
    evaluation: LiveCanaryEvaluation,
    decision: str,
    duplicate: bool,
    response_text: str,
) -> list[dict[str, Any]]:
    base = {
        "account": evaluation.account_id,
        "action": evaluation.signal.action,
        "armed": evaluation.armed,
        "decision": decision,
        "duplicate": duplicate,
        "event_id": evaluation.signal.event_id,
        "response_text": response_text,
        "ts_chicago": evaluation.timestamp_chicago,
    }
    if not evaluation.orders:
        return [
            {
                **base,
                "classification": None,
                "current_broker_shares": None,
                "desired_canary_shares": None,
                "desired_signal_shares": evaluation.signal.desired_target_shares,
                "executable_qty": 0,
                "reference_price": evaluation.signal.price,
                "requested_qty": 0,
                "side": None,
                "symbol": evaluation.signal.symbol,
            }
        ]
    return [
        {
            **base,
            "classification": order.classification,
            "current_broker_shares": order.current_broker_shares,
            "desired_canary_shares": order.desired_canary_shares,
            "desired_signal_shares": order.desired_signal_shares,
            "executable_qty": order.executable_qty,
            "reference_price": order.reference_price,
            "requested_qty": order.requested_qty,
            "side": order.side,
            "symbol": order.symbol,
        }
        for order in evaluation.orders
    ]


def append_live_canary_audit(
    *,
    audit_path: Path,
    rows: list[dict[str, Any]],
) -> None:
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_directory(audit_path.parent)


def live_canary_live_submit_limits(export: SimulatedSubmissionExport) -> tuple[float, int]:
    if not export.orders:
        return 1.0, DEFAULT_LIVE_CANARY_MAX_LONG_SHARES
    max_notional = max((order.estimated_notional or 0.0) for order in export.orders)
    max_qty = max(order.quantity for order in export.orders)
    return max_notional if max_notional > 0 else 1.0, max_qty if max_qty > 0 else 1


def response_text_from_live_submission(export: LiveSubmissionExport) -> str:
    if export.refusal_reasons:
        return "; ".join(export.refusal_reasons)
    failed_orders = [order for order in export.orders if not order.succeeded]
    if failed_orders:
        return "; ".join(
            f"{order.symbol} {order.side} {order.quantity}: {order.error or 'submission failed'}"
            for order in failed_orders
        )
    return export.submission_result or "submitted"
