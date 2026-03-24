from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
import os
from datetime import date, datetime, time, timedelta
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
DEFAULT_LIVE_CANARY_BROKER_SNAPSHOT_MAX_AGE = timedelta(minutes=15)
LIVE_CANARY_SUPPORTED_ENTER_ACTIONS = {"BUY", "ENTER", "RESIZE", "ROTATE"}
LIVE_CANARY_SUPPORTED_EXIT_ACTIONS = {"EXIT", "SELL"}
LIVE_CANARY_NOOP_ACTIONS = {"HOLD"}
LIVE_CANARY_STATE_PENDING = "claim_pending_manual_clearance_required"
LIVE_CANARY_SUBMISSION_CAP_BLOCKER = "live_canary_existing_position_exceeds_cap"
LIVE_CANARY_REGULAR_SESSION_BLOCKER = "live_canary_submit_outside_regular_session"
LIVE_CANARY_MARKET_HOLIDAY_BLOCKER_PREFIX = "live_canary_submit_market_holiday"
LIVE_CANARY_SIGNAL_DATE_MISMATCH_PREFIX = "live_canary_signal_date_mismatch"
LIVE_CANARY_SIGNAL_DATE_UNPARSEABLE = "live_canary_signal_date_unparseable"
LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_MISSING = "live_canary_broker_snapshot_as_of_missing"
LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_UNPARSEABLE = "live_canary_broker_snapshot_as_of_unparseable"
LIVE_CANARY_REGULAR_SESSION_OPEN = time(hour=9, minute=30, second=0)
LIVE_CANARY_REGULAR_SESSION_CLOSE = time(hour=16, minute=0, second=0)
LIVE_CANARY_EARLY_SESSION_CLOSE = time(hour=13, minute=0, second=0)


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


def _normalize_timestamp(timestamp: datetime | None) -> datetime:
    resolved = timestamp or _chicago_now()
    if ZoneInfo is not None and resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=ZoneInfo("America/Chicago"))
    return resolved.replace(microsecond=0)


def _to_new_york_time(timestamp: datetime) -> datetime:
    if ZoneInfo is None:
        return timestamp
    return timestamp.astimezone(ZoneInfo("America/New_York"))


def _parse_iso_timestamp(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if ZoneInfo is not None and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("America/Chicago"))
    return parsed


def _observed_fixed_market_holiday(holiday: date) -> date:
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> date:
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    return first_day + timedelta(days=offset + (occurrence - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


@lru_cache(maxsize=None)
def _nyse_market_holidays(year: int) -> frozenset[date]:
    holidays: set[date] = set()

    new_year = date(year, 1, 1)
    if new_year.weekday() < 5:
        holidays.add(new_year)
    elif new_year.weekday() == 6:
        holidays.add(date(year, 1, 2))

    if year >= 1998:
        holidays.add(_nth_weekday_of_month(year, 1, 0, 3))
    holidays.add(_nth_weekday_of_month(year, 2, 0, 3))
    holidays.add(_easter_sunday(year) - timedelta(days=2))
    holidays.add(_last_weekday_of_month(year, 5, 0))
    if year >= 2022:
        holidays.add(_observed_fixed_market_holiday(date(year, 6, 19)))
    holidays.add(_observed_fixed_market_holiday(date(year, 7, 4)))
    holidays.add(_nth_weekday_of_month(year, 9, 0, 1))
    holidays.add(_nth_weekday_of_month(year, 11, 3, 4))
    holidays.add(_observed_fixed_market_holiday(date(year, 12, 25)))
    if date(year + 1, 1, 1).weekday() == 5:
        holidays.add(date(year, 12, 31))

    return frozenset(holidays)


def _is_new_york_market_holiday(day: date) -> bool:
    return day in _nyse_market_holidays(day.year)


@lru_cache(maxsize=None)
def _nyse_early_close_days(year: int) -> frozenset[date]:
    early_closes: set[date] = set()

    thanksgiving = _nth_weekday_of_month(year, 11, 3, 4)
    black_friday = thanksgiving + timedelta(days=1)
    if _is_new_york_regular_trading_day(black_friday):
        early_closes.add(black_friday)

    independence_eve = _prior_trading_weekday(_observed_fixed_market_holiday(date(year, 7, 4)))
    if independence_eve.year == year:
        early_closes.add(independence_eve)

    christmas_eve = date(year, 12, 24)
    if _is_new_york_regular_trading_day(christmas_eve):
        early_closes.add(christmas_eve)

    return frozenset(early_closes)


def _new_york_regular_session_close(day: date) -> time:
    if day in _nyse_early_close_days(day.year):
        return LIVE_CANARY_EARLY_SESSION_CLOSE
    return LIVE_CANARY_REGULAR_SESSION_CLOSE


def _is_new_york_regular_trading_day(day: date) -> bool:
    return day.weekday() < 5 and not _is_new_york_market_holiday(day)


def _is_new_york_regular_session_open(timestamp_new_york: datetime) -> bool:
    if not _is_new_york_regular_trading_day(timestamp_new_york.date()):
        return False
    current_time = timestamp_new_york.timetz().replace(tzinfo=None)
    return LIVE_CANARY_REGULAR_SESSION_OPEN <= current_time <= _new_york_regular_session_close(
        timestamp_new_york.date()
    )


def _prior_trading_weekday(day: date) -> date:
    current = day - timedelta(days=1)
    while not _is_new_york_regular_trading_day(current):
        current -= timedelta(days=1)
    return current


def _latest_completed_regular_session_date(timestamp_new_york: datetime) -> date:
    if not _is_new_york_regular_trading_day(timestamp_new_york.date()):
        return _prior_trading_weekday(timestamp_new_york.date())
    current_time = timestamp_new_york.timetz().replace(tzinfo=None)
    if current_time > _new_york_regular_session_close(timestamp_new_york.date()):
        return timestamp_new_york.date()
    return _prior_trading_weekday(timestamp_new_york.date())


def _live_submit_readiness_messages(
    *,
    plan: ExecutionPlan,
    timestamp: datetime,
    broker_snapshot_max_age: timedelta,
) -> list[str]:
    messages: list[str] = []
    timestamp_new_york = _to_new_york_time(timestamp)
    session_day = timestamp_new_york.date()

    if _is_new_york_market_holiday(session_day):
        messages.append(f"{LIVE_CANARY_MARKET_HOLIDAY_BLOCKER_PREFIX}:{session_day.isoformat()}")
    elif not _is_new_york_regular_session_open(timestamp_new_york):
        messages.append(LIVE_CANARY_REGULAR_SESSION_BLOCKER)

    expected_signal_date = _latest_completed_regular_session_date(timestamp_new_york)
    try:
        signal_date = date.fromisoformat(plan.signal.date)
    except ValueError:
        messages.append(LIVE_CANARY_SIGNAL_DATE_UNPARSEABLE)
    else:
        if signal_date != expected_signal_date:
            messages.append(
                f"{LIVE_CANARY_SIGNAL_DATE_MISMATCH_PREFIX}:"
                f"{signal_date.isoformat()}:{expected_signal_date.isoformat()}"
            )

    broker_as_of = plan.broker_snapshot.as_of
    if broker_as_of is None or broker_as_of.strip() == "":
        messages.append(LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_MISSING)
        return messages

    try:
        broker_as_of_dt = _parse_iso_timestamp(broker_as_of)
    except ValueError:
        messages.append(LIVE_CANARY_BROKER_SNAPSHOT_AS_OF_UNPARSEABLE)
        return messages

    broker_snapshot_age = timestamp - broker_as_of_dt.astimezone(timestamp.tzinfo) if timestamp.tzinfo else timestamp - broker_as_of_dt
    if broker_snapshot_age > broker_snapshot_max_age:
        messages.append(
            "live_canary_broker_snapshot_stale:"
            f"{int(broker_snapshot_age.total_seconds())}:"
            f"{int(broker_snapshot_max_age.total_seconds())}"
        )
    return messages


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
    broker_snapshot_max_age: timedelta = DEFAULT_LIVE_CANARY_BROKER_SNAPSHOT_MAX_AGE,
) -> LiveCanaryEvaluation:
    if max_long_shares <= 0:
        raise ValueError("max_long_shares must be > 0.")
    if broker_snapshot_max_age <= timedelta(0):
        raise ValueError("broker_snapshot_max_age must be > 0.")

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
    resolved_timestamp = _normalize_timestamp(timestamp)

    if account_id is None:
        blockers.append("live_canary_requires_account_binding")
    if broker_account_id is None:
        blockers.append("live_canary_broker_snapshot_missing_account")
    elif account_id is not None and broker_account_id != account_id:
        blockers.append("live_canary_account_binding_mismatch")

    armed = bool(live_submit_requested and account_id is not None and arm_value == account_id)
    if live_submit_requested and not armed:
        blockers.append("live_canary_not_armed")

    readiness_messages = _live_submit_readiness_messages(
        plan=plan,
        timestamp=resolved_timestamp,
        broker_snapshot_max_age=broker_snapshot_max_age,
    )
    if live_submit_requested:
        blockers.extend(readiness_messages)
    else:
        warnings.extend(readiness_messages)

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
        timestamp_chicago=resolved_timestamp.isoformat(),
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
    live_submission: dict[str, Any] | None = None,
    pre_submit_reconciliation: dict[str, Any] | None = None,
    submit_error: dict[str, Any] | None = None,
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
    if live_submission is not None:
        base["live_submission"] = live_submission
    if pre_submit_reconciliation is not None:
        base["pre_submit_reconciliation"] = pre_submit_reconciliation
    if submit_error is not None:
        base["submit_error"] = submit_error
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
