from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.data import LocalStore
from trading_codex.execution.models import BrokerPosition, BrokerSnapshot, SignalPayload
from trading_codex.execution.planner import build_execution_plan, execution_plan_to_dict
from trading_codex.execution.signals import parse_signal_payload
from trading_codex.run_archive import write_run_archive

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


DEFAULT_PAPER_STATE_KEY = "primary_live_candidate_v1"
DEFAULT_PAPER_STARTING_CASH = 10_000.0
DEFAULT_SIGNAL_BASELINE_CAPITAL = 10_000.0
SUPPORTED_PAPER_ACTIONS = frozenset({"HOLD", "ENTER", "EXIT", "ROTATE", "RESIZE"})
BUY_CLASSIFICATIONS = frozenset({"BUY", "RESIZE_BUY"})
SELL_CLASSIFICATIONS = frozenset({"SELL", "RESIZE_SELL", "EXIT"})
PAPER_LANE_STATE_SCHEMA_NAME = "paper_lane_state"
PAPER_LANE_STATE_SCHEMA_VERSION = 1
PAPER_LANE_LEDGER_SCHEMA_NAME = "paper_lane_ledger_entry"
PAPER_LANE_LEDGER_SCHEMA_VERSION = 1
PAPER_LANE_EVENT_RECEIPT_SCHEMA_NAME = "paper_lane_event_receipt"
PAPER_LANE_EVENT_RECEIPT_SCHEMA_VERSION = 1
PAPER_LANE_INIT_RESULT_SCHEMA_NAME = "paper_lane_init_result"
PAPER_LANE_INIT_RESULT_SCHEMA_VERSION = 1
PAPER_LANE_STATUS_SCHEMA_NAME = "paper_lane_status"
PAPER_LANE_STATUS_SCHEMA_VERSION = 1
PAPER_LANE_APPLY_RESULT_SCHEMA_NAME = "paper_lane_apply_result"
PAPER_LANE_APPLY_RESULT_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class PaperHolding:
    symbol: str
    shares: int
    last_price: float | None


@dataclass(frozen=True)
class PaperState:
    state_key: str
    strategy: str | None
    starting_cash: float
    cash: float
    holdings: dict[str, PaperHolding]
    last_applied_event_id: str | None
    created_at_chicago: str
    updated_at_chicago: str


@dataclass(frozen=True)
class PaperLanePaths:
    base_dir: Path
    state_path: Path
    ledger_path: Path
    event_receipts_dir: Path


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _safe_slug(value: str, *, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned.strip("._-") or fallback


def resolve_paper_lane_base_dir(
    *,
    state_key: str = DEFAULT_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    create: bool,
) -> Path:
    if base_dir is not None:
        path = Path(base_dir)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    from trading_codex.run_archive import resolve_archive_root

    archive_root = resolve_archive_root(create=create)
    path = archive_root / "paper_lane" / _safe_slug(state_key, fallback=DEFAULT_PAPER_STATE_KEY)
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_paper_lane_paths(
    *,
    state_key: str = DEFAULT_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    create: bool,
) -> PaperLanePaths:
    resolved_base = resolve_paper_lane_base_dir(state_key=state_key, base_dir=base_dir, create=create)
    if create:
        (resolved_base / "event_receipts").mkdir(parents=True, exist_ok=True)
    return PaperLanePaths(
        base_dir=resolved_base,
        state_path=resolved_base / "paper_state.json",
        ledger_path=resolved_base / "paper_ledger.jsonl",
        event_receipts_dir=resolved_base / "event_receipts",
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


def _append_jsonl_record(path: Path, *, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_directory(path.parent)


def _timestamp(value: str | None) -> datetime:
    if value is None:
        return _chicago_now()
    parsed = datetime.fromisoformat(value)
    if ZoneInfo is not None:
        chicago = ZoneInfo("America/Chicago")
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=chicago)
        return parsed.astimezone(chicago)
    return parsed


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _coerce_positive_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric.")
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError(f"{field_name} must not be empty.")
        try:
            number = float(stripped)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be numeric.") from exc
    else:
        raise ValueError(f"{field_name} must be numeric.")
    if number < 0.0:
        raise ValueError(f"{field_name} must be >= 0.")
    return number


def _coerce_positive_int_like(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer.")
    if isinstance(value, int):
        number = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field_name} must be a whole number.")
        number = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError(f"{field_name} must not be empty.")
        try:
            number = int(stripped)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an integer.") from exc
    else:
        raise ValueError(f"{field_name} must be an integer.")
    if number < 0:
        raise ValueError(f"{field_name} must be >= 0.")
    return number


def _snapshot_holdings(holdings: dict[str, PaperHolding]) -> list[dict[str, Any]]:
    return [
        {
            "last_price": None if item.last_price is None else round(float(item.last_price), 6),
            "shares": item.shares,
            "symbol": item.symbol,
        }
        for item in sorted(holdings.values(), key=lambda item: item.symbol)
    ]


def _state_to_dict(state: PaperState) -> dict[str, Any]:
    return {
        "cash": _round_money(state.cash),
        "created_at_chicago": state.created_at_chicago,
        "holdings": _snapshot_holdings(state.holdings),
        "last_applied_event_id": state.last_applied_event_id,
        "schema_name": PAPER_LANE_STATE_SCHEMA_NAME,
        "schema_version": PAPER_LANE_STATE_SCHEMA_VERSION,
        "starting_cash": _round_money(state.starting_cash),
        "state_key": state.state_key,
        "strategy": state.strategy,
        "updated_at_chicago": state.updated_at_chicago,
    }


def _state_from_dict(payload: dict[str, Any]) -> PaperState:
    if payload.get("schema_name") != PAPER_LANE_STATE_SCHEMA_NAME:
        raise ValueError(f"Paper lane state schema_name must be {PAPER_LANE_STATE_SCHEMA_NAME!r}.")

    raw_state_key = payload.get("state_key")
    if not isinstance(raw_state_key, str) or not raw_state_key.strip():
        raise ValueError("Paper lane state missing non-empty state_key.")
    state_key = raw_state_key.strip()

    strategy = payload.get("strategy")
    if strategy is not None:
        if not isinstance(strategy, str) or not strategy.strip():
            raise ValueError("Paper lane state strategy must be a non-empty string or null.")
        strategy = strategy.strip()

    holdings_payload = payload.get("holdings", [])
    if not isinstance(holdings_payload, list):
        raise ValueError("Paper lane state holdings must be a list.")

    holdings: dict[str, PaperHolding] = {}
    for raw_holding in holdings_payload:
        if not isinstance(raw_holding, dict):
            raise ValueError("Paper lane state holdings entries must be objects.")
        raw_symbol = raw_holding.get("symbol")
        if not isinstance(raw_symbol, str) or not raw_symbol.strip():
            raise ValueError("Paper lane holding symbol must be a non-empty string.")
        symbol = raw_symbol.strip().upper()
        shares = _coerce_positive_int_like(raw_holding.get("shares"), field_name=f"{symbol}.shares")
        if shares <= 0:
            raise ValueError("Paper lane holding shares must be > 0.")
        last_price_value = raw_holding.get("last_price")
        last_price = None if last_price_value is None else float(
            _coerce_positive_float(last_price_value, field_name=f"{symbol}.last_price")
        )
        holdings[symbol] = PaperHolding(symbol=symbol, shares=shares, last_price=last_price)

    if len(holdings) > 1:
        raise ValueError("Paper lane supports at most one active ETF holding.")

    raw_last_applied = payload.get("last_applied_event_id")
    if raw_last_applied is not None and (not isinstance(raw_last_applied, str) or not raw_last_applied.strip()):
        raise ValueError("Paper lane state last_applied_event_id must be a non-empty string or null.")

    created_at = payload.get("created_at_chicago")
    updated_at = payload.get("updated_at_chicago")
    if not isinstance(created_at, str) or not created_at.strip():
        raise ValueError("Paper lane state missing created_at_chicago.")
    if not isinstance(updated_at, str) or not updated_at.strip():
        raise ValueError("Paper lane state missing updated_at_chicago.")

    return PaperState(
        state_key=state_key,
        strategy=strategy,
        starting_cash=float(_coerce_positive_float(payload.get("starting_cash"), field_name="starting_cash")),
        cash=float(_coerce_positive_float(payload.get("cash"), field_name="cash")),
        holdings=holdings,
        last_applied_event_id=None if raw_last_applied is None else raw_last_applied.strip(),
        created_at_chicago=created_at.strip(),
        updated_at_chicago=updated_at.strip(),
    )


def load_paper_state(paths: PaperLanePaths) -> PaperState:
    if not paths.state_path.exists():
        raise FileNotFoundError(
            f"Paper lane state does not exist yet: {paths.state_path}. Run init first."
        )
    payload = json.loads(paths.state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Paper lane state file must contain a JSON object.")
    return _state_from_dict(payload)


def _write_paper_state(paths: PaperLanePaths, state: PaperState) -> None:
    _atomic_write_json(paths.state_path, _state_to_dict(state))


def _event_receipt_path(paths: PaperLanePaths, event_id: str) -> Path:
    digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
    return paths.event_receipts_dir / f"{digest}.json"


def event_already_applied(paths: PaperLanePaths, event_id: str) -> bool:
    return _event_receipt_path(paths, event_id).exists()


def _write_event_receipt(
    paths: PaperLanePaths,
    *,
    timestamp: datetime,
    state_key: str,
    signal: SignalPayload,
    paper_target_shares: int,
    holdings_after: dict[str, PaperHolding],
    cash_after: float,
) -> Path:
    receipt_path = _event_receipt_path(paths, signal.event_id)
    payload = {
        "applied_at_chicago": timestamp.isoformat(),
        "cash_after": _round_money(cash_after),
        "event_id": signal.event_id,
        "holdings_after": _snapshot_holdings(holdings_after),
        "paper_target_shares": paper_target_shares,
        "schema_name": PAPER_LANE_EVENT_RECEIPT_SCHEMA_NAME,
        "schema_version": PAPER_LANE_EVENT_RECEIPT_SCHEMA_VERSION,
        "signal_action": signal.action,
        "signal_date": signal.date,
        "signal_symbol": signal.symbol,
        "state_key": state_key,
        "strategy": signal.strategy,
    }
    _atomic_write_json(receipt_path, payload)
    return receipt_path


def _append_ledger_entry(paths: PaperLanePaths, *, entry: dict[str, Any]) -> None:
    _append_jsonl_record(paths.ledger_path, record=entry)


def _price_from_store(*, data_dir: Path | None, symbol: str, as_of_date: str) -> float | None:
    if data_dir is None:
        return None
    try:
        bars = LocalStore(base_dir=data_dir).read_bars(symbol, end=as_of_date)
    except (FileNotFoundError, ValueError):
        return None
    if bars.empty or "close" not in bars.columns:
        return None
    return float(bars["close"].iloc[-1])


def _resolve_holding_price(
    *,
    holding: PaperHolding,
    signal: SignalPayload,
    data_dir: Path | None,
) -> float | None:
    if holding.symbol == signal.symbol and signal.price is not None:
        return float(signal.price)
    store_price = _price_from_store(data_dir=data_dir, symbol=holding.symbol, as_of_date=signal.date)
    if store_price is not None:
        return store_price
    return None


def _paper_broker_snapshot(
    *,
    state: PaperState,
    signal: SignalPayload,
    data_dir: Path | None,
) -> tuple[BrokerSnapshot, float]:
    positions: dict[str, BrokerPosition] = {}
    holdings_value = 0.0
    for symbol, holding in sorted(state.holdings.items()):
        price = _resolve_holding_price(holding=holding, signal=signal, data_dir=data_dir)
        if price is None:
            raise ValueError(
                f"Paper lane cannot value existing holding {symbol} for signal date {signal.date} from a current "
                "approved pricing source; pass --data-dir with current bars for held symbols. "
                "Stored last_price state marks are not accepted for status/apply."
            )
        positions[symbol] = BrokerPosition(
            symbol=symbol,
            shares=holding.shares,
            price=price,
            instrument_type="Equity",
            underlying_symbol=symbol,
            raw={
                "source": "paper_lane_state",
                "last_price": holding.last_price,
                "resolved_price": price,
            },
        )
        if price is not None:
            holdings_value += holding.shares * price

    equity = float(state.cash) + holdings_value
    snapshot = BrokerSnapshot(
        broker_name="paper_local",
        account_id=state.state_key,
        as_of=signal.date,
        cash=float(state.cash),
        buying_power=float(equity),
        positions=positions,
        raw={
            "state_key": state.state_key,
            "holdings_count": len(state.holdings),
            "signal_date": signal.date,
        },
    )
    return snapshot, float(equity)


def _classification_side(classification: str) -> str | None:
    normalized = classification.upper()
    if normalized in BUY_CLASSIFICATIONS:
        return "BUY"
    if normalized in SELL_CLASSIFICATIONS:
        return "SELL"
    return None


def _validate_signal(signal: SignalPayload) -> None:
    action = signal.action.upper()
    if action not in SUPPORTED_PAPER_ACTIONS:
        raise ValueError(
            "Paper lane only supports HOLD / ENTER / EXIT / ROTATE / RESIZE next_action payloads."
        )
    if signal.target_shares < 0 or signal.desired_target_shares < 0:
        raise ValueError("Paper lane does not support negative share targets.")


def _build_status_payload(
    *,
    timestamp: datetime,
    paths: PaperLanePaths,
    state: PaperState,
    signal_raw: dict[str, Any],
    signal: SignalPayload,
    source_kind: str,
    source_label: str,
    source_ref: str | None,
    data_dir: Path | None,
) -> dict[str, Any]:
    _validate_signal(signal)
    if state.strategy is not None and state.strategy != signal.strategy:
        raise ValueError(
            f"Paper lane state is bound to strategy {state.strategy!r} and cannot consume {signal.strategy!r}."
        )

    broker_snapshot, equity = _paper_broker_snapshot(state=state, signal=signal, data_dir=data_dir)
    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker_snapshot,
        account_scope="full_account",
        managed_symbols=None,
        ack_unmanaged_holdings=False,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        broker_source_ref=f"paper_local:{state.state_key}",
        data_dir=data_dir,
        generated_at=timestamp,
        sizing_mode="account_capital",
        capital_input=equity,
        cap_to_buying_power=False,
        reserve_cash_pct=0.0,
        max_allocation_pct=1.0,
        baseline_signal_capital=DEFAULT_SIGNAL_BASELINE_CAPITAL,
    )

    desired_positions = {
        item.symbol: item.desired_target_shares
        for item in plan.items
        if item.desired_target_shares > 0
    }
    current_positions = {
        item.symbol: item.current_broker_shares
        for item in plan.items
        if item.current_broker_shares > 0
    }
    trades_required = []
    for item in plan.items:
        if item.delta_shares == 0:
            continue
        side = _classification_side(item.classification)
        trades_required.append(
            {
                "classification": item.classification,
                "current_broker_shares": item.current_broker_shares,
                "delta_shares": item.delta_shares,
                "desired_target_shares": item.desired_target_shares,
                "estimated_notional": item.estimated_notional,
                "quantity": abs(int(item.delta_shares)),
                "reference_price": item.reference_price,
                "side": side,
                "symbol": item.symbol,
            }
        )

    already_applied = event_already_applied(paths, signal.event_id)
    return {
        "current_positions": current_positions,
        "data_dir": None if data_dir is None else str(data_dir),
        "drift_present": bool(trades_required),
        "event_already_applied": already_applied,
        "execution_plan": execution_plan_to_dict(plan),
        "generated_at_chicago": timestamp.isoformat(),
        "paper_state": _state_to_dict(state),
        "paths": {
            "base_dir": str(paths.base_dir),
            "event_receipts_dir": str(paths.event_receipts_dir),
            "ledger_path": str(paths.ledger_path),
            "state_path": str(paths.state_path),
        },
        "raw_signal_target_shares": signal.target_shares,
        "scaled_target_positions": desired_positions,
        "schema_name": PAPER_LANE_STATUS_SCHEMA_NAME,
        "schema_version": PAPER_LANE_STATUS_SCHEMA_VERSION,
        "signal": dict(signal_raw),
        "source": {
            "kind": source_kind,
            "label": source_label,
            "ref": source_ref,
        },
        "state_aligned_to_target": not trades_required,
        "strategy_locked": state.strategy or signal.strategy,
        "trade_required": trades_required,
    }


def _render_positions_summary(positions: dict[str, int]) -> str:
    if not positions:
        return "CASH"
    parts = [f"{symbol} {shares}" for symbol, shares in sorted(positions.items())]
    return ", ".join(parts)


def render_paper_status_text(payload: dict[str, Any]) -> str:
    signal = payload["signal"]
    paper_state = payload["paper_state"]
    lines = [
        f"Paper lane {paper_state['state_key']}",
        f"Signal: {signal['date']} {signal['strategy']} {signal['action']} {signal['symbol']} event_id={signal['event_id']}",
        f"Latest event already applied: {'yes' if payload['event_already_applied'] else 'no'}",
        f"Cash: {paper_state['cash']:.2f}",
        f"Current: {_render_positions_summary(payload['current_positions'])}",
        (
            f"Target: {_render_positions_summary(payload['scaled_target_positions'])} "
            f"(raw signal target_shares={payload['raw_signal_target_shares']})"
        ),
        f"Drift present: {'yes' if payload['drift_present'] else 'no'}",
    ]
    trades = payload["trade_required"]
    if trades:
        lines.append("Trades needed:")
        for trade in trades:
            price = "-" if trade["reference_price"] is None else f"{float(trade['reference_price']):.2f}"
            notional = "-" if trade["estimated_notional"] is None else f"{float(trade['estimated_notional']):.2f}"
            lines.append(
                f"- {trade['side'] or trade['classification']} {trade['quantity']} {trade['symbol']} "
                f"@ {price} est={notional} [{trade['classification']}]"
            )
    else:
        lines.append("Trades needed: none")
    lines.append(f"State path: {payload['paths']['state_path']}")
    lines.append(f"Ledger path: {payload['paths']['ledger_path']}")
    return "\n".join(lines)


def build_paper_lane_status(
    *,
    state_key: str = DEFAULT_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    signal_raw: dict[str, Any],
    source_kind: str,
    source_label: str,
    source_ref: str | None = None,
    data_dir: Path | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paper_lane_paths(state_key=state_key, base_dir=base_dir, create=False)
    state = load_paper_state(paths)
    resolved_timestamp = _timestamp(timestamp)
    signal = parse_signal_payload(signal_raw)
    payload = _build_status_payload(
        timestamp=resolved_timestamp,
        paths=paths,
        state=state,
        signal_raw=signal_raw,
        signal=signal,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        data_dir=data_dir,
    )
    archive = write_run_archive(
        timestamp=resolved_timestamp,
        run_kind="paper_lane_status",
        mode="status",
        label=state.state_key,
        identity_parts=[signal.event_id, state.state_key, source_kind],
        manifest_fields={
            "action": signal.action,
            "event_already_applied": payload["event_already_applied"],
            "event_id": signal.event_id,
            "signal_date": signal.date,
            "state_key": state.state_key,
            "strategy": signal.strategy,
        },
        json_artifacts={
            "paper_status": payload,
            "paper_state": payload["paper_state"],
            "signal_payload": signal_raw,
        },
        text_artifacts={"summary_text": render_paper_status_text(payload)},
    )
    output = dict(payload)
    output["archive_manifest_path"] = str(archive.paths.manifest_path)
    return output


def initialize_paper_lane(
    *,
    state_key: str = DEFAULT_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    starting_cash: float = DEFAULT_PAPER_STARTING_CASH,
    timestamp: str | None = None,
    reset: bool = False,
) -> dict[str, Any]:
    if starting_cash <= 0.0:
        raise ValueError("starting_cash must be > 0.")
    paths = resolve_paper_lane_paths(state_key=state_key, base_dir=base_dir, create=True)
    if paths.state_path.exists() and not reset:
        raise ValueError(f"Paper lane state already exists: {paths.state_path}. Use --reset to reinitialize it.")

    resolved_timestamp = _timestamp(timestamp)
    if reset and paths.event_receipts_dir.exists():
        shutil.rmtree(paths.event_receipts_dir)
        paths.event_receipts_dir.mkdir(parents=True, exist_ok=True)

    state = PaperState(
        state_key=state_key,
        strategy=None,
        starting_cash=float(starting_cash),
        cash=float(starting_cash),
        holdings={},
        last_applied_event_id=None,
        created_at_chicago=resolved_timestamp.isoformat(),
        updated_at_chicago=resolved_timestamp.isoformat(),
    )
    _write_paper_state(paths, state)

    entry = {
        "cash_after": _round_money(state.cash),
        "cash_before": None,
        "entry_kind": "reset" if reset else "init",
        "event_id": None,
        "generated_at_chicago": resolved_timestamp.isoformat(),
        "holdings_after": [],
        "holdings_before": None,
        "schema_name": PAPER_LANE_LEDGER_SCHEMA_NAME,
        "schema_version": PAPER_LANE_LEDGER_SCHEMA_VERSION,
        "state_key": state.state_key,
        "strategy": None,
    }
    _append_ledger_entry(paths, entry=entry)

    payload = {
        "generated_at_chicago": resolved_timestamp.isoformat(),
        "paper_state": _state_to_dict(state),
        "paths": {
            "base_dir": str(paths.base_dir),
            "event_receipts_dir": str(paths.event_receipts_dir),
            "ledger_path": str(paths.ledger_path),
            "state_path": str(paths.state_path),
        },
        "reset": bool(reset),
        "schema_name": PAPER_LANE_INIT_RESULT_SCHEMA_NAME,
        "schema_version": PAPER_LANE_INIT_RESULT_SCHEMA_VERSION,
    }
    archive = write_run_archive(
        timestamp=resolved_timestamp,
        run_kind="paper_lane_init",
        mode="init",
        label=state.state_key,
        identity_parts=[state.state_key, "reset" if reset else "init", starting_cash],
        manifest_fields={
            "state_key": state.state_key,
            "strategy": None,
        },
        json_artifacts={"paper_init_result": payload, "paper_state": payload["paper_state"]},
        text_artifacts={
            "summary_text": (
                f"Initialized paper lane {state.state_key} with cash={state.cash:.2f}"
                + (" (reset)" if reset else "")
            )
        },
    )
    output = dict(payload)
    output["archive_manifest_path"] = str(archive.paths.manifest_path)
    return output


def render_paper_apply_text(payload: dict[str, Any]) -> str:
    result = payload["result"]
    before = payload["paper_state_before"]
    after = payload["paper_state_after"]
    lines = [
        f"Paper lane {after['state_key']}",
        f"Result: {result}",
        f"Event: {payload['signal']['event_id']}",
        f"Cash: {before['cash']:.2f} -> {after['cash']:.2f}",
        f"Before: {_render_positions_summary(payload['status_before_apply']['current_positions'])}",
        f"After: {_render_positions_summary(payload['paper_positions_after_apply'])}",
    ]
    fills = payload["fills"]
    if fills:
        lines.append("Fills:")
        for fill in fills:
            lines.append(
                f"- {fill['side']} {fill['quantity']} {fill['symbol']} @ {float(fill['price']):.2f} "
                f"notional={float(fill['notional']):.2f} [{fill['classification']}]"
            )
    else:
        lines.append("Fills: none")
    lines.append(f"State path: {payload['paths']['state_path']}")
    lines.append(f"Ledger path: {payload['paths']['ledger_path']}")
    return "\n".join(lines)


def apply_paper_lane_signal(
    *,
    state_key: str = DEFAULT_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    signal_raw: dict[str, Any],
    source_kind: str,
    source_label: str,
    source_ref: str | None = None,
    data_dir: Path | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paper_lane_paths(state_key=state_key, base_dir=base_dir, create=True)
    state_before = load_paper_state(paths)
    resolved_timestamp = _timestamp(timestamp)
    signal = parse_signal_payload(signal_raw)
    status_before = _build_status_payload(
        timestamp=resolved_timestamp,
        paths=paths,
        state=state_before,
        signal_raw=signal_raw,
        signal=signal,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        data_dir=data_dir,
    )

    if status_before["execution_plan"]["blockers"]:
        blockers = ", ".join(status_before["execution_plan"]["blockers"])
        raise ValueError(f"Paper lane apply refused because the reconciliation plan is blocked: {blockers}")

    duplicate = bool(status_before["event_already_applied"])
    fills: list[dict[str, Any]] = []
    holdings_after = dict(state_before.holdings)
    cash_after = float(state_before.cash)
    result = "duplicate_event_noop" if duplicate else "applied"

    if not duplicate:
        for item in status_before["execution_plan"]["items"]:
            classification = str(item["classification"])
            delta = int(item["delta_shares"])
            if delta >= 0 or classification not in SELL_CLASSIFICATIONS:
                continue
            symbol = str(item["symbol"])
            price = item["reference_price"]
            if price is None:
                raise ValueError(f"Paper lane cannot sell {symbol} without a reference price.")
            quantity = abs(delta)
            current = holdings_after.get(symbol)
            if current is None or current.shares < quantity:
                raise ValueError(f"Paper lane state cannot sell {quantity} shares of {symbol}; state is inconsistent.")
            notional = float(quantity) * float(price)
            cash_after += notional
            remaining = current.shares - quantity
            if remaining > 0:
                holdings_after[symbol] = PaperHolding(symbol=symbol, shares=remaining, last_price=float(price))
            else:
                holdings_after.pop(symbol, None)
            fills.append(
                {
                    "classification": classification,
                    "notional": _round_money(notional),
                    "price": round(float(price), 6),
                    "quantity": quantity,
                    "side": "SELL",
                    "symbol": symbol,
                }
            )

        for item in status_before["execution_plan"]["items"]:
            classification = str(item["classification"])
            delta = int(item["delta_shares"])
            if delta <= 0 or classification not in BUY_CLASSIFICATIONS:
                continue
            symbol = str(item["symbol"])
            price = item["reference_price"]
            if price is None:
                raise ValueError(f"Paper lane cannot buy {symbol} without a reference price.")
            quantity = abs(delta)
            notional = float(quantity) * float(price)
            if notional > cash_after + 0.01:
                raise ValueError(
                    f"Paper lane apply would overspend cash for {symbol}: need {notional:.2f}, have {cash_after:.2f}."
                )
            cash_after -= notional
            existing = holdings_after.get(symbol)
            new_shares = quantity if existing is None else existing.shares + quantity
            holdings_after[symbol] = PaperHolding(symbol=symbol, shares=new_shares, last_price=float(price))
            fills.append(
                {
                    "classification": classification,
                    "notional": _round_money(notional),
                    "price": round(float(price), 6),
                    "quantity": quantity,
                    "side": "BUY",
                    "symbol": symbol,
                }
            )

        if signal.symbol != "CASH" and signal.symbol in holdings_after and signal.price is not None:
            current = holdings_after[signal.symbol]
            holdings_after[signal.symbol] = PaperHolding(
                symbol=current.symbol,
                shares=current.shares,
                last_price=float(signal.price),
            )

        state_after = PaperState(
            state_key=state_before.state_key,
            strategy=state_before.strategy or signal.strategy,
            starting_cash=state_before.starting_cash,
            cash=_round_money(cash_after),
            holdings=holdings_after,
            last_applied_event_id=signal.event_id,
            created_at_chicago=state_before.created_at_chicago,
            updated_at_chicago=resolved_timestamp.isoformat(),
        )
        _write_paper_state(paths, state_after)
        paper_target_shares = 0
        if status_before["scaled_target_positions"]:
            _, paper_target_shares = next(iter(sorted(status_before["scaled_target_positions"].items())))
        receipt_path = _write_event_receipt(
            paths,
            timestamp=resolved_timestamp,
            state_key=state_after.state_key,
            signal=signal,
            paper_target_shares=paper_target_shares,
            holdings_after=holdings_after,
            cash_after=state_after.cash,
        )
        ledger_entry = {
            "cash_after": _round_money(state_after.cash),
            "cash_before": _round_money(state_before.cash),
            "entry_kind": "apply",
            "event_id": signal.event_id,
            "fills": fills,
            "generated_at_chicago": resolved_timestamp.isoformat(),
            "holdings_after": _snapshot_holdings(state_after.holdings),
            "holdings_before": _snapshot_holdings(state_before.holdings),
            "result": result,
            "schema_name": PAPER_LANE_LEDGER_SCHEMA_NAME,
            "schema_version": PAPER_LANE_LEDGER_SCHEMA_VERSION,
            "signal_action": signal.action,
            "signal_date": signal.date,
            "signal_symbol": signal.symbol,
            "state_key": state_after.state_key,
            "strategy": signal.strategy,
        }
        _append_ledger_entry(paths, entry=ledger_entry)
        paper_state_after = _state_to_dict(state_after)
    else:
        receipt_path = _event_receipt_path(paths, signal.event_id)
        paper_state_after = _state_to_dict(state_before)
        ledger_entry = {
            "cash_after": _round_money(state_before.cash),
            "cash_before": _round_money(state_before.cash),
            "entry_kind": "duplicate_refused",
            "event_id": signal.event_id,
            "fills": [],
            "generated_at_chicago": resolved_timestamp.isoformat(),
            "holdings_after": _snapshot_holdings(state_before.holdings),
            "holdings_before": _snapshot_holdings(state_before.holdings),
            "result": result,
            "schema_name": PAPER_LANE_LEDGER_SCHEMA_NAME,
            "schema_version": PAPER_LANE_LEDGER_SCHEMA_VERSION,
            "signal_action": signal.action,
            "signal_date": signal.date,
            "signal_symbol": signal.symbol,
            "state_key": state_before.state_key,
            "strategy": signal.strategy,
        }
        _append_ledger_entry(paths, entry=ledger_entry)

    payload = {
        "archive_manifest_path": None,
        "duplicate_event_blocked": duplicate,
        "event_receipt_path": str(receipt_path),
        "fills": fills,
        "generated_at_chicago": resolved_timestamp.isoformat(),
        "paper_state_after": paper_state_after,
        "paper_state_before": _state_to_dict(state_before),
        "paper_positions_after_apply": {
            holding["symbol"]: int(holding["shares"])
            for holding in paper_state_after["holdings"]
        },
        "paper_target_positions_after_apply": dict(status_before["scaled_target_positions"]),
        "paths": {
            "base_dir": str(paths.base_dir),
            "event_receipts_dir": str(paths.event_receipts_dir),
            "ledger_path": str(paths.ledger_path),
            "state_path": str(paths.state_path),
        },
        "result": result,
        "schema_name": PAPER_LANE_APPLY_RESULT_SCHEMA_NAME,
        "schema_version": PAPER_LANE_APPLY_RESULT_SCHEMA_VERSION,
        "signal": dict(signal_raw),
        "source": {
            "kind": source_kind,
            "label": source_label,
            "ref": source_ref,
        },
        "status_before_apply": status_before,
    }
    archive = write_run_archive(
        timestamp=resolved_timestamp,
        run_kind="paper_lane_apply",
        mode="apply",
        label=state_before.state_key,
        identity_parts=[signal.event_id, state_before.state_key, result],
        manifest_fields={
            "action": signal.action,
            "duplicate_event_blocked": duplicate,
            "event_id": signal.event_id,
            "result": result,
            "signal_date": signal.date,
            "state_key": state_before.state_key,
            "strategy": signal.strategy,
        },
        json_artifacts={
            "paper_apply_result": payload,
            "paper_state_after": paper_state_after,
            "paper_state_before": _state_to_dict(state_before),
            "paper_status_before": status_before,
            "signal_payload": signal_raw,
        },
        text_artifacts={"summary_text": render_paper_apply_text(payload)},
    )
    output = dict(payload)
    output["archive_manifest_path"] = str(archive.paths.manifest_path)
    return output
