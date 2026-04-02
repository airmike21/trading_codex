from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from trading_codex.execution.models import BrokerPosition, BrokerSnapshot, ExecutionPlan, PlanItem
from trading_codex.execution.planner import build_execution_plan, execution_plan_to_dict
from trading_codex.execution.signals import parse_signal_payload

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


DEFAULT_IBKR_SHADOW_HOST = "172.26.192.1"
DEFAULT_IBKR_SHADOW_PORT = 7497
DEFAULT_IBKR_SHADOW_CLIENT_ID = 7601
DEFAULT_IBKR_SHADOW_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_IBKR_SHADOW_READ_TIMEOUT_SECONDS = 10.0
DEFAULT_SIGNAL_BASELINE_CAPITAL = 10_000.0

IBKR_SHADOW_SCHEMA_NAME = "ibkr_paper_shadow_execution"
IBKR_SHADOW_SCHEMA_VERSION = 1

SUPPORTED_SIGNAL_ACTIONS = frozenset({"HOLD", "ENTER", "EXIT", "ROTATE", "RESIZE"})
SUPPORTED_IBKR_SEC_TYPES = frozenset({"STK"})
ORDER_SIDE_BY_CLASSIFICATION = {
    "BUY": "BUY",
    "RESIZE_BUY": "BUY",
    "SELL": "SELL",
    "RESIZE_SELL": "SELL",
    "EXIT": "SELL",
}
NON_FATAL_IBKR_ERROR_CODES = frozenset(
    {
        2104,  # Market data farm connection is OK.
        2106,  # HMDS data farm connection is OK.
        2107,  # HMDS data farm inactive.
        2108,  # Market data farm connection inactive.
        2109,  # Order event warning; no orders are submitted here.
        2158,  # Sec-def farm connection is OK.
        1102,  # Connectivity restored.
    }
)


@dataclass(frozen=True)
class IbkrShadowConfig:
    host: str = DEFAULT_IBKR_SHADOW_HOST
    port: int = DEFAULT_IBKR_SHADOW_PORT
    client_id: int = DEFAULT_IBKR_SHADOW_CLIENT_ID
    account_id: str | None = None
    connect_timeout_seconds: float = DEFAULT_IBKR_SHADOW_CONNECT_TIMEOUT_SECONDS
    read_timeout_seconds: float = DEFAULT_IBKR_SHADOW_READ_TIMEOUT_SECONDS


@dataclass(frozen=True)
class IbkrShadowSnapshot:
    broker_snapshot: BrokerSnapshot
    available_accounts: tuple[str, ...]
    resolved_account_id: str
    net_liquidation: float | None
    cash: float | None
    buying_power: float | None
    raw_positions: list[dict[str, Any]]
    raw_account_summary: list[dict[str, Any]]
    warnings: list[str]


class IbkrShadowClient(Protocol):
    def load_shadow_snapshot(
        self,
        *,
        config: IbkrShadowConfig,
        timestamp: datetime,
    ) -> IbkrShadowSnapshot:
        """Load the current paper-account snapshot without submitting orders."""


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def resolve_shadow_timestamp(value: str | None) -> datetime:
    if value is None:
        return _chicago_now()
    parsed = datetime.fromisoformat(value)
    if ZoneInfo is not None:
        chicago = ZoneInfo("America/Chicago")
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=chicago)
        return parsed.astimezone(chicago)
    return parsed


def _parse_float_env(value: str | None, *, field_name: str, default: float) -> float:
    if value is None or not value.strip():
        return float(default)
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric.") from exc
    return parsed


def _parse_int_env(value: str | None, *, field_name: str, default: int) -> int:
    if value is None or not value.strip():
        return int(default)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc
    return parsed


def _normalized_account_id(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    return normalized or None


def _looks_like_paper_account_id(account_id: str) -> bool:
    normalized = account_id.strip().upper()
    return normalized.startswith("DU") and normalized[2:].isdigit()


def load_ibkr_shadow_config(
    *,
    host: str | None = None,
    port: int | None = None,
    client_id: int | None = None,
    account_id: str | None = None,
    connect_timeout_seconds: float | None = None,
    read_timeout_seconds: float | None = None,
) -> IbkrShadowConfig:
    resolved_host = (host or os.environ.get("IBKR_TWS_HOST") or DEFAULT_IBKR_SHADOW_HOST).strip()
    if not resolved_host:
        raise ValueError("IBKR shadow host must not be empty.")

    resolved_port = (
        int(port)
        if port is not None
        else _parse_int_env(
            os.environ.get("IBKR_TWS_PORT"),
            field_name="IBKR_TWS_PORT",
            default=DEFAULT_IBKR_SHADOW_PORT,
        )
    )
    if resolved_port != DEFAULT_IBKR_SHADOW_PORT:
        raise ValueError(
            f"IBKR shadow execution is hard-guarded to the paper TWS port {DEFAULT_IBKR_SHADOW_PORT}. "
            f"Refusing port {resolved_port}."
        )

    resolved_client_id = (
        int(client_id)
        if client_id is not None
        else _parse_int_env(
            os.environ.get("IBKR_TWS_CLIENT_ID"),
            field_name="IBKR_TWS_CLIENT_ID",
            default=DEFAULT_IBKR_SHADOW_CLIENT_ID,
        )
    )
    if resolved_client_id < 0:
        raise ValueError("IBKR shadow client_id must be >= 0.")

    resolved_account_id = _normalized_account_id(account_id or os.environ.get("IBKR_PAPER_ACCOUNT_ID"))
    if resolved_account_id is not None and not _looks_like_paper_account_id(resolved_account_id):
        raise ValueError(
            "IBKR shadow execution only supports explicit paper DU account ids. "
            f"Got {resolved_account_id!r}."
        )

    resolved_connect_timeout = (
        float(connect_timeout_seconds)
        if connect_timeout_seconds is not None
        else _parse_float_env(
            os.environ.get("IBKR_TWS_CONNECT_TIMEOUT_SECONDS"),
            field_name="IBKR_TWS_CONNECT_TIMEOUT_SECONDS",
            default=DEFAULT_IBKR_SHADOW_CONNECT_TIMEOUT_SECONDS,
        )
    )
    resolved_read_timeout = (
        float(read_timeout_seconds)
        if read_timeout_seconds is not None
        else _parse_float_env(
            os.environ.get("IBKR_TWS_READ_TIMEOUT_SECONDS"),
            field_name="IBKR_TWS_READ_TIMEOUT_SECONDS",
            default=DEFAULT_IBKR_SHADOW_READ_TIMEOUT_SECONDS,
        )
    )
    if resolved_connect_timeout <= 0.0:
        raise ValueError("IBKR shadow connect_timeout_seconds must be > 0.")
    if resolved_read_timeout <= 0.0:
        raise ValueError("IBKR shadow read_timeout_seconds must be > 0.")

    return IbkrShadowConfig(
        host=resolved_host,
        port=resolved_port,
        client_id=resolved_client_id,
        account_id=resolved_account_id,
        connect_timeout_seconds=resolved_connect_timeout,
        read_timeout_seconds=resolved_read_timeout,
    )


def _safe_int_from_float(value: float, *, field_name: str) -> int:
    if float(value).is_integer():
        return int(value)
    raise ValueError(f"{field_name} must be a whole-share quantity. Got {value!r}.")


def _summary_float(rows: list[dict[str, Any]], *, account_id: str, tag: str) -> float | None:
    preferred_rows = [
        row
        for row in rows
        if _normalized_account_id(str(row.get("account", ""))) == account_id
        and str(row.get("tag", "")).strip().lower() == tag.lower()
    ]
    fallback_rows = [
        row
        for row in rows
        if str(row.get("account", "")).strip().upper() == "ALL"
        and str(row.get("tag", "")).strip().lower() == tag.lower()
    ]
    for row in [*preferred_rows, *fallback_rows]:
        raw_value = row.get("value")
        if raw_value is None:
            continue
        try:
            return float(str(raw_value).strip().replace(",", ""))
        except ValueError:
            continue
    return None


def _resolve_shadow_account(
    *,
    configured_account_id: str | None,
    available_accounts: tuple[str, ...],
    raw_positions: list[dict[str, Any]],
    raw_account_summary: list[dict[str, Any]],
) -> str:
    position_accounts = {
        account_id
        for account_id in (
            _normalized_account_id(str(item.get("account", "")))
            for item in raw_positions
        )
        if account_id is not None
    }
    summary_accounts = {
        account_id
        for account_id in (
            _normalized_account_id(str(item.get("account", "")))
            for item in raw_account_summary
        )
        if account_id is not None and account_id != "ALL"
    }

    candidate_accounts = tuple(
        sorted(
            {
                *available_accounts,
                *position_accounts,
                *summary_accounts,
            }
        )
    )
    paper_accounts = tuple(account_id for account_id in candidate_accounts if _looks_like_paper_account_id(account_id))

    if configured_account_id is not None:
        if candidate_accounts and configured_account_id not in candidate_accounts:
            rendered = ", ".join(candidate_accounts)
            raise ValueError(
                f"Configured paper account {configured_account_id!r} was not reported by TWS. "
                f"Available accounts: {rendered or '-'}."
            )
        return configured_account_id

    if len(paper_accounts) == 1:
        return paper_accounts[0]
    if len(paper_accounts) > 1:
        rendered = ", ".join(paper_accounts)
        raise ValueError(f"Multiple paper accounts were reported by TWS ({rendered}). Pass --ibkr-account-id.")
    if len(candidate_accounts) == 1:
        raise ValueError(
            f"TWS reported only {candidate_accounts[0]!r}, which is not a paper DU account. "
            "Shadow execution refuses non-paper accounts."
        )
    raise ValueError("Could not resolve a single paper DU account from TWS. Pass --ibkr-account-id.")


def _normalize_shadow_snapshot(
    *,
    config: IbkrShadowConfig,
    timestamp: datetime,
    available_accounts: tuple[str, ...],
    raw_positions: list[dict[str, Any]],
    raw_account_summary: list[dict[str, Any]],
) -> IbkrShadowSnapshot:
    resolved_account_id = _resolve_shadow_account(
        configured_account_id=config.account_id,
        available_accounts=available_accounts,
        raw_positions=raw_positions,
        raw_account_summary=raw_account_summary,
    )
    if not _looks_like_paper_account_id(resolved_account_id):
        raise ValueError(
            f"Shadow execution resolved {resolved_account_id!r}, which is not a paper DU account."
        )

    positions: dict[str, BrokerPosition] = {}
    warnings: list[str] = []
    for item in raw_positions:
        account_id = _normalized_account_id(str(item.get("account", "")))
        if account_id != resolved_account_id:
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            raise ValueError("IBKR TWS position payload is missing a symbol.")
        sec_type = str(item.get("secType", "")).strip().upper()
        quantity = _safe_int_from_float(float(item.get("position", 0.0)), field_name=f"{symbol}.position")
        if symbol in positions:
            raise ValueError(f"Duplicate IBKR TWS position for symbol {symbol!r}.")
        avg_cost = item.get("avgCost")
        price = None if avg_cost is None else float(avg_cost)
        if sec_type not in SUPPORTED_IBKR_SEC_TYPES and quantity != 0:
            warnings.append(f"unsupported_sec_type:{symbol}:{sec_type or 'unknown'}")
        if quantity < 0:
            warnings.append(f"short_position:{symbol}")
        positions[symbol] = BrokerPosition(
            symbol=symbol,
            shares=quantity,
            price=price,
            instrument_type="Equity" if sec_type in SUPPORTED_IBKR_SEC_TYPES else sec_type or None,
            underlying_symbol=symbol,
            raw=dict(item),
        )

    cash = _summary_float(raw_account_summary, account_id=resolved_account_id, tag="TotalCashValue")
    buying_power = _summary_float(raw_account_summary, account_id=resolved_account_id, tag="BuyingPower")
    net_liquidation = _summary_float(raw_account_summary, account_id=resolved_account_id, tag="NetLiquidation")

    snapshot = BrokerSnapshot(
        broker_name="ibkr_tws_paper_shadow",
        account_id=resolved_account_id,
        as_of=timestamp.isoformat(),
        cash=None if cash is None else round(float(cash), 2),
        buying_power=None if buying_power is None else round(float(buying_power), 2),
        positions=positions,
        raw={
            "available_accounts": list(available_accounts),
            "positions_payload": raw_positions,
            "account_summary_payload": raw_account_summary,
            "warnings": list(warnings),
        },
    )
    return IbkrShadowSnapshot(
        broker_snapshot=snapshot,
        available_accounts=available_accounts,
        resolved_account_id=resolved_account_id,
        net_liquidation=None if net_liquidation is None else round(float(net_liquidation), 2),
        cash=snapshot.cash,
        buying_power=snapshot.buying_power,
        raw_positions=list(raw_positions),
        raw_account_summary=list(raw_account_summary),
        warnings=sorted(set(warnings)),
    )


class _IbkrShadowApp:
    def __init__(self) -> None:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper

        class _App(EWrapper, EClient):
            def __init__(self, outer: "_IbkrShadowApp") -> None:
                EWrapper.__init__(self)
                EClient.__init__(self, self)
                self._outer = outer

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                self._outer._connected_event.set()

            def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
                accounts = tuple(
                    sorted(
                        {
                            account_id
                            for account_id in (
                                _normalized_account_id(part)
                                for part in accountsList.split(",")
                            )
                            if account_id is not None
                        }
                    )
                )
                self._outer.available_accounts = accounts
                self._outer._managed_accounts_event.set()

            def position(self, account: str, contract: Any, position: float, avgCost: float) -> None:  # noqa: N802
                self._outer.raw_positions.append(
                    {
                        "account": account,
                        "avgCost": avgCost,
                        "conId": getattr(contract, "conId", None),
                        "currency": getattr(contract, "currency", None),
                        "exchange": getattr(contract, "exchange", None),
                        "localSymbol": getattr(contract, "localSymbol", None),
                        "position": position,
                        "primaryExchange": getattr(contract, "primaryExchange", None),
                        "secType": getattr(contract, "secType", None),
                        "symbol": getattr(contract, "symbol", None),
                    }
                )

            def positionEnd(self) -> None:  # noqa: N802
                self._outer._positions_end_event.set()

            def accountSummary(  # noqa: N802
                self,
                reqId: int,
                account: str,
                tag: str,
                value: str,
                currency: str,
            ) -> None:
                self._outer.raw_account_summary.append(
                    {
                        "account": account,
                        "currency": currency,
                        "reqId": reqId,
                        "tag": tag,
                        "value": value,
                    }
                )

            def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802
                self._outer._account_summary_end_event.set()

            def error(  # noqa: A003
                self,
                reqId: int,
                errorCode: int,
                errorString: str,
                advancedOrderRejectJson: str = "",
            ) -> None:
                message = f"IBKR TWS error {errorCode} (reqId={reqId}): {errorString}"
                if advancedOrderRejectJson:
                    message = f"{message} | {advancedOrderRejectJson}"
                if errorCode in NON_FATAL_IBKR_ERROR_CODES:
                    self._outer.warnings.append(message)
                    return
                self._outer._fatal_error = RuntimeError(message)
                self._outer._connected_event.set()
                self._outer._managed_accounts_event.set()
                self._outer._positions_end_event.set()
                self._outer._account_summary_end_event.set()

        self.available_accounts: tuple[str, ...] = ()
        self.raw_positions: list[dict[str, Any]] = []
        self.raw_account_summary: list[dict[str, Any]] = []
        self.warnings: list[str] = []
        self._fatal_error: RuntimeError | None = None
        self._connected_event = threading.Event()
        self._managed_accounts_event = threading.Event()
        self._positions_end_event = threading.Event()
        self._account_summary_end_event = threading.Event()
        self.client = _App(self)

    def _raise_if_fatal(self) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error

    def wait_for(self, event: threading.Event, *, timeout_seconds: float, label: str) -> None:
        deadline = time.monotonic() + float(timeout_seconds)
        while time.monotonic() < deadline:
            self._raise_if_fatal()
            if event.wait(timeout=0.05):
                self._raise_if_fatal()
                return
        self._raise_if_fatal()
        raise TimeoutError(f"Timed out waiting for IBKR TWS {label}.")


class RequestsIbkrShadowClient:
    def load_shadow_snapshot(
        self,
        *,
        config: IbkrShadowConfig,
        timestamp: datetime,
    ) -> IbkrShadowSnapshot:
        app = _IbkrShadowApp()
        summary_req_id = 9001
        connect_error: RuntimeError | Exception | None = None
        connect_result: bool | None = None

        def _connect() -> None:
            nonlocal connect_error, connect_result
            try:
                connect_result = app.client.connect(config.host, config.port, config.client_id)
            except Exception as exc:  # pragma: no cover - exercised against real ibapi/socket state
                connect_error = exc

        connect_thread = threading.Thread(target=_connect, name="ibkr-shadow-connect", daemon=True)
        thread = threading.Thread(target=app.client.run, name="ibkr-shadow-tws", daemon=True)
        try:
            connect_thread.start()
            connect_thread.join(timeout=config.connect_timeout_seconds)
            if connect_thread.is_alive():
                try:
                    app.client.disconnect()
                except Exception:
                    pass
                connect_thread.join(timeout=1.0)
                raise TimeoutError(
                    f"Timed out during the IBKR TWS handshake to {config.host}:{config.port}."
                )
            if connect_error is not None:
                raise connect_error
            if not connect_result:
                raise RuntimeError(
                    f"IBKR TWS connect({config.host!r}, {config.port}, {config.client_id}) returned False."
                )
            thread.start()
            app.wait_for(app._connected_event, timeout_seconds=config.connect_timeout_seconds, label="connection")

            app.client.reqPositions()
            app.client.reqAccountSummary(summary_req_id, "All", "NetLiquidation,TotalCashValue,BuyingPower")
            app.wait_for(app._positions_end_event, timeout_seconds=config.read_timeout_seconds, label="positions")
            app.wait_for(
                app._account_summary_end_event,
                timeout_seconds=config.read_timeout_seconds,
                label="account summary",
            )

            try:
                app.client.cancelPositions()
            except Exception:
                pass
            try:
                app.client.cancelAccountSummary(summary_req_id)
            except Exception:
                pass

            if not app._managed_accounts_event.is_set():
                app._managed_accounts_event.wait(timeout=0.25)

            snapshot = _normalize_shadow_snapshot(
                config=config,
                timestamp=timestamp,
                available_accounts=app.available_accounts,
                raw_positions=app.raw_positions,
                raw_account_summary=app.raw_account_summary,
            )
            merged_warnings = sorted(set([*snapshot.warnings, *app.warnings]))
            return IbkrShadowSnapshot(
                broker_snapshot=snapshot.broker_snapshot,
                available_accounts=snapshot.available_accounts,
                resolved_account_id=snapshot.resolved_account_id,
                net_liquidation=snapshot.net_liquidation,
                cash=snapshot.cash,
                buying_power=snapshot.buying_power,
                raw_positions=snapshot.raw_positions,
                raw_account_summary=snapshot.raw_account_summary,
                warnings=merged_warnings,
            )
        finally:
            try:
                app.client.disconnect()
            except Exception:
                pass
            if thread.is_alive():
                thread.join(timeout=1.0)


def build_ibkr_shadow_client(*, config: IbkrShadowConfig) -> RequestsIbkrShadowClient:
    return RequestsIbkrShadowClient()


def _validate_signal(*, signal: Any, allowed_symbols: tuple[str, ...]) -> None:
    action = signal.action.upper()
    if action not in SUPPORTED_SIGNAL_ACTIONS:
        raise ValueError(
            "IBKR paper shadow only supports HOLD / ENTER / EXIT / ROTATE / RESIZE next_action payloads."
        )
    if signal.target_shares < 0 or signal.desired_target_shares < 0:
        raise ValueError("IBKR paper shadow does not support negative share targets.")
    if signal.symbol.upper() != "CASH" and signal.symbol.upper() not in allowed_symbols:
        raise ValueError(
            f"IBKR paper shadow signal symbol {signal.symbol!r} is outside the allowed ETF universe: "
            f"{', '.join(allowed_symbols)}"
        )


def _normalize_allowed_symbols(allowed_symbols: set[str] | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(sorted({symbol.strip().upper() for symbol in allowed_symbols if symbol and symbol.strip()}))
    if not normalized:
        raise ValueError("IBKR paper shadow allowed symbol universe must not be empty.")
    return normalized


def _build_shadow_order_shape(
    *,
    account_id: str,
    symbol: str,
    side: str,
    quantity: int,
) -> dict[str, Any]:
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported shadow side {side!r}.")
    if quantity <= 0:
        raise ValueError("Shadow order quantity must be > 0.")
    return {
        "account": account_id,
        "contract": {
            "currency": "USD",
            "exchange": "SMART",
            "secType": "STK",
            "symbol": symbol,
        },
        "order": {
            "action": side,
            "orderType": "MKT",
            "outsideRth": False,
            "timeInForce": "DAY",
            "totalQuantity": quantity,
        },
        "simulation_only": True,
        "no_submit": True,
    }


def _reconciliation_status(*, item: PlanItem, has_run_blockers: bool) -> str:
    if item.delta_shares == 0:
        return "no_op"
    if has_run_blockers:
        return "blocked"
    if ORDER_SIDE_BY_CLASSIFICATION.get(item.classification) is not None and not item.blockers:
        return "actionable"
    return "drift"


def _signal_target_payload(signal: Any) -> dict[str, Any]:
    return {
        "action": signal.action,
        "desired_target_shares": signal.desired_target_shares,
        "event_id": signal.event_id,
        "next_rebalance": signal.next_rebalance,
        "raw_target_shares": signal.target_shares,
        "resize_new_shares": signal.resize_new_shares,
        "symbol": signal.symbol,
    }


def _reconciliation_items(plan: ExecutionPlan, *, has_run_blockers: bool) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for item in plan.items:
        status = _reconciliation_status(item=item, has_run_blockers=has_run_blockers)
        payload.append(
            {
                "action": item.classification,
                "blockers": list(item.blockers),
                "broker_current_position": item.current_broker_shares,
                "current_position": item.current_broker_shares,
                "delta_to_target": item.delta_shares,
                "estimated_notional": item.estimated_notional,
                "has_drift": item.delta_shares != 0,
                "is_actionable": status == "actionable",
                "has_blockers": has_run_blockers or bool(item.blockers),
                "is_noop": status == "no_op",
                "reconciliation_status": status,
                "reference_price": item.reference_price,
                "signal_target_shares": item.desired_target_shares,
                "symbol": item.symbol,
                "target_shares": item.desired_target_shares,
                "warnings": list(item.warnings),
            }
        )
    return payload


def _proposed_orders(plan: ExecutionPlan, *, account_id: str, config: IbkrShadowConfig) -> list[dict[str, Any]]:
    proposed: list[dict[str, Any]] = []
    for item in plan.items:
        side = ORDER_SIDE_BY_CLASSIFICATION.get(item.classification)
        quantity = abs(int(item.delta_shares))
        if side is None or quantity <= 0:
            continue
        proposed.append(
            {
                "action": side,
                "classification": item.classification,
                "current_position": item.current_broker_shares,
                "delta_to_target": item.delta_shares,
                "endpoint_used": {
                    "client_id": config.client_id,
                    "host": config.host,
                    "port": config.port,
                },
                "estimated_notional": item.estimated_notional,
                "intended_ibkr_order_shape": _build_shadow_order_shape(
                    account_id=account_id,
                    symbol=item.symbol,
                    side=side,
                    quantity=quantity,
                ),
                "quantity": quantity,
                "reference_price": item.reference_price,
                "simulation_only": True,
                "no_submit": True,
                "symbol": item.symbol,
                "target_shares": item.desired_target_shares,
            }
        )
    return proposed


def _decision_summary(*, proposed_orders: list[dict[str, Any]]) -> str:
    if not proposed_orders:
        return "HOLD"
    rendered = [f"would {order['action']} {order['quantity']} {order['symbol']}" for order in proposed_orders]
    return "; ".join(rendered)


def _count_nonzero_broker_positions(snapshot: BrokerSnapshot) -> int:
    return sum(1 for position in snapshot.positions.values() if position.shares != 0)


def _reconciliation_summary(
    *,
    reconciliation_items: list[dict[str, Any]],
    proposed_orders: list[dict[str, Any]],
    managed_symbol_count: int,
    broker_position_symbol_count: int,
    blockers: list[str],
) -> dict[str, Any]:
    has_blockers = bool(blockers)
    drift_symbol_count = sum(1 for item in reconciliation_items if item["has_drift"])
    blocked_symbol_count = sum(1 for item in reconciliation_items if item["reconciliation_status"] == "blocked")
    noop_symbol_count = sum(1 for item in reconciliation_items if item["is_noop"])
    actionable_symbol_count = sum(1 for item in reconciliation_items if item["is_actionable"])
    proposed_order_count = len(proposed_orders)
    has_drift = drift_symbol_count > 0
    if has_blockers:
        action_state = "blocked"
        is_noop = False
    elif not has_drift and proposed_order_count == 0:
        action_state = "no_op"
        is_noop = True
    elif proposed_order_count > 0:
        action_state = "actionable"
        is_noop = False
    else:
        action_state = "drift"
        is_noop = False
    return {
        "action_state": action_state,
        "actionable_symbol_count": actionable_symbol_count,
        "blocked_symbol_count": blocked_symbol_count,
        "blocker_count": len(blockers),
        "broker_position_symbol_count": broker_position_symbol_count,
        "drift_symbol_count": drift_symbol_count,
        "has_blockers": has_blockers,
        "has_drift": has_drift,
        "is_noop": is_noop,
        "managed_symbol_count": managed_symbol_count,
        "noop_symbol_count": noop_symbol_count,
        "proposed_order_count": proposed_order_count,
    }


def _shadow_action_fingerprint(
    *,
    account_id: str,
    allowed_symbols: tuple[str, ...],
    signal_target: dict[str, Any],
    reconciliation_items: list[dict[str, Any]],
    proposed_orders: list[dict[str, Any]],
    reconciliation_summary: dict[str, Any],
) -> str:
    fingerprint_payload = {
        "account_id": account_id,
        "allowed_symbols": list(allowed_symbols),
        "proposed_orders": [
            {
                "action": order["action"],
                "classification": order["classification"],
                "current_position": order["current_position"],
                "delta_to_target": order["delta_to_target"],
                "quantity": order["quantity"],
                "symbol": order["symbol"],
                "target_shares": order["target_shares"],
            }
            for order in sorted(
                proposed_orders,
                key=lambda item: (
                    str(item["symbol"]),
                    str(item["action"]),
                    int(item["quantity"]),
                    int(item["target_shares"]),
                    int(item["current_position"]),
                ),
            )
        ],
        "reconciliation_items": [
            {
                "action": item["action"],
                "broker_current_position": item["broker_current_position"],
                "delta_to_target": item["delta_to_target"],
                "reconciliation_status": item["reconciliation_status"],
                "signal_target_shares": item["signal_target_shares"],
                "symbol": item["symbol"],
            }
            for item in sorted(
                reconciliation_items,
                key=lambda current: (
                    str(current["symbol"]),
                    str(current["action"]),
                    int(current["signal_target_shares"]),
                    int(current["broker_current_position"]),
                ),
            )
        ],
        "reconciliation_summary": {
            "action_state": reconciliation_summary["action_state"],
            "blocked_symbol_count": reconciliation_summary["blocked_symbol_count"],
            "blocker_count": reconciliation_summary["blocker_count"],
            "broker_position_symbol_count": reconciliation_summary["broker_position_symbol_count"],
            "drift_symbol_count": reconciliation_summary["drift_symbol_count"],
            "has_blockers": reconciliation_summary["has_blockers"],
            "has_drift": reconciliation_summary["has_drift"],
            "is_noop": reconciliation_summary["is_noop"],
            "managed_symbol_count": reconciliation_summary["managed_symbol_count"],
            "noop_symbol_count": reconciliation_summary["noop_symbol_count"],
            "proposed_order_count": reconciliation_summary["proposed_order_count"],
        },
        "signal_target": signal_target,
    }
    digest_payload = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()


def _render_action_state(value: str) -> str:
    return str(value).replace("_", "-")


def build_ibkr_shadow_report(
    *,
    client: IbkrShadowClient,
    config: IbkrShadowConfig,
    allowed_symbols: set[str] | list[str] | tuple[str, ...],
    signal_raw: dict[str, Any],
    source_kind: str,
    source_label: str,
    source_ref: str | None,
    data_dir: Path | None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    generated_at = resolve_shadow_timestamp(timestamp)
    normalized_allowed_symbols = _normalize_allowed_symbols(allowed_symbols)
    signal = parse_signal_payload(signal_raw)
    _validate_signal(signal=signal, allowed_symbols=normalized_allowed_symbols)

    snapshot = client.load_shadow_snapshot(config=config, timestamp=generated_at)
    net_liquidation = snapshot.net_liquidation
    sizing_mode = "signal_target_shares"
    capital_input: float | None = None
    if net_liquidation is not None and net_liquidation > 0:
        sizing_mode = "account_capital"
        capital_input = float(net_liquidation)

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=snapshot.broker_snapshot,
        account_scope="managed_sleeve",
        managed_symbols=set(normalized_allowed_symbols),
        ack_unmanaged_holdings=True,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        broker_source_ref=f"ibkr_tws_paper:{snapshot.resolved_account_id}",
        data_dir=data_dir,
        generated_at=generated_at,
        sizing_mode=sizing_mode,
        capital_input=capital_input,
        cap_to_buying_power=False,
        reserve_cash_pct=0.0,
        max_allocation_pct=1.0,
        baseline_signal_capital=DEFAULT_SIGNAL_BASELINE_CAPITAL,
    )
    plan_payload = execution_plan_to_dict(plan)
    signal_target = _signal_target_payload(signal)
    plan_blockers = list(plan.blockers)
    has_blockers = bool(plan_blockers)
    reconciliation_items = _reconciliation_items(plan, has_run_blockers=has_blockers)
    proposed_orders = _proposed_orders(plan, account_id=snapshot.resolved_account_id, config=config)
    broker_position_symbol_count = _count_nonzero_broker_positions(snapshot.broker_snapshot)
    reconciliation_summary = _reconciliation_summary(
        reconciliation_items=reconciliation_items,
        proposed_orders=proposed_orders,
        managed_symbol_count=len(normalized_allowed_symbols),
        broker_position_symbol_count=broker_position_symbol_count,
        blockers=plan_blockers,
    )
    shadow_action_fingerprint = _shadow_action_fingerprint(
        account_id=snapshot.resolved_account_id,
        allowed_symbols=normalized_allowed_symbols,
        signal_target=signal_target,
        reconciliation_items=reconciliation_items,
        proposed_orders=proposed_orders,
        reconciliation_summary=reconciliation_summary,
    )

    return {
        "action_state": reconciliation_summary["action_state"],
        "allowed_symbols": list(normalized_allowed_symbols),
        "archive_manifest_path": None,
        "broker_account": {
            "account_id": snapshot.resolved_account_id,
            "available_accounts": list(snapshot.available_accounts),
        },
        "broker_position_symbol_count": broker_position_symbol_count,
        "broker_snapshot": {
            "account_id": snapshot.broker_snapshot.account_id,
            "as_of": snapshot.broker_snapshot.as_of,
            "broker_name": snapshot.broker_snapshot.broker_name,
            "buying_power": snapshot.broker_snapshot.buying_power,
            "cash": snapshot.broker_snapshot.cash,
            "positions": [
                {
                    "instrument_type": position.instrument_type,
                    "price": position.price,
                    "shares": position.shares,
                    "symbol": position.symbol,
                }
                for position in sorted(snapshot.broker_snapshot.positions.values(), key=lambda item: item.symbol)
            ],
        },
        "decision_summary": _decision_summary(proposed_orders=proposed_orders),
        "endpoint_used": {
            "client_id": config.client_id,
            "host": config.host,
            "port": config.port,
        },
        "execution_plan": plan_payload,
        "generated_at_chicago": generated_at.isoformat(),
        "has_blockers": reconciliation_summary["has_blockers"],
        "has_drift": reconciliation_summary["has_drift"],
        "is_noop": reconciliation_summary["is_noop"],
        "managed_symbol_count": len(normalized_allowed_symbols),
        "no_submit": True,
        "paper_endpoint_used": f"{config.host}:{config.port}",
        "proposed_order_count": len(proposed_orders),
        "proposed_orders": proposed_orders,
        "raw_account_summary": list(snapshot.raw_account_summary),
        "raw_positions": list(snapshot.raw_positions),
        "reconciliation_items": reconciliation_items,
        "reconciliation_summary": reconciliation_summary,
        "schema_name": IBKR_SHADOW_SCHEMA_NAME,
        "schema_version": IBKR_SHADOW_SCHEMA_VERSION,
        "signal": dict(signal_raw),
        "signal_target": signal_target,
        "shadow_action_fingerprint": shadow_action_fingerprint,
        "simulation_only": True,
        "sizing_mode": sizing_mode,
        "source": {
            "kind": source_kind,
            "label": source_label,
            "ref": source_ref,
        },
        "summary_metrics": {
            "buying_power": snapshot.buying_power,
            "cash": snapshot.cash,
            "net_liquidation": snapshot.net_liquidation,
        },
        "warnings": sorted(set([*snapshot.warnings, *plan.warnings])),
        "blockers": plan_blockers,
    }


def render_ibkr_shadow_text(payload: dict[str, Any]) -> str:
    summary = payload.get("reconciliation_summary") or {}
    blockers = payload.get("blockers") or []
    lines = [
        f"IBKR paper shadow {payload['source']['label']}",
        f"Endpoint: {payload['paper_endpoint_used']} client_id={payload['endpoint_used']['client_id']}",
        f"Account: {payload['broker_account']['account_id']}",
        f"Mode: simulation-only / no-submit",
        f"Timestamp: {payload['generated_at_chicago']}",
        "Signal target: "
        f"{payload['signal_target']['symbol']} "
        f"action={payload['signal_target']['action']} "
        f"target={payload['signal_target']['desired_target_shares']}",
        f"Run state: {_render_action_state(payload.get('action_state', 'drift'))}",
        "Summary: "
        f"{summary.get('drift_symbol_count', 0)} drifted, "
        f"{summary.get('noop_symbol_count', 0)} no-op, "
        f"{summary.get('actionable_symbol_count', 0)} actionable, "
        f"{summary.get('blocked_symbol_count', 0)} blocked",
        f"Orders: {payload.get('proposed_order_count', 0)} proposed",
        f"Decision: {payload['decision_summary']}",
    ]
    warnings = payload.get("warnings") or []
    if blockers:
        lines.append("Blockers: " + ", ".join(str(item) for item in blockers))
    if warnings:
        lines.append("Warnings: " + ", ".join(str(item) for item in warnings))
    reconciliation_items = payload.get("reconciliation_items") or []
    if reconciliation_items:
        for item in reconciliation_items:
            lines.append(
                "Reconciliation: "
                f"{item['symbol']} {_render_action_state(item['reconciliation_status'])} "
                f"(target {item['signal_target_shares']}, "
                f"current {item['broker_current_position']}, "
                f"delta {item['delta_to_target']:+d})"
            )
    elif blockers:
        lines.append("Reconciliation: blocked before managed-symbol reconciliation items were produced")
    else:
        lines.append("Reconciliation: no managed symbol drift detected")
    if payload.get("proposed_orders"):
        rendered_orders = [f"{order['action']} {order['quantity']} {order['symbol']}" for order in payload["proposed_orders"]]
        lines.append("Proposed orders: " + "; ".join(rendered_orders))
    else:
        lines.append("Proposed orders: none")
    return "\n".join(lines)
