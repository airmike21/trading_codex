from __future__ import annotations

import getpass
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Protocol

import requests

from trading_codex.execution.models import (
    ExecutionPlan,
    BrokerPosition,
    BrokerSnapshot,
    LiveSubmissionExport,
    LiveSubmittedOrder,
    SimulatedOrderRequest,
    SimulatedSubmissionExport,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


LIVE_SUPPORTED_SIDES = {"BUY", "SELL"}
LIVE_SUPPORTED_CLASSIFICATIONS = {"BUY", "SELL", "RESIZE_BUY", "RESIZE_SELL", "EXIT"}
LIVE_SUPPORTED_INSTRUMENT_TYPE = "Equity"
LIVE_SUPPORTED_ORDER_TYPE = "MARKET"
LIVE_SUPPORTED_TIME_IN_FORCE = "DAY"


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _coerce_int_like(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field_name} must be a whole number.")
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            raise ValueError(f"{field_name} must not be empty.")
        try:
            return int(stripped)
        except ValueError as exc:
            try:
                numeric = float(stripped)
            except ValueError:
                raise ValueError(f"{field_name} must be an integer.") from exc
            if not numeric.is_integer():
                raise ValueError(f"{field_name} must be a whole number.") from exc
            return int(numeric)
    raise ValueError(f"{field_name} must be an integer.")


def _coerce_optional_float(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric.")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return float(stripped)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be numeric.") from exc
    raise ValueError(f"{field_name} must be numeric.")


def _position_price(raw: dict[str, Any]) -> float | None:
    for key in ("price", "current_price", "last_price", "market_price"):
        if key in raw:
            return _coerce_optional_float(raw.get(key), field_name=key)
    return None


def _coerce_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _coerce_optional_string(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    stripped = value.strip()
    return stripped or None


def _tastytrade_position_price(raw: dict[str, Any]) -> float | None:
    for key in (
        "close-price",
        "price",
        "mark-price",
        "market-price",
        "average-daily-market-close-price",
        "average-open-price",
    ):
        if key in raw:
            return _coerce_optional_float(raw.get(key), field_name=key)
    return None


def _extract_tastytrade_data_object(raw: Any, *, endpoint: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"Tastytrade {endpoint} payload must be a JSON object.")
    data = raw.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"Tastytrade {endpoint} payload must include a data object.")
    return data


def _extract_tastytrade_items(raw: Any, *, endpoint: str) -> list[dict[str, Any]]:
    data = _extract_tastytrade_data_object(raw, endpoint=endpoint)
    items = data.get("items")
    if not isinstance(items, list):
        raise ValueError(f"Tastytrade {endpoint} payload must include data.items as a list.")
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"Tastytrade {endpoint} data.items entries must be objects.")
        normalized.append(item)
    return normalized


def _format_tastytrade_error(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if not isinstance(error, dict):
        return None

    parts: list[str] = []
    code = error.get("code")
    if isinstance(code, str) and code.strip():
        parts.append(code.strip())
    message = error.get("message")
    if isinstance(message, str) and message.strip():
        parts.append(message.strip())

    redirect = error.get("redirect")
    if isinstance(redirect, dict):
        redirect_bits: list[str] = []
        method = redirect.get("method")
        if isinstance(method, str) and method.strip():
            redirect_bits.append(f"method={method.strip()}")
        url = redirect.get("url")
        if isinstance(url, str) and url.strip():
            redirect_bits.append(f"url={url.strip()}")
        headers = redirect.get("required_headers")
        if isinstance(headers, list) and headers:
            rendered_headers = ",".join(str(header).strip() for header in headers if str(header).strip())
            if rendered_headers:
                redirect_bits.append(f"required_headers={rendered_headers}")
        if redirect_bits:
            parts.append(f"redirect[{'; '.join(redirect_bits)}]")

    rendered = ": ".join(parts[:2]) if parts else None
    if rendered and len(parts) > 2:
        rendered = f"{rendered} ({'; '.join(parts[2:])})"
    return rendered


def _extract_tastytrade_error(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if not isinstance(error, dict):
        return None
    return error


def _extract_redirect_metadata(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    redirect = raw.get("redirect")
    return redirect if isinstance(redirect, dict) else None


def _extract_required_headers(redirect: dict[str, Any] | None) -> list[str]:
    if redirect is None:
        return []
    raw_headers = redirect.get("required_headers", redirect.get("required-headers"))
    if raw_headers is None:
        return []
    if not isinstance(raw_headers, list):
        raise ValueError("Challenge redirect required headers must be a list.")
    rendered: list[str] = []
    for value in raw_headers:
        header = str(value).strip()
        if header:
            rendered.append(header)
    return rendered


class TastytradeApiError(ValueError):
    def __init__(
        self,
        *,
        path: str,
        status_code: int,
        payload: object,
        headers: dict[str, str],
    ) -> None:
        self.path = path
        self.status_code = status_code
        self.payload = payload
        self.headers = dict(headers)
        detail = _format_tastytrade_error(payload)
        if detail:
            message = f"Tastytrade {path} request failed: {detail}"
        else:
            message = f"Tastytrade {path} request failed with status {status_code}"
        super().__init__(message)

    @property
    def error_code(self) -> str | None:
        error = _extract_tastytrade_error(self.payload)
        code = None if error is None else error.get("code")
        return code.strip() if isinstance(code, str) and code.strip() else None

    @property
    def redirect(self) -> dict[str, Any] | None:
        error = _extract_tastytrade_error(self.payload)
        redirect = None if error is None else error.get("redirect")
        return redirect if isinstance(redirect, dict) else None

    def header_value(self, name: str) -> str | None:
        target = name.lower()
        for key, value in self.headers.items():
            if key.lower() == target:
                return value
        return None


def _signed_tastytrade_quantity(raw: dict[str, Any], *, symbol: str) -> int:
    quantity = _coerce_int_like(raw.get("quantity"), field_name=f"{symbol}.quantity")
    direction_raw = raw.get("quantity-direction", "Long")
    direction = _coerce_non_empty_string(direction_raw, field_name=f"{symbol}.quantity-direction").lower()
    if direction == "short":
        return -abs(quantity)
    if direction in {"long", "buy"}:
        return abs(quantity)
    raise ValueError(f"{symbol}.quantity-direction must be Long or Short.")


def normalize_tastytrade_snapshot(
    *,
    account_id: str,
    positions_payload: Any,
    balances_payload: Any,
) -> BrokerSnapshot:
    normalized_account_id = _coerce_non_empty_string(account_id, field_name="account_id")
    positions_raw = _extract_tastytrade_items(positions_payload, endpoint="/accounts/{account_id}/positions")
    balances_raw = _extract_tastytrade_data_object(balances_payload, endpoint="/accounts/{account_id}/balances")

    payload_account = balances_raw.get("account-number")
    if payload_account is not None and str(payload_account).strip() != normalized_account_id:
        raise ValueError("Tastytrade balances payload account-number does not match requested account_id.")

    positions: dict[str, BrokerPosition] = {}
    as_of_candidates: list[str] = []
    for item in positions_raw:
        symbol_value = item.get("symbol", item.get("underlying-symbol"))
        symbol = _coerce_non_empty_string(symbol_value, field_name="position.symbol").upper()
        if symbol in positions:
            raise ValueError(f"Duplicate Tastytrade broker position for symbol {symbol!r}.")
        shares = _signed_tastytrade_quantity(item, symbol=symbol)
        price = _tastytrade_position_price(item)
        updated_at = item.get("updated-at")
        if isinstance(updated_at, str) and updated_at.strip():
            as_of_candidates.append(updated_at.strip())
        positions[symbol] = BrokerPosition(
            symbol=symbol,
            shares=shares,
            price=price,
            instrument_type=_coerce_optional_string(item.get("instrument-type"), field_name=f"{symbol}.instrument-type"),
            underlying_symbol=(
                None
                if item.get("underlying-symbol") is None
                else _coerce_non_empty_string(item.get("underlying-symbol"), field_name=f"{symbol}.underlying-symbol").upper()
            ),
            raw=dict(item),
        )

    cash = None
    for key in ("cash-balance", "cash-balance-effective", "cash-available-to-withdraw"):
        if key in balances_raw:
            cash = _coerce_optional_float(balances_raw.get(key), field_name=key)
            break

    buying_power = None
    for key in ("equity-buying-power", "buying-power-adjusted-for-futures", "available-trading-funds"):
        if key in balances_raw:
            buying_power = _coerce_optional_float(balances_raw.get(key), field_name=key)
            break

    as_of = max(as_of_candidates) if as_of_candidates else None
    return BrokerSnapshot(
        broker_name="tastytrade",
        account_id=normalized_account_id,
        as_of=as_of,
        cash=cash,
        buying_power=buying_power,
        positions=positions,
        raw={
            "balances_payload": balances_payload,
            "positions_payload": positions_payload,
        },
    )


def parse_broker_snapshot(raw: Any) -> BrokerSnapshot:
    if isinstance(raw, list):
        raw = {"positions": raw}
    if not isinstance(raw, dict):
        raise ValueError("Broker snapshot must be a JSON object or a positions array.")

    positions_raw = raw.get("positions")
    if not isinstance(positions_raw, list):
        raise ValueError("Broker snapshot must include positions as a list.")

    positions: dict[str, BrokerPosition] = {}
    for item in positions_raw:
        if not isinstance(item, dict):
            raise ValueError("Each broker position must be an object.")
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Each broker position must include a non-empty symbol.")
        normalized_symbol = symbol.strip().upper()
        if normalized_symbol in positions:
            raise ValueError(f"Duplicate broker position for symbol {normalized_symbol!r}.")
        shares = _coerce_int_like(item.get("shares"), field_name=f"{normalized_symbol}.shares")
        positions[normalized_symbol] = BrokerPosition(
            symbol=normalized_symbol,
            shares=shares,
            price=_position_price(item),
            instrument_type=_coerce_optional_string(
                item.get("instrument_type", item.get("instrument-type")),
                field_name=f"{normalized_symbol}.instrument_type",
            ),
            underlying_symbol=(
                None
                if item.get("underlying_symbol", item.get("underlying-symbol")) is None
                else _coerce_non_empty_string(
                    item.get("underlying_symbol", item.get("underlying-symbol")),
                    field_name=f"{normalized_symbol}.underlying_symbol",
                ).upper()
            ),
            raw=dict(item),
        )

    broker_name_raw = raw.get("broker_name", raw.get("broker", "mock_file"))
    broker_name = str(broker_name_raw).strip() if broker_name_raw is not None else "mock_file"
    if broker_name == "":
        broker_name = "mock_file"

    return BrokerSnapshot(
        broker_name=broker_name,
        account_id=None if raw.get("account_id") is None else str(raw.get("account_id")),
        as_of=None if raw.get("as_of") is None else str(raw.get("as_of")),
        cash=_coerce_optional_float(raw.get("cash"), field_name="cash"),
        buying_power=_coerce_optional_float(raw.get("buying_power"), field_name="buying_power"),
        positions=positions,
        raw=dict(raw),
    )


def _scoped_positions_payload(items: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "classification_reason": item.classification_reason,
            "instrument_type": item.instrument_type,
            "price": item.price,
            "scope_symbol": item.scope_symbol,
            "shares": item.shares,
            "symbol": item.symbol,
            "underlying_symbol": item.underlying_symbol,
        }
        for item in items
    ]


def _dedupe_strings(items: list[str]) -> list[str]:
    return sorted({item for item in items if item})


def _stable_sha256(payload: dict[str, Any]) -> str:
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _live_submission_fingerprint(*, broker_account_id: str, plan_sha256: str) -> str:
    return _stable_sha256(
        {
            "broker_account_id": broker_account_id,
            "plan_sha256": plan_sha256,
        }
    )


def _load_live_submission_ledger(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_number, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Live submission ledger is malformed at line {line_number}: {exc}") from exc
            if not isinstance(entry, dict):
                raise ValueError(f"Live submission ledger line {line_number} must be a JSON object.")
            entries.append(entry)
    return entries


def _find_duplicate_live_submission_record(path: Path, *, fingerprint: str) -> dict[str, Any] | None:
    blocking_results = {"accepted", "submitted"}
    for entry in reversed(_load_live_submission_ledger(path)):
        if entry.get("live_submission_fingerprint") != fingerprint:
            continue
        if entry.get("result") in blocking_results:
            return entry
    return None


def _append_live_submission_ledger_record(path: Path, *, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")


def _refusal_orders_from_simulated(export: SimulatedSubmissionExport, *, error: str) -> list[LiveSubmittedOrder]:
    return [
        LiveSubmittedOrder(
            submitted_at_chicago=None,
            account_id=order.account_id,
            broker_name=order.broker_name,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            instrument_type=order.instrument_type,
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            strategy=order.strategy,
            event_id=order.event_id,
            reference_price=order.reference_price,
            estimated_notional=order.estimated_notional,
            classification=order.classification,
            dry_run=True,
            attempted=False,
            succeeded=False,
            broker_order_id=None,
            broker_status=None,
            broker_response=None,
            error=error,
        )
        for order in export.orders
    ]


def build_live_submission_refusal_from_plan(
    *,
    plan: ExecutionPlan,
    refusal_reasons: list[str],
    plan_preview: dict[str, Any] | None = None,
    plan_sha256: str | None = None,
    live_submission_fingerprint: str | None = None,
    live_allowed_account: str | None = None,
    live_max_order_notional: float | None = None,
    live_max_order_qty: int | None = None,
    duplicate_submit_refusal: dict[str, Any] | None = None,
) -> LiveSubmissionExport:
    rendered_reasons = _dedupe_strings(refusal_reasons)
    return LiveSubmissionExport(
        generated_at_chicago=_chicago_now().isoformat(),
        dry_run=True,
        live_submit_requested=True,
        live_submit_attempted=False,
        submission_succeeded=False,
        source_kind=plan.source_kind,
        source_label=plan.source_label,
        source_ref=plan.source_ref,
        broker_name=plan.broker_snapshot.broker_name,
        account_id=plan.broker_snapshot.account_id,
        broker_source_ref=plan.broker_source_ref,
        account_scope=plan.account_scope,
        plan_math_scope=plan.plan_math_scope,
        sizing=plan.sizing,
        managed_symbols_universe=list(plan.managed_symbols_universe),
        blockers=list(plan.blockers),
        warnings=list(plan.warnings),
        unmanaged_holdings_acknowledged=plan.unmanaged_holdings_acknowledged,
        unmanaged_positions_count=len(plan.unmanaged_positions),
        unmanaged_positions_summary=list(plan.unmanaged_positions),
        refusal_reasons=rendered_reasons,
        orders=[],
        plan_preview=None if plan_preview is None else dict(plan_preview),
        plan_sha256=plan_sha256,
        live_submission_fingerprint=live_submission_fingerprint,
        live_allowed_account=live_allowed_account,
        live_max_order_notional=live_max_order_notional,
        live_max_order_qty=live_max_order_qty,
        duplicate_submit_refusal=duplicate_submit_refusal,
    )


def _build_live_submission_refusal_from_simulated(
    *,
    export: SimulatedSubmissionExport,
    refusal_reasons: list[str],
    live_submission_fingerprint: str | None = None,
    live_allowed_account: str | None = None,
    live_max_order_notional: float | None = None,
    live_max_order_qty: int | None = None,
    duplicate_submit_refusal: dict[str, Any] | None = None,
) -> LiveSubmissionExport:
    rendered_reasons = _dedupe_strings(refusal_reasons)
    error = "; ".join(rendered_reasons) if rendered_reasons else "live submission refused"
    return LiveSubmissionExport(
        generated_at_chicago=_chicago_now().isoformat(),
        dry_run=True,
        live_submit_requested=True,
        live_submit_attempted=False,
        submission_succeeded=False,
        source_kind=export.source_kind,
        source_label=export.source_label,
        source_ref=export.source_ref,
        broker_name=export.broker_name,
        account_id=export.account_id,
        broker_source_ref=export.broker_source_ref,
        account_scope=export.account_scope,
        plan_math_scope=export.plan_math_scope,
        sizing=export.sizing,
        managed_symbols_universe=list(export.managed_symbols_universe),
        blockers=list(export.blockers),
        warnings=list(export.warnings),
        unmanaged_holdings_acknowledged=export.unmanaged_holdings_acknowledged,
        unmanaged_positions_count=export.unmanaged_positions_count,
        unmanaged_positions_summary=list(export.unmanaged_positions_summary),
        refusal_reasons=rendered_reasons,
        orders=_refusal_orders_from_simulated(export, error=error),
        plan_preview=dict(export.plan_preview),
        plan_sha256=export.plan_sha256,
        live_submission_fingerprint=live_submission_fingerprint,
        live_allowed_account=live_allowed_account,
        live_max_order_notional=live_max_order_notional,
        live_max_order_qty=live_max_order_qty,
        duplicate_submit_refusal=duplicate_submit_refusal,
    )


def _live_refusal_reasons(
    *,
    export: SimulatedSubmissionExport,
    broker_account_id: str,
    confirm_account_id: str | None,
    live_allowed_account: str | None,
    confirm_plan_sha256: str | None,
    allowed_symbols: set[str],
    live_max_order_notional: float | None,
    live_max_order_qty: int | None,
) -> list[str]:
    reasons: list[str] = []
    allowed_symbol_set = {symbol.upper() for symbol in allowed_symbols}

    if export.broker_name != "tastytrade":
        reasons.append("live_submit_requires_tastytrade_broker")
    if export.account_scope != "managed_sleeve":
        reasons.append("live_submit_requires_managed_sleeve_scope")
    if export.blockers:
        reasons.append("live_submit_refused_for_blocked_plan")
    if export.unmanaged_positions_count > 0:
        reasons.append("live_submit_refused_for_unmanaged_positions")
    if not export.account_id:
        reasons.append("live_submit_requires_account_id")
    elif export.account_id != broker_account_id:
        reasons.append("live_submit_broker_account_mismatch")
    if confirm_account_id is None:
        reasons.append("live_submit_requires_confirmation")
    elif export.account_id is not None and confirm_account_id != export.account_id:
        reasons.append("live_submit_confirmation_account_mismatch")
    if live_allowed_account is None:
        reasons.append("live_submit_requires_live_allowed_account")
    elif export.account_id is not None and live_allowed_account != export.account_id:
        reasons.append("live_submit_live_allowed_account_mismatch")
    if confirm_plan_sha256 is None:
        reasons.append("live_submit_requires_confirm_plan_sha256")
    elif confirm_plan_sha256 != export.plan_sha256:
        reasons.append("live_submit_plan_sha256_mismatch")
    if not allowed_symbol_set:
        reasons.append("live_submit_requires_allowed_symbols")
    if live_max_order_notional is None:
        reasons.append("live_submit_requires_live_max_order_notional")
    elif live_max_order_notional <= 0:
        reasons.append("live_submit_invalid_live_max_order_notional")
    if live_max_order_qty is None:
        reasons.append("live_submit_requires_live_max_order_qty")
    elif live_max_order_qty <= 0:
        reasons.append("live_submit_invalid_live_max_order_qty")
    if not export.orders:
        reasons.append("live_submit_requires_actionable_orders")

    for order in export.orders:
        if order.side not in LIVE_SUPPORTED_SIDES:
            reasons.append(f"live_submit_unsupported_side:{order.symbol}:{order.side}")
        if order.classification not in LIVE_SUPPORTED_CLASSIFICATIONS:
            reasons.append(f"live_submit_unsupported_classification:{order.symbol}:{order.classification}")
        if order.instrument_type != LIVE_SUPPORTED_INSTRUMENT_TYPE:
            reasons.append(f"live_submit_unsupported_instrument_type:{order.symbol}:{order.instrument_type}")
        if order.symbol.upper() not in allowed_symbol_set:
            reasons.append(f"live_submit_symbol_outside_allowed_universe:{order.symbol}")
        if not isinstance(order.quantity, int) or isinstance(order.quantity, bool) or order.quantity <= 0:
            reasons.append(f"live_submit_invalid_quantity:{order.symbol}:{order.quantity}")
        elif live_max_order_qty is not None and live_max_order_qty > 0 and order.quantity > live_max_order_qty:
            reasons.append(f"live_submit_order_qty_exceeds_cap:{order.symbol}:{order.quantity}:{live_max_order_qty}")
        if order.estimated_notional is None:
            reasons.append(f"live_submit_missing_estimated_notional:{order.symbol}")
        elif (
            live_max_order_notional is not None
            and live_max_order_notional > 0
            and order.estimated_notional > live_max_order_notional
        ):
            reasons.append(
                f"live_submit_order_notional_exceeds_cap:{order.symbol}:{order.estimated_notional:.2f}:{live_max_order_notional:.2f}"
            )
        if order.order_type != LIVE_SUPPORTED_ORDER_TYPE:
            reasons.append(f"live_submit_unsupported_order_type:{order.symbol}:{order.order_type}")
        if order.time_in_force != LIVE_SUPPORTED_TIME_IN_FORCE:
            reasons.append(f"live_submit_unsupported_time_in_force:{order.symbol}:{order.time_in_force}")

    return _dedupe_strings(reasons)


def _tastytrade_equity_order_payload(order: SimulatedOrderRequest) -> dict[str, Any]:
    action = "Buy to Open" if order.side == "BUY" else "Sell to Close"
    return {
        "order-type": "Market",
        "time-in-force": "Day",
        "legs": [
            {
                "instrument-type": "Equity",
                "symbol": order.symbol,
                "quantity": order.quantity,
                "action": action,
            }
        ],
    }


def _normalize_tastytrade_order_submission(
    *,
    order: SimulatedOrderRequest,
    payload: Any,
    submitted_at_chicago: str,
) -> LiveSubmittedOrder:
    data = _extract_tastytrade_data_object(payload, endpoint="/accounts/{account_id}/orders")
    order_id = None
    for key in ("id", "order-id", "order_id"):
        raw_value = data.get(key)
        if raw_value is not None and str(raw_value).strip():
            order_id = str(raw_value).strip()
            break
    status = None
    for key in ("status", "order-status", "order_status"):
        raw_value = data.get(key)
        if raw_value is not None and str(raw_value).strip():
            status = str(raw_value).strip()
            break
    if order_id is None and status is None:
        raise ValueError("Tastytrade order submission response must include an order id or status.")

    return LiveSubmittedOrder(
        submitted_at_chicago=submitted_at_chicago,
        account_id=order.account_id,
        broker_name=order.broker_name,
        symbol=order.symbol,
        side=order.side,
        quantity=order.quantity,
        instrument_type=order.instrument_type,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        strategy=order.strategy,
        event_id=order.event_id,
        reference_price=order.reference_price,
        estimated_notional=order.estimated_notional,
        classification=order.classification,
        dry_run=False,
        attempted=True,
        succeeded=True,
        broker_order_id=order_id,
        broker_status=status,
        broker_response=data,
        error=None,
    )


def _accepted_order_count(orders: list[LiveSubmittedOrder]) -> int:
    return sum(1 for order in orders if order.succeeded)


def _live_submission_result_label(export: LiveSubmissionExport) -> str:
    accepted_order_count = _accepted_order_count(export.orders)
    if export.live_submit_attempted and export.submission_succeeded:
        return "submitted"
    if accepted_order_count > 0:
        return "accepted"
    if export.live_submit_attempted:
        return "failed_before_acceptance"
    if "live_submit_duplicate_fingerprint" in export.refusal_reasons:
        return "refused_duplicate"
    return "refused"


def _live_submission_ledger_record(
    *,
    export: LiveSubmissionExport,
    live_submission_fingerprint: str | None,
    artifact_path: Path | None,
) -> dict[str, Any]:
    return {
        "accepted_order_count": _accepted_order_count(export.orders),
        "account_id": export.account_id,
        "artifact_path": None if artifact_path is None else str(artifact_path),
        "attempted_order_count": sum(1 for order in export.orders if order.attempted),
        "broker_name": export.broker_name,
        "generated_at_chicago": export.generated_at_chicago,
        "live_submission_fingerprint": live_submission_fingerprint,
        "plan_sha256": export.plan_sha256,
        "result": _live_submission_result_label(export),
        "submission_succeeded": export.submission_succeeded,
    }


class BrokerPositionAdapter(Protocol):
    def load_snapshot(self) -> BrokerSnapshot:
        ...


class TastytradeHttpClient(Protocol):
    def get_balances(self, *, account_id: str) -> Any:
        ...

    def get_positions(self, *, account_id: str) -> Any:
        ...


class TastytradeOrderSubmitCapableClient(TastytradeHttpClient, Protocol):
    def place_order(self, *, account_id: str, payload: dict[str, Any]) -> Any:
        ...


class RequestsTastytradeHttpClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        challenge_code: str | None = None,
        challenge_token: str | None = None,
    ) -> None:
        self.session = session or requests.Session()
        self.base_url = (base_url or os.getenv("TASTYTRADE_API_BASE_URL") or "https://api.tastytrade.com").rstrip("/")
        self.timeout = timeout if timeout is not None else float(os.getenv("TASTYTRADE_TIMEOUT_SECONDS", "30"))
        self.challenge_code = challenge_code or os.getenv("TASTYTRADE_CHALLENGE_CODE")
        self.challenge_token = challenge_token or os.getenv("TASTYTRADE_CHALLENGE_TOKEN")
        self._auth_header: str | None = None

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_payload: dict[str, Any] | None = None,
        include_auth: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = dict(extra_headers or {})
        if include_auth:
            headers["Authorization"] = self._authorization_header()
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            json=json_payload,
            headers=headers,
            timeout=self.timeout,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            payload = None
            if response.ok:
                raise ValueError(f"Tastytrade {path} response must be valid JSON.") from exc
        if not response.ok:
            detail = _format_tastytrade_error(payload)
            if detail:
                raise TastytradeApiError(
                    path=path,
                    status_code=response.status_code,
                    payload=payload,
                    headers=dict(response.headers),
                )
            response.raise_for_status()
        if not isinstance(payload, dict):
            raise ValueError(f"Tastytrade {path} response must be a JSON object.")
        return payload

    def _session_request_payload(self, *, username: str, password: str) -> dict[str, Any]:
        return {"login": username, "password": password, "rememberMe": True}

    def _challenge_token_header(self, *, auth_error: TastytradeApiError, header_name: str) -> str:
        token = self.challenge_token or auth_error.header_value(header_name)
        if not token:
            raise ValueError(
                "Tastytrade device challenge requires X-Tastyworks-Challenge-Token. "
                "Set TASTYTRADE_CHALLENGE_TOKEN or pass --tastytrade-challenge-token."
            )
        return token

    def _device_challenge_headers(self, auth_error: TastytradeApiError) -> dict[str, str]:
        headers: dict[str, str] = {}
        for header_name in _extract_required_headers(auth_error.redirect):
            if header_name.lower() != "x-tastyworks-challenge-token":
                raise ValueError(f"{auth_error} (unsupported challenge header requirement: {header_name})")
            headers[header_name] = self._challenge_token_header(auth_error=auth_error, header_name=header_name)
        return headers

    def _resolve_challenge_code(self) -> str | None:
        challenge_code = None if self.challenge_code is None else "".join(self.challenge_code.split())
        if challenge_code:
            self.challenge_code = challenge_code
            return challenge_code
        if not sys.stdin.isatty():
            return None
        prompted = "".join(getpass.getpass("Tastytrade challenge code: ").split())
        if prompted == "":
            return None
        self.challenge_code = prompted
        return prompted

    def _device_challenge_retry_headers(
        self,
        *,
        auth_error: TastytradeApiError,
        challenge_payload: dict[str, Any],
        challenge_code: str,
    ) -> dict[str, str]:
        data = _extract_tastytrade_data_object(challenge_payload, endpoint="/device-challenge")
        redirect = _extract_redirect_metadata(data)
        required_headers = _extract_required_headers(redirect)
        if not required_headers:
            raise ValueError("Tastytrade device challenge response did not include retry headers.")

        headers: dict[str, str] = {}
        for header_name in required_headers:
            lowered = header_name.lower()
            if lowered == "x-tastyworks-challenge-token":
                headers[header_name] = self._challenge_token_header(auth_error=auth_error, header_name=header_name)
            elif lowered == "x-tastyworks-otp":
                headers[header_name] = challenge_code
            else:
                raise ValueError(f"Tastytrade device challenge requires unsupported retry header: {header_name}")
        return headers

    def _complete_device_challenge(self, auth_error: TastytradeApiError) -> dict[str, str]:
        redirect = auth_error.redirect
        if redirect is None:
            raise ValueError(f"{auth_error} (missing redirect metadata)")

        method = redirect.get("method")
        if not isinstance(method, str) or not method.strip():
            raise ValueError(f"{auth_error} (redirect.method is required)")
        normalized_method = method.strip().upper()
        if normalized_method != "POST":
            raise ValueError(f"{auth_error} (unsupported challenge method: {normalized_method})")

        path = redirect.get("url")
        if not isinstance(path, str) or not path.strip():
            raise ValueError(f"{auth_error} (redirect.url is required)")

        challenge_code = self._resolve_challenge_code()
        if not challenge_code:
            raise ValueError(
                "Tastytrade device challenge requires a challenge code. "
                "Set TASTYTRADE_CHALLENGE_CODE or pass --tastytrade-challenge-code."
            )

        headers = self._device_challenge_headers(auth_error)
        try:
            challenge_payload = self._request_json(
                normalized_method,
                path.strip(),
                json_payload={},
                include_auth=False,
                extra_headers=headers,
            )
        except TastytradeApiError as exc:
            raise ValueError(f"Tastytrade device challenge failed: {exc}") from exc
        return self._device_challenge_retry_headers(
            auth_error=auth_error,
            challenge_payload=challenge_payload,
            challenge_code=challenge_code,
        )

    def _authorization_header(self) -> str:
        if self._auth_header:
            return self._auth_header

        access_token = os.getenv("TASTYTRADE_ACCESS_TOKEN") or os.getenv("TASTYTRADE_API_TOKEN")
        if access_token:
            self._auth_header = f"Bearer {access_token.strip()}"
            return self._auth_header

        session_token = os.getenv("TASTYTRADE_SESSION_TOKEN")
        if session_token:
            self._auth_header = session_token.strip()
            return self._auth_header

        username = os.getenv("TASTYTRADE_USERNAME")
        password = os.getenv("TASTYTRADE_PASSWORD")
        if not username or not password:
            raise ValueError(
                "Tastytrade credentials not configured. Set TASTYTRADE_SESSION_TOKEN, "
                "TASTYTRADE_ACCESS_TOKEN, or TASTYTRADE_USERNAME/TASTYTRADE_PASSWORD."
            )

        session_payload = self._session_request_payload(username=username, password=password)
        try:
            payload = self._request_json(
                "POST",
                "/sessions",
                json_payload=session_payload,
                include_auth=False,
            )
        except TastytradeApiError as exc:
            if exc.error_code != "device_challenge_required":
                raise
            retry_headers = self._complete_device_challenge(exc)
            try:
                payload = self._request_json(
                    "POST",
                    "/sessions",
                    json_payload=session_payload,
                    include_auth=False,
                    extra_headers=retry_headers,
                )
            except TastytradeApiError as retry_exc:
                raise ValueError(f"Tastytrade session retry failed: {retry_exc}") from retry_exc
        data = _extract_tastytrade_data_object(payload, endpoint="/sessions")
        session_token_value = data.get("session-token")
        self._auth_header = _coerce_non_empty_string(session_token_value, field_name="data.session-token")
        return self._auth_header

    def get_balances(self, *, account_id: str) -> Any:
        return self._request_json("GET", f"/accounts/{account_id}/balances")

    def get_positions(self, *, account_id: str) -> Any:
        return self._request_json("GET", f"/accounts/{account_id}/positions")

    def place_order(self, *, account_id: str, payload: dict[str, Any]) -> Any:
        return self._request_json("POST", f"/accounts/{account_id}/orders", json_payload=payload)


class FileBrokerPositionAdapter:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load_snapshot(self) -> BrokerSnapshot:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return parse_broker_snapshot(raw)


class TastytradeBrokerPositionAdapter:
    def __init__(self, *, account_id: str, client: TastytradeHttpClient | None = None) -> None:
        self.account_id = _coerce_non_empty_string(account_id, field_name="account_id")
        self.client = client or RequestsTastytradeHttpClient()

    def load_snapshot(self) -> BrokerSnapshot:
        positions_payload = self.client.get_positions(account_id=self.account_id)
        balances_payload = self.client.get_balances(account_id=self.account_id)
        return normalize_tastytrade_snapshot(
            account_id=self.account_id,
            positions_payload=positions_payload,
            balances_payload=balances_payload,
        )


class TastytradeBrokerExecutionAdapter(TastytradeBrokerPositionAdapter):
    client: TastytradeHttpClient

    def __init__(self, *, account_id: str, client: TastytradeHttpClient | None = None) -> None:
        super().__init__(account_id=account_id, client=client)

    def submit_live_orders(
        self,
        *,
        export: SimulatedSubmissionExport,
        confirm_account_id: str | None,
        live_allowed_account: str | None,
        confirm_plan_sha256: str | None,
        allowed_symbols: set[str],
        live_max_order_notional: float | None,
        live_max_order_qty: int | None,
        ledger_path: Path | None = None,
        live_submission_artifact_path: Path | None = None,
    ) -> LiveSubmissionExport:
        normalized_confirm_account_id = (
            _coerce_non_empty_string(confirm_account_id, field_name="confirm_account_id")
            if confirm_account_id is not None and confirm_account_id.strip()
            else None
        )
        normalized_live_allowed_account = (
            _coerce_non_empty_string(live_allowed_account, field_name="live_allowed_account")
            if live_allowed_account is not None and live_allowed_account.strip()
            else None
        )
        normalized_confirm_plan_sha256 = (
            _coerce_non_empty_string(confirm_plan_sha256, field_name="confirm_plan_sha256")
            if confirm_plan_sha256 is not None and confirm_plan_sha256.strip()
            else None
        )
        live_submission_fingerprint = _live_submission_fingerprint(
            broker_account_id=self.account_id,
            plan_sha256=export.plan_sha256,
        )
        refusal_reasons = _live_refusal_reasons(
            export=export,
            broker_account_id=self.account_id,
            confirm_account_id=normalized_confirm_account_id,
            live_allowed_account=normalized_live_allowed_account,
            confirm_plan_sha256=normalized_confirm_plan_sha256,
            allowed_symbols=allowed_symbols,
            live_max_order_notional=live_max_order_notional,
            live_max_order_qty=live_max_order_qty,
        )
        duplicate_submit_refusal: dict[str, Any] | None = None
        if ledger_path is not None:
            try:
                duplicate_record = _find_duplicate_live_submission_record(
                    ledger_path,
                    fingerprint=live_submission_fingerprint,
                )
            except ValueError as exc:
                refusal_reasons.append("live_submit_duplicate_ledger_unreadable")
                duplicate_submit_refusal = {
                    "error": str(exc),
                    "ledger_path": str(ledger_path),
                    "live_submission_fingerprint": live_submission_fingerprint,
                }
            else:
                if duplicate_record is not None:
                    refusal_reasons.append("live_submit_duplicate_fingerprint")
                    duplicate_submit_refusal = {
                        "ledger_path": str(ledger_path),
                        "live_submission_fingerprint": live_submission_fingerprint,
                        "prior_record": duplicate_record,
                    }
        place_order = getattr(self.client, "place_order", None)
        if not callable(place_order):
            refusal_reasons.append("broker_adapter_not_live_submit_capable")
        refusal_reasons = _dedupe_strings(refusal_reasons)
        if refusal_reasons:
            refused_export = _build_live_submission_refusal_from_simulated(
                export=export,
                refusal_reasons=refusal_reasons,
                live_submission_fingerprint=live_submission_fingerprint,
                live_allowed_account=normalized_live_allowed_account,
                live_max_order_notional=live_max_order_notional,
                live_max_order_qty=live_max_order_qty,
                duplicate_submit_refusal=duplicate_submit_refusal,
            )
            if ledger_path is not None:
                _append_live_submission_ledger_record(
                    ledger_path,
                    record=_live_submission_ledger_record(
                        export=refused_export,
                        live_submission_fingerprint=live_submission_fingerprint,
                        artifact_path=live_submission_artifact_path,
                    ),
                )
            return refused_export

        submitted_orders: list[LiveSubmittedOrder] = []
        submission_succeeded = True
        for order in export.orders:
            submitted_at = _chicago_now().isoformat()
            request_payload = _tastytrade_equity_order_payload(order)
            try:
                raw_response = place_order(account_id=self.account_id, payload=request_payload)
                submitted_orders.append(
                    _normalize_tastytrade_order_submission(
                        order=order,
                        payload=raw_response,
                        submitted_at_chicago=submitted_at,
                    )
                )
            except Exception as exc:
                submission_succeeded = False
                submitted_orders.append(
                    LiveSubmittedOrder(
                        submitted_at_chicago=submitted_at,
                        account_id=order.account_id,
                        broker_name=order.broker_name,
                        symbol=order.symbol,
                        side=order.side,
                        quantity=order.quantity,
                        instrument_type=order.instrument_type,
                        order_type=order.order_type,
                        time_in_force=order.time_in_force,
                        strategy=order.strategy,
                        event_id=order.event_id,
                        reference_price=order.reference_price,
                        estimated_notional=order.estimated_notional,
                        classification=order.classification,
                        dry_run=False,
                        attempted=True,
                        succeeded=False,
                        broker_order_id=None,
                        broker_status=None,
                        broker_response=None,
                        error=str(exc),
                    )
                )
                break

        live_export = LiveSubmissionExport(
            generated_at_chicago=_chicago_now().isoformat(),
            dry_run=False,
            live_submit_requested=True,
            live_submit_attempted=True,
            submission_succeeded=submission_succeeded and len(submitted_orders) == len(export.orders),
            source_kind=export.source_kind,
            source_label=export.source_label,
            source_ref=export.source_ref,
            broker_name=export.broker_name,
            account_id=export.account_id,
            broker_source_ref=export.broker_source_ref,
            account_scope=export.account_scope,
            plan_math_scope=export.plan_math_scope,
            sizing=export.sizing,
            managed_symbols_universe=list(export.managed_symbols_universe),
            blockers=list(export.blockers),
            warnings=list(export.warnings),
            unmanaged_holdings_acknowledged=export.unmanaged_holdings_acknowledged,
            unmanaged_positions_count=export.unmanaged_positions_count,
            unmanaged_positions_summary=list(export.unmanaged_positions_summary),
            refusal_reasons=[],
            orders=submitted_orders,
            plan_preview=dict(export.plan_preview),
            plan_sha256=export.plan_sha256,
            live_submission_fingerprint=live_submission_fingerprint,
            live_allowed_account=normalized_live_allowed_account,
            live_max_order_notional=live_max_order_notional,
            live_max_order_qty=live_max_order_qty,
            duplicate_submit_refusal=duplicate_submit_refusal,
        )
        if ledger_path is not None:
            _append_live_submission_ledger_record(
                ledger_path,
                record=_live_submission_ledger_record(
                    export=live_export,
                    live_submission_fingerprint=live_submission_fingerprint,
                    artifact_path=live_submission_artifact_path,
                ),
            )
        return live_export
