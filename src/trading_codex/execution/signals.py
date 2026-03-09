from __future__ import annotations

from typing import Any

from trading_codex.execution.models import SignalPayload


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
            raise ValueError(f"{field_name} must be an integer.") from exc
    raise ValueError(f"{field_name} must be an integer.")


def _coerce_optional_int_like(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    return _coerce_int_like(value, field_name=field_name)


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


def expected_event_id(payload: dict[str, object]) -> str:
    def s(value: object) -> str:
        return "" if value is None else str(value)

    return (
        f"{s(payload.get('date'))}:"
        f"{s(payload.get('strategy'))}:"
        f"{s(payload.get('action'))}:"
        f"{s(payload.get('symbol'))}:"
        f"{s(payload.get('target_shares'))}:"
        f"{s(payload.get('resize_new_shares'))}:"
        f"{s(payload.get('next_rebalance'))}"
    )


def parse_signal_payload(raw: dict[str, Any]) -> SignalPayload:
    if not isinstance(raw, dict):
        raise ValueError("Signal payload must be a JSON object.")

    schema_name = raw.get("schema_name")
    if schema_name != "next_action":
        raise ValueError(f"Signal payload schema_name must be 'next_action'. Got: {schema_name!r}")

    required_text_fields = ("date", "strategy", "action", "symbol", "event_id")
    parsed_text: dict[str, str] = {}
    for field_name in required_text_fields:
        value = raw.get(field_name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Signal payload missing non-empty {field_name!r}.")
        parsed_text[field_name] = value

    target_shares = _coerce_int_like(raw.get("target_shares"), field_name="target_shares")
    if target_shares < 0:
        raise ValueError("Signal payload target_shares must be >= 0.")

    resize_prev_shares = _coerce_optional_int_like(raw.get("resize_prev_shares"), field_name="resize_prev_shares")
    resize_new_shares = _coerce_optional_int_like(raw.get("resize_new_shares"), field_name="resize_new_shares")
    if resize_new_shares is not None and resize_new_shares < 0:
        raise ValueError("Signal payload resize_new_shares must be >= 0.")

    expected = expected_event_id(raw)
    if parsed_text["event_id"] != expected:
        raise ValueError(
            "Signal payload event_id does not match the expected "
            '"{date}:{strategy}:{action}:{symbol}:{target_shares}:{resize_new_shares}:{next_rebalance}" contract.'
        )

    return SignalPayload(
        date=parsed_text["date"],
        strategy=parsed_text["strategy"],
        action=parsed_text["action"],
        symbol=parsed_text["symbol"],
        price=_coerce_optional_float(raw.get("price"), field_name="price"),
        target_shares=target_shares,
        resize_prev_shares=resize_prev_shares,
        resize_new_shares=resize_new_shares,
        next_rebalance=None if raw.get("next_rebalance") is None else str(raw.get("next_rebalance")),
        event_id=parsed_text["event_id"],
        raw=dict(raw),
    )


def desired_positions_from_signal(signal: SignalPayload) -> dict[str, int]:
    desired_target_shares = signal.desired_target_shares
    if desired_target_shares <= 0:
        return {}
    if signal.symbol.upper() == "CASH":
        return {}
    return {signal.symbol: desired_target_shares}
