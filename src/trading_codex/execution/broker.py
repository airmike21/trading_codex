from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from trading_codex.execution.models import BrokerPosition, BrokerSnapshot


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


class BrokerPositionAdapter(Protocol):
    def load_snapshot(self) -> BrokerSnapshot:
        ...


class FileBrokerPositionAdapter:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load_snapshot(self) -> BrokerSnapshot:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return parse_broker_snapshot(raw)
