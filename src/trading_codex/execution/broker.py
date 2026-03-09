from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Protocol

import requests

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


class TastytradeHttpClient(Protocol):
    def get_balances(self, *, account_id: str) -> Any:
        ...

    def get_positions(self, *, account_id: str) -> Any:
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
        return {"login": username, "password": password, "remember-me": True}

    def _device_challenge_headers(self, auth_error: TastytradeApiError) -> dict[str, str]:
        redirect = auth_error.redirect
        required_headers = [] if redirect is None else redirect.get("required_headers")
        if required_headers is None:
            required_headers = []
        if not isinstance(required_headers, list):
            raise ValueError(f"{auth_error} (redirect.required_headers must be a list)")

        headers: dict[str, str] = {}
        for header_name_raw in required_headers:
            header_name = str(header_name_raw).strip()
            if not header_name:
                continue
            if header_name.lower() != "x-tastyworks-challenge-token":
                raise ValueError(f"{auth_error} (unsupported challenge header requirement: {header_name})")
            token = self.challenge_token or auth_error.header_value(header_name)
            if not token:
                raise ValueError(
                    "Tastytrade device challenge requires X-Tastyworks-Challenge-Token. "
                    "Set TASTYTRADE_CHALLENGE_TOKEN or pass --tastytrade-challenge-token."
                )
            headers[header_name] = token
        return headers

    def _complete_device_challenge(self, auth_error: TastytradeApiError) -> None:
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

        challenge_code = None if self.challenge_code is None else self.challenge_code.strip()
        if not challenge_code:
            raise ValueError(
                "Tastytrade device challenge requires a challenge code. "
                "Set TASTYTRADE_CHALLENGE_CODE or pass --tastytrade-challenge-code."
            )

        headers = self._device_challenge_headers(auth_error)
        try:
            self._request_json(
                normalized_method,
                path.strip(),
                json_payload={"code": challenge_code},
                include_auth=False,
                extra_headers=headers,
            )
        except TastytradeApiError as exc:
            raise ValueError(f"Tastytrade device challenge failed: {exc}") from exc

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
            self._complete_device_challenge(exc)
            try:
                payload = self._request_json(
                    "POST",
                    "/sessions",
                    json_payload=session_payload,
                    include_auth=False,
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
