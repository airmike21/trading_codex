from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import requests

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from trading_codex.execution.models import BrokerPosition, BrokerSnapshot, SignalPayload
from trading_codex.execution.planner import build_execution_plan, execution_plan_to_dict
from trading_codex.execution.signals import parse_signal_payload
from trading_codex.run_archive import resolve_archive_root, write_run_archive


DEFAULT_IBKR_PAPER_STATE_KEY = "primary_live_candidate_v1"
DEFAULT_IBKR_PAPER_BASE_URL = "https://127.0.0.1:5000/v1/api"
DEFAULT_IBKR_PAPER_TIMEOUT_SECONDS = 15.0
DEFAULT_IBKR_PAPER_VERIFY_SSL = False
DEFAULT_SIGNAL_BASELINE_CAPITAL = 10_000.0
SUPPORTED_SIGNAL_ACTIONS = frozenset({"HOLD", "ENTER", "EXIT", "ROTATE", "RESIZE"})
SUPPORTED_IBKR_ASSET_CLASSES = frozenset({"STK"})
BUY_CLASSIFICATIONS = frozenset({"BUY", "RESIZE_BUY"})
SELL_CLASSIFICATIONS = frozenset({"SELL", "RESIZE_SELL", "EXIT"})

IBKR_PAPER_STATE_SCHEMA_NAME = "ibkr_paper_lane_state"
IBKR_PAPER_STATE_SCHEMA_VERSION = 1
IBKR_PAPER_LEDGER_SCHEMA_NAME = "ibkr_paper_lane_ledger_entry"
IBKR_PAPER_LEDGER_SCHEMA_VERSION = 1
IBKR_PAPER_EVENT_RECEIPT_SCHEMA_NAME = "ibkr_paper_lane_event_receipt"
IBKR_PAPER_EVENT_RECEIPT_SCHEMA_VERSION = 1
IBKR_PAPER_CLAIM_SCHEMA_NAME = "ibkr_paper_lane_submit_claim"
IBKR_PAPER_CLAIM_SCHEMA_VERSION = 1
IBKR_PAPER_STATUS_SCHEMA_NAME = "ibkr_paper_lane_status"
IBKR_PAPER_STATUS_SCHEMA_VERSION = 1
IBKR_PAPER_APPLY_SCHEMA_NAME = "ibkr_paper_lane_apply_result"
IBKR_PAPER_APPLY_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class IbkrPaperClientConfig:
    account_id: str
    base_url: str = DEFAULT_IBKR_PAPER_BASE_URL
    verify_ssl: bool = DEFAULT_IBKR_PAPER_VERIFY_SSL
    timeout_seconds: float = DEFAULT_IBKR_PAPER_TIMEOUT_SECONDS


@dataclass(frozen=True)
class IbkrPaperState:
    state_key: str
    strategy: str | None
    account_id: str | None
    allowed_symbols: tuple[str, ...]
    last_status: dict[str, Any] | None
    last_attempt: dict[str, Any] | None
    last_applied: dict[str, Any] | None
    created_at_chicago: str
    updated_at_chicago: str


@dataclass(frozen=True)
class IbkrPaperPaths:
    base_dir: Path
    state_path: Path
    ledger_path: Path
    event_receipts_dir: Path
    pending_claims_dir: Path
    lock_path: Path


class IbkrPaperClient(Protocol):
    def ensure_account_access(self, *, account_id: str) -> dict[str, Any]:
        """Verify the trading account is reachable and selected."""

    def load_positions(self, *, account_id: str) -> list[dict[str, Any]]:
        """Return the current account positions."""

    def load_summary(self, *, account_id: str) -> dict[str, Any]:
        """Return the current account summary payload."""

    def resolve_stock_contract(self, *, symbol: str) -> dict[str, Any]:
        """Resolve a stock/ETF contract for order placement."""

    def place_order(self, *, account_id: str, payload: dict[str, Any]) -> object:
        """Submit one order payload to IBKR."""

    def confirm_order_reply(self, *, reply_id: str, confirmed: bool) -> object:
        """Confirm a deferred order reply warning."""

    def load_order_status(self, *, order_id: str) -> dict[str, Any]:
        """Return one broker order status payload."""


class RequestsIbkrPaperClient:
    def __init__(
        self,
        *,
        config: IbkrPaperClientConfig,
        session: requests.Session | None = None,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        if not self.config.verify_ssl:
            requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    def ensure_account_access(self, *, account_id: str) -> dict[str, Any]:
        brokerage_accounts = self._get_json("/iserver/accounts")
        if not isinstance(brokerage_accounts, dict):
            raise ValueError("IBKR /iserver/accounts must return a JSON object.")
        accounts = brokerage_accounts.get("accounts")
        if not isinstance(accounts, list):
            raise ValueError("IBKR /iserver/accounts response missing accounts list.")
        normalized_account = account_id.strip()
        if normalized_account not in {str(item).strip() for item in accounts if str(item).strip()}:
            raise ValueError(
                f"Configured IBKR account {normalized_account!r} is not present in /iserver/accounts."
            )

        portfolio_accounts = self._get_json("/portfolio/accounts")
        if not isinstance(portfolio_accounts, list):
            raise ValueError("IBKR /portfolio/accounts must return a JSON array.")
        portfolio_visible = {
            str(item.get("accountId", item.get("id", ""))).strip()
            for item in portfolio_accounts
            if isinstance(item, dict)
        }
        if normalized_account not in portfolio_visible:
            raise ValueError(
                f"Configured IBKR account {normalized_account!r} is not present in /portfolio/accounts."
            )

        if len(accounts) > 1:
            switched = self._post_json("/iserver/account", {"acctId": normalized_account})
            if not isinstance(switched, dict) or not bool(switched.get("set")):
                raise ValueError(
                    f"IBKR refused to switch the active brokerage account to {normalized_account!r}."
                )
        return {
            "brokerage_accounts": brokerage_accounts,
            "portfolio_accounts": portfolio_accounts,
        }

    def load_positions(self, *, account_id: str) -> list[dict[str, Any]]:
        payload = self._get_json(f"/portfolio2/{account_id}/positions")
        if not isinstance(payload, list):
            raise ValueError("IBKR /portfolio2/{accountId}/positions must return a JSON array.")
        positions: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                raise ValueError("IBKR positions entries must be JSON objects.")
            positions.append(item)
        return positions

    def load_summary(self, *, account_id: str) -> dict[str, Any]:
        payload = self._get_json(f"/portfolio/{account_id}/summary")
        if not isinstance(payload, dict):
            raise ValueError("IBKR /portfolio/{accountId}/summary must return a JSON object.")
        return payload

    def resolve_stock_contract(self, *, symbol: str) -> dict[str, Any]:
        payload = self._get_json("/trsrv/stocks", params={"symbols": symbol.strip().upper()})
        if not isinstance(payload, dict):
            raise ValueError("IBKR /trsrv/stocks must return a JSON object.")
        raw_bucket = payload.get(symbol.strip().upper())
        if not isinstance(raw_bucket, list) or not raw_bucket:
            raise ValueError(f"IBKR contract search returned no stock contracts for {symbol!r}.")

        candidates: list[dict[str, Any]] = []
        for item in raw_bucket:
            if not isinstance(item, dict):
                continue
            asset_class = str(item.get("assetClass", "")).strip().upper()
            if asset_class and asset_class not in SUPPORTED_IBKR_ASSET_CLASSES:
                continue
            contracts = item.get("contracts")
            if not isinstance(contracts, list):
                continue
            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                conid = contract.get("conid")
                if not _is_int_like(conid):
                    continue
                candidate = dict(contract)
                candidate.setdefault("symbol", symbol.strip().upper())
                candidate.setdefault("assetClass", asset_class or "STK")
                candidates.append(candidate)

        if not candidates:
            raise ValueError(f"IBKR contract search returned no usable stock contracts for {symbol!r}.")

        def sort_key(candidate: dict[str, Any]) -> tuple[int, int, int, str]:
            exchange = str(candidate.get("exchange", candidate.get("description", ""))).strip().upper()
            return (
                0 if bool(candidate.get("isUS")) else 1,
                0 if exchange == "SMART" else 1,
                0 if "ARCA" in exchange or "AMEX" in exchange or "NYSE" in exchange or "NASDAQ" in exchange else 1,
                exchange,
            )

        selected = sorted(candidates, key=sort_key)[0]
        return selected

    def place_order(self, *, account_id: str, payload: dict[str, Any]) -> object:
        return self._post_json(f"/iserver/account/{account_id}/orders", payload)

    def confirm_order_reply(self, *, reply_id: str, confirmed: bool) -> object:
        return self._post_json(f"/iserver/reply/{reply_id}", {"confirmed": bool(confirmed)})

    def load_order_status(self, *, order_id: str) -> dict[str, Any]:
        payload = self._get_json(f"/iserver/account/order/status/{order_id}")
        if not isinstance(payload, dict):
            raise ValueError("IBKR /iserver/account/order/status/{orderId} must return a JSON object.")
        return payload

    def _get_json(self, path: str, *, params: dict[str, Any] | None = None) -> object:
        return self._request_json("GET", path, params=params)

    def _post_json(self, path: str, payload: dict[str, Any]) -> object:
        return self._request_json("POST", path, json_payload=payload)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_payload: dict[str, Any] | None = None,
    ) -> object:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_payload,
            timeout=float(self.config.timeout_seconds),
            verify=bool(self.config.verify_ssl),
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError(f"IBKR {path} returned a non-JSON response ({response.status_code}).") from exc
        if not response.ok:
            detail = _ibkr_error_detail(payload) or f"status {response.status_code}"
            raise ValueError(f"IBKR {path} request failed: {detail}")
        if isinstance(payload, dict) and isinstance(payload.get("error"), str) and payload.get("error"):
            raise ValueError(f"IBKR {path} error: {payload['error']}")
        return payload


def build_ibkr_paper_client(*, config: IbkrPaperClientConfig) -> RequestsIbkrPaperClient:
    return RequestsIbkrPaperClient(config=config)


def load_ibkr_paper_client_config(
    *,
    account_id: str | None = None,
    base_url: str | None = None,
    verify_ssl: bool | None = None,
    timeout_seconds: float | None = None,
) -> IbkrPaperClientConfig:
    resolved_account_id = (account_id or os.environ.get("IBKR_PAPER_ACCOUNT_ID", "")).strip()
    if not resolved_account_id:
        raise ValueError("IBKR PaperTrader account id is required. Pass --ibkr-account-id or set IBKR_PAPER_ACCOUNT_ID.")

    resolved_base_url = (base_url or os.environ.get("IBKR_WEB_API_BASE_URL") or DEFAULT_IBKR_PAPER_BASE_URL).strip()
    if not resolved_base_url:
        raise ValueError("IBKR PaperTrader base URL must not be empty.")

    resolved_verify_ssl = (
        _parse_bool_env(os.environ.get("IBKR_WEB_API_VERIFY_SSL"))
        if verify_ssl is None
        else bool(verify_ssl)
    )
    if resolved_verify_ssl is None:
        resolved_verify_ssl = DEFAULT_IBKR_PAPER_VERIFY_SSL

    raw_timeout = timeout_seconds
    if raw_timeout is None and os.environ.get("IBKR_WEB_API_TIMEOUT_SECONDS"):
        try:
            raw_timeout = float(os.environ["IBKR_WEB_API_TIMEOUT_SECONDS"])
        except ValueError as exc:
            raise ValueError("IBKR_WEB_API_TIMEOUT_SECONDS must be numeric.") from exc
    resolved_timeout = float(raw_timeout if raw_timeout is not None else DEFAULT_IBKR_PAPER_TIMEOUT_SECONDS)
    if resolved_timeout <= 0.0:
        raise ValueError("IBKR PaperTrader timeout_seconds must be > 0.")

    return IbkrPaperClientConfig(
        account_id=resolved_account_id,
        base_url=resolved_base_url,
        verify_ssl=resolved_verify_ssl,
        timeout_seconds=resolved_timeout,
    )


def _parse_bool_env(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("Boolean env vars must be one of: 1/0 true/false yes/no on/off.")


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


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


def _safe_slug(value: str, *, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned.strip("._-") or fallback


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


def resolve_ibkr_paper_base_dir(
    *,
    state_key: str = DEFAULT_IBKR_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    create: bool,
) -> Path:
    if base_dir is not None:
        path = Path(base_dir)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    archive_root = resolve_archive_root(create=create)
    path = archive_root / "ibkr_paper_lane" / _safe_slug(state_key, fallback=DEFAULT_IBKR_PAPER_STATE_KEY)
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_ibkr_paper_paths(
    *,
    state_key: str = DEFAULT_IBKR_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    create: bool,
) -> IbkrPaperPaths:
    resolved_base = resolve_ibkr_paper_base_dir(state_key=state_key, base_dir=base_dir, create=create)
    if create:
        (resolved_base / "event_receipts").mkdir(parents=True, exist_ok=True)
        (resolved_base / "pending_claims").mkdir(parents=True, exist_ok=True)
    return IbkrPaperPaths(
        base_dir=resolved_base,
        state_path=resolved_base / "ibkr_paper_state.json",
        ledger_path=resolved_base / "ibkr_paper_ledger.jsonl",
        event_receipts_dir=resolved_base / "event_receipts",
        pending_claims_dir=resolved_base / "pending_claims",
        lock_path=resolved_base / "ibkr_paper_state.lock",
    )


@contextmanager
def _state_lock(paths: IbkrPaperPaths):
    if fcntl is None:  # pragma: no cover
        yield paths.lock_path
        return

    paths.lock_path.parent.mkdir(parents=True, exist_ok=True)
    with paths.lock_path.open("a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.seek(0)
        fh.truncate(0)
        fh.write(f"pid={os.getpid()} acquired_at={datetime.now(timezone.utc).isoformat()}\n")
        fh.flush()
        os.fsync(fh.fileno())
        try:
            yield paths.lock_path
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _empty_ibkr_paper_state(*, state_key: str, timestamp: datetime) -> IbkrPaperState:
    iso = timestamp.isoformat()
    return IbkrPaperState(
        state_key=state_key,
        strategy=None,
        account_id=None,
        allowed_symbols=(),
        last_status=None,
        last_attempt=None,
        last_applied=None,
        created_at_chicago=iso,
        updated_at_chicago=iso,
    )


def _state_to_dict(state: IbkrPaperState) -> dict[str, Any]:
    return {
        "account_id": state.account_id,
        "allowed_symbols": list(state.allowed_symbols),
        "created_at_chicago": state.created_at_chicago,
        "last_applied": state.last_applied,
        "last_attempt": state.last_attempt,
        "last_status": state.last_status,
        "schema_name": IBKR_PAPER_STATE_SCHEMA_NAME,
        "schema_version": IBKR_PAPER_STATE_SCHEMA_VERSION,
        "state_key": state.state_key,
        "strategy": state.strategy,
        "updated_at_chicago": state.updated_at_chicago,
    }


def _state_from_dict(payload: dict[str, Any]) -> IbkrPaperState:
    if payload.get("schema_name") != IBKR_PAPER_STATE_SCHEMA_NAME:
        raise ValueError(f"IBKR paper lane state schema_name must be {IBKR_PAPER_STATE_SCHEMA_NAME!r}.")

    raw_state_key = payload.get("state_key")
    if not isinstance(raw_state_key, str) or not raw_state_key.strip():
        raise ValueError("IBKR paper lane state missing non-empty state_key.")

    raw_allowed_symbols = payload.get("allowed_symbols", [])
    if not isinstance(raw_allowed_symbols, list):
        raise ValueError("IBKR paper lane state allowed_symbols must be a list.")
    allowed_symbols = tuple(sorted({str(item).strip().upper() for item in raw_allowed_symbols if str(item).strip()}))

    for field_name in ("last_status", "last_attempt", "last_applied"):
        value = payload.get(field_name)
        if value is not None and not isinstance(value, dict):
            raise ValueError(f"IBKR paper lane state {field_name} must be a JSON object or null.")

    strategy = payload.get("strategy")
    if strategy is not None and (not isinstance(strategy, str) or not strategy.strip()):
        raise ValueError("IBKR paper lane state strategy must be a non-empty string or null.")

    account_id = payload.get("account_id")
    if account_id is not None and (not isinstance(account_id, str) or not account_id.strip()):
        raise ValueError("IBKR paper lane state account_id must be a non-empty string or null.")

    created_at = payload.get("created_at_chicago")
    updated_at = payload.get("updated_at_chicago")
    if not isinstance(created_at, str) or not created_at.strip():
        raise ValueError("IBKR paper lane state missing created_at_chicago.")
    if not isinstance(updated_at, str) or not updated_at.strip():
        raise ValueError("IBKR paper lane state missing updated_at_chicago.")

    return IbkrPaperState(
        state_key=raw_state_key.strip(),
        strategy=None if strategy is None else strategy.strip(),
        account_id=None if account_id is None else account_id.strip(),
        allowed_symbols=allowed_symbols,
        last_status=None if payload.get("last_status") is None else dict(payload["last_status"]),
        last_attempt=None if payload.get("last_attempt") is None else dict(payload["last_attempt"]),
        last_applied=None if payload.get("last_applied") is None else dict(payload["last_applied"]),
        created_at_chicago=created_at.strip(),
        updated_at_chicago=updated_at.strip(),
    )


def load_ibkr_paper_state(paths: IbkrPaperPaths) -> IbkrPaperState:
    if not paths.state_path.exists():
        raise FileNotFoundError(paths.state_path)
    payload = json.loads(paths.state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("IBKR paper lane state file must contain a JSON object.")
    return _state_from_dict(payload)


def _load_or_create_state(
    *,
    paths: IbkrPaperPaths,
    state_key: str,
    timestamp: datetime,
) -> IbkrPaperState:
    if not paths.state_path.exists():
        state = _empty_ibkr_paper_state(state_key=state_key, timestamp=timestamp)
        _atomic_write_json(paths.state_path, _state_to_dict(state))
        return state
    return load_ibkr_paper_state(paths)


def _write_ibkr_paper_state(paths: IbkrPaperPaths, state: IbkrPaperState) -> None:
    _atomic_write_json(paths.state_path, _state_to_dict(state))


def _event_digest(event_id: str) -> str:
    import hashlib

    return hashlib.sha256(event_id.encode("utf-8")).hexdigest()


def _event_receipt_path(paths: IbkrPaperPaths, event_id: str) -> Path:
    return paths.event_receipts_dir / f"{_event_digest(event_id)}.json"


def _pending_claim_path(paths: IbkrPaperPaths, event_id: str) -> Path:
    return paths.pending_claims_dir / f"{_event_digest(event_id)}.json"


def event_already_applied(paths: IbkrPaperPaths, event_id: str) -> bool:
    return _event_receipt_path(paths, event_id).exists()


def event_claim_pending(paths: IbkrPaperPaths, event_id: str) -> bool:
    return _pending_claim_path(paths, event_id).exists()


def _load_event_claim(paths: IbkrPaperPaths, event_id: str) -> dict[str, Any] | None:
    claim_path = _pending_claim_path(paths, event_id)
    if not claim_path.exists():
        return None
    payload = json.loads(claim_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"IBKR paper lane claim file must contain a JSON object: {claim_path}")
    return payload


def _write_event_claim(paths: IbkrPaperPaths, payload: dict[str, Any]) -> Path:
    event_id = payload.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("IBKR paper lane claim payload missing event_id.")
    claim_path = _pending_claim_path(paths, event_id)
    _atomic_write_json(claim_path, payload)
    return claim_path


def _remove_event_claim(paths: IbkrPaperPaths, event_id: str) -> None:
    claim_path = _pending_claim_path(paths, event_id)
    try:
        claim_path.unlink()
    except FileNotFoundError:
        return
    _fsync_directory(claim_path.parent)


def _write_event_receipt(paths: IbkrPaperPaths, payload: dict[str, Any]) -> Path:
    event_id = payload.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("IBKR paper lane receipt payload missing event_id.")
    receipt_path = _event_receipt_path(paths, event_id)
    _atomic_write_json(receipt_path, payload)
    return receipt_path


def _normalize_allowed_symbols(allowed_symbols: set[str] | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(sorted({symbol.strip().upper() for symbol in allowed_symbols if symbol and symbol.strip()}))
    if not normalized:
        raise ValueError("IBKR paper lane allowed symbol universe must not be empty.")
    return normalized


def _ensure_state_constraints(
    *,
    state: IbkrPaperState,
    signal: SignalPayload,
    config: IbkrPaperClientConfig,
    allowed_symbols: tuple[str, ...],
) -> None:
    if state.strategy is not None and state.strategy != signal.strategy:
        raise ValueError(
            f"IBKR paper lane state is already locked to strategy {state.strategy!r}; got {signal.strategy!r}."
        )
    if state.account_id is not None and state.account_id != config.account_id:
        raise ValueError(
            f"IBKR paper lane state is already locked to account {state.account_id!r}; got {config.account_id!r}."
        )
    if state.allowed_symbols and tuple(state.allowed_symbols) != allowed_symbols:
        raise ValueError(
            "IBKR paper lane state is already locked to a different allowed symbol universe."
        )


def _with_state_update(
    *,
    state: IbkrPaperState,
    strategy: str,
    account_id: str,
    allowed_symbols: tuple[str, ...],
    timestamp: datetime,
    last_status: dict[str, Any] | None = None,
    last_attempt: dict[str, Any] | None = None,
    last_applied: dict[str, Any] | None = None,
) -> IbkrPaperState:
    return IbkrPaperState(
        state_key=state.state_key,
        strategy=state.strategy or strategy,
        account_id=state.account_id or account_id,
        allowed_symbols=state.allowed_symbols or allowed_symbols,
        last_status=last_status if last_status is not None else state.last_status,
        last_attempt=last_attempt if last_attempt is not None else state.last_attempt,
        last_applied=last_applied if last_applied is not None else state.last_applied,
        created_at_chicago=state.created_at_chicago,
        updated_at_chicago=timestamp.isoformat(),
    )


def _coerce_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric.")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if not stripped:
            raise ValueError(f"{field_name} must not be empty.")
        return float(stripped)
    raise ValueError(f"{field_name} must be numeric.")


def _coerce_optional_float(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    return _coerce_float(value, field_name=field_name)


def _is_int_like(value: object) -> bool:
    try:
        coerced = _coerce_float(value, field_name="int_like")
    except ValueError:
        return False
    return float(coerced).is_integer()


def _coerce_int_like(value: object, *, field_name: str) -> int:
    number = _coerce_float(value, field_name=field_name)
    if not float(number).is_integer():
        raise ValueError(f"{field_name} must be a whole number.")
    return int(number)


def _ibkr_error_detail(payload: object) -> str | None:
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, str) and error.strip():
            return error.strip()
    return None


def _normalized_summary_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def _summary_numeric_value(summary: dict[str, Any], *keys: str) -> float | None:
    normalized_candidates = {_normalized_summary_key(key) for key in keys}
    for raw_key, raw_value in summary.items():
        if _normalized_summary_key(str(raw_key)) not in normalized_candidates:
            continue
        extracted = _extract_summary_number(raw_value)
        if extracted is not None:
            return extracted
    return None


def _extract_summary_number(raw_value: object) -> float | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, bool):
        return None
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    if isinstance(raw_value, str):
        stripped = raw_value.strip().replace(",", "")
        if not stripped:
            return None
        if stripped in {"N/A", "--"}:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    if isinstance(raw_value, dict):
        for key in ("amount", "value", "current", "amt", "base"):
            if key in raw_value:
                extracted = _extract_summary_number(raw_value.get(key))
                if extracted is not None:
                    return extracted
    return None


def _positions_as_of(positions: list[dict[str, Any]], *, fallback: datetime) -> str:
    timestamps: list[int] = []
    for item in positions:
        raw_timestamp = item.get("timestamp")
        if _is_int_like(raw_timestamp):
            timestamps.append(int(float(raw_timestamp)))
    if not timestamps:
        return fallback.isoformat()
    latest = max(timestamps)
    return datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()


def _normalize_ibkr_broker_snapshot(
    *,
    account_id: str,
    positions_raw: list[dict[str, Any]],
    summary_raw: dict[str, Any],
    generated_at: datetime,
) -> tuple[BrokerSnapshot, dict[str, Any]]:
    positions: dict[str, BrokerPosition] = {}
    total_market_value = 0.0
    unsupported_position_reasons: list[str] = []
    for item in positions_raw:
        symbol = str(item.get("ticker", item.get("description", item.get("contractDesc", "")))).strip().upper()
        if not symbol:
            raise ValueError("IBKR position is missing a usable symbol/description.")
        if symbol in positions:
            raise ValueError(f"Duplicate IBKR broker position for symbol {symbol!r}.")

        shares = _coerce_int_like(item.get("position"), field_name=f"{symbol}.position")
        price = _coerce_optional_float(
            item.get("mktPrice", item.get("marketPrice")),
            field_name=f"{symbol}.mktPrice",
        )
        market_value = _coerce_optional_float(
            item.get("mktValue", item.get("marketValue")),
            field_name=f"{symbol}.mktValue",
        )
        if market_value is not None:
            total_market_value += float(market_value)

        asset_class = str(item.get("assetClass", item.get("secType", ""))).strip().upper() or None
        if asset_class not in SUPPORTED_IBKR_ASSET_CLASSES and shares != 0:
            unsupported_position_reasons.append(f"{symbol}:{asset_class or 'unknown_asset_class'}")
        if shares < 0:
            unsupported_position_reasons.append(f"{symbol}:short_position")

        positions[symbol] = BrokerPosition(
            symbol=symbol,
            shares=shares,
            price=price,
            instrument_type="Equity" if asset_class in SUPPORTED_IBKR_ASSET_CLASSES else asset_class,
            underlying_symbol=symbol,
            raw=dict(item),
        )

    cash = _summary_numeric_value(summary_raw, "totalcashvalue", "cashbalance", "settledcash", "cash")
    buying_power = _summary_numeric_value(summary_raw, "buyingpower", "availablefunds", "equitywithloanvalue")
    net_liquidation = _summary_numeric_value(summary_raw, "netliquidation", "netliq")
    if cash is None and net_liquidation is not None:
        cash = round(float(net_liquidation) - float(total_market_value), 2)
    if net_liquidation is None and cash is not None:
        net_liquidation = round(float(cash) + float(total_market_value), 2)
    if buying_power is None and net_liquidation is not None:
        buying_power = float(net_liquidation)

    snapshot = BrokerSnapshot(
        broker_name="ibkr_paper",
        account_id=account_id,
        as_of=_positions_as_of(positions_raw, fallback=generated_at),
        cash=None if cash is None else round(float(cash), 2),
        buying_power=None if buying_power is None else round(float(buying_power), 2),
        positions=positions,
        raw={
            "positions_payload": positions_raw,
            "summary_payload": summary_raw,
            "unsupported_position_reasons": unsupported_position_reasons,
        },
    )
    metrics = {
        "cash": snapshot.cash,
        "buying_power": snapshot.buying_power,
        "net_liquidation": None if net_liquidation is None else round(float(net_liquidation), 2),
        "total_market_value": round(float(total_market_value), 2),
        "unsupported_position_reasons": unsupported_position_reasons,
    }
    return snapshot, metrics


def _broker_snapshot_to_dict(snapshot: BrokerSnapshot) -> dict[str, Any]:
    return {
        "account_id": snapshot.account_id,
        "as_of": snapshot.as_of,
        "broker_name": snapshot.broker_name,
        "buying_power": snapshot.buying_power,
        "cash": snapshot.cash,
        "positions": [
            {
                "instrument_type": position.instrument_type,
                "price": position.price,
                "shares": position.shares,
                "symbol": position.symbol,
                "underlying_symbol": position.underlying_symbol,
            }
            for position in sorted(snapshot.positions.values(), key=lambda item: item.symbol)
        ],
    }


def _validate_signal(signal: SignalPayload, *, allowed_symbols: tuple[str, ...]) -> None:
    action = signal.action.upper()
    if action not in SUPPORTED_SIGNAL_ACTIONS:
        raise ValueError("IBKR paper lane only supports HOLD / ENTER / EXIT / ROTATE / RESIZE next_action payloads.")
    if signal.target_shares < 0 or signal.desired_target_shares < 0:
        raise ValueError("IBKR paper lane does not support negative share targets.")
    if signal.symbol.upper() != "CASH" and signal.symbol.upper() not in allowed_symbols:
        raise ValueError(
            f"IBKR paper lane signal symbol {signal.symbol!r} is outside the allowed ETF universe: {', '.join(allowed_symbols)}"
        )


def _build_trades_required(plan_payload: dict[str, Any]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for item in plan_payload["items"]:
        delta = int(item["delta_shares"])
        if delta == 0:
            continue
        classification = str(item["classification"])
        side = None
        if classification in BUY_CLASSIFICATIONS:
            side = "BUY"
        elif classification in SELL_CLASSIFICATIONS:
            side = "SELL"
        trades.append(
            {
                "classification": classification,
                "current_broker_shares": int(item["current_broker_shares"]),
                "delta_shares": delta,
                "desired_target_shares": int(item["desired_target_shares"]),
                "estimated_notional": item["estimated_notional"],
                "quantity": abs(delta),
                "reference_price": item["reference_price"],
                "side": side,
                "symbol": str(item["symbol"]),
            }
        )
    return trades


def _build_status_payload(
    *,
    timestamp: datetime,
    paths: IbkrPaperPaths,
    state: IbkrPaperState,
    signal_raw: dict[str, Any],
    source_kind: str,
    source_label: str,
    source_ref: str | None,
    data_dir: Path | None,
    allowed_symbols: tuple[str, ...],
    config: IbkrPaperClientConfig,
    client: IbkrPaperClient,
) -> dict[str, Any]:
    signal = parse_signal_payload(signal_raw)
    _validate_signal(signal, allowed_symbols=allowed_symbols)
    _ensure_state_constraints(state=state, signal=signal, config=config, allowed_symbols=allowed_symbols)

    account_prep = client.ensure_account_access(account_id=config.account_id)
    positions_raw = client.load_positions(account_id=config.account_id)
    summary_raw = client.load_summary(account_id=config.account_id)
    broker_snapshot, broker_metrics = _normalize_ibkr_broker_snapshot(
        account_id=config.account_id,
        positions_raw=positions_raw,
        summary_raw=summary_raw,
        generated_at=timestamp,
    )
    effective_capital = broker_metrics["net_liquidation"]
    if effective_capital is None or float(effective_capital) <= 0:
        raise ValueError("IBKR paper lane could not determine a positive account capital value from broker summary.")

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker_snapshot,
        account_scope="full_account",
        managed_symbols=set(allowed_symbols),
        ack_unmanaged_holdings=False,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        broker_source_ref=f"ibkr_paper:{config.account_id}",
        data_dir=data_dir,
        generated_at=timestamp,
        sizing_mode="account_capital",
        capital_input=float(effective_capital),
        cap_to_buying_power=False,
        reserve_cash_pct=0.0,
        max_allocation_pct=1.0,
        baseline_signal_capital=DEFAULT_SIGNAL_BASELINE_CAPITAL,
    )
    plan_payload = execution_plan_to_dict(plan)
    desired_positions = {
        item["symbol"]: int(item["desired_target_shares"])
        for item in plan_payload["items"]
        if int(item["desired_target_shares"]) > 0
    }
    current_positions = {
        position.symbol: int(position.shares)
        for position in broker_snapshot.positions.values()
        if position.shares != 0
    }
    trades_required = _build_trades_required(plan_payload)
    receipt_exists = event_already_applied(paths, signal.event_id)
    pending_claim = _load_event_claim(paths, signal.event_id)
    return {
        "allowed_symbols": list(allowed_symbols),
        "archive_manifest_path": None,
        "broker_account": {
            "account_id": config.account_id,
            "base_url": config.base_url,
            "timeout_seconds": config.timeout_seconds,
            "verify_ssl": config.verify_ssl,
        },
        "broker_snapshot": _broker_snapshot_to_dict(broker_snapshot),
        "broker_summary_metrics": broker_metrics,
        "current_positions": current_positions,
        "data_dir": None if data_dir is None else str(data_dir),
        "drift_present": bool(trades_required),
        "event_already_applied": receipt_exists,
        "event_claim_pending": pending_claim is not None,
        "execution_plan": plan_payload,
        "generated_at_chicago": timestamp.isoformat(),
        "paths": {
            "base_dir": str(paths.base_dir),
            "event_receipts_dir": str(paths.event_receipts_dir),
            "ledger_path": str(paths.ledger_path),
            "pending_claims_dir": str(paths.pending_claims_dir),
            "state_path": str(paths.state_path),
        },
        "pending_event_claim": pending_claim,
        "raw_signal_target_shares": signal.target_shares,
        "scaled_target_positions": desired_positions,
        "schema_name": IBKR_PAPER_STATUS_SCHEMA_NAME,
        "schema_version": IBKR_PAPER_STATUS_SCHEMA_VERSION,
        "signal": dict(signal_raw),
        "source": {
            "kind": source_kind,
            "label": source_label,
            "ref": source_ref,
        },
        "state_aligned_to_target": not trades_required,
        "strategy_locked": state.strategy or signal.strategy,
        "submission_ready": not plan.blockers and not receipt_exists and pending_claim is None,
        "trade_required": trades_required,
        "local_state": _state_to_dict(state),
        "broker_account_prep": account_prep,
    }


def _archive_run(
    *,
    timestamp: datetime,
    run_kind: str,
    mode: str,
    label: str,
    identity_parts: list[object],
    manifest_fields: dict[str, Any],
    json_artifacts: dict[str, Any],
    text_artifacts: dict[str, str],
) -> tuple[str | None, str | None]:
    try:
        archive = write_run_archive(
            timestamp=timestamp,
            run_kind=run_kind,
            mode=mode,
            label=label,
            identity_parts=identity_parts,
            manifest_fields=manifest_fields,
            json_artifacts=json_artifacts,
            text_artifacts=text_artifacts,
        )
    except Exception as exc:
        return None, str(exc)
    return str(archive.paths.manifest_path), None


def build_ibkr_paper_status(
    *,
    client: IbkrPaperClient,
    config: IbkrPaperClientConfig,
    allowed_symbols: set[str] | list[str] | tuple[str, ...],
    state_key: str = DEFAULT_IBKR_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    signal_raw: dict[str, Any],
    source_kind: str,
    source_label: str,
    source_ref: str | None = None,
    data_dir: Path | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    resolved_timestamp = _timestamp(timestamp)
    normalized_allowed_symbols = _normalize_allowed_symbols(allowed_symbols)
    paths = resolve_ibkr_paper_paths(state_key=state_key, base_dir=base_dir, create=True)
    with _state_lock(paths):
        state_before = _load_or_create_state(paths=paths, state_key=state_key, timestamp=resolved_timestamp)
        payload = _build_status_payload(
            timestamp=resolved_timestamp,
            paths=paths,
            state=state_before,
            signal_raw=signal_raw,
            source_kind=source_kind,
            source_label=source_label,
            source_ref=source_ref,
            data_dir=data_dir,
            allowed_symbols=normalized_allowed_symbols,
            config=config,
            client=client,
        )
        signal = parse_signal_payload(signal_raw)
        archive_manifest_path, archive_error = _archive_run(
            timestamp=resolved_timestamp,
            run_kind="ibkr_paper_lane_status",
            mode="status",
            label=state_key,
            identity_parts=[signal.event_id, config.account_id, source_kind, "status"],
            manifest_fields={
                "account_id": config.account_id,
                "action": signal.action,
                "event_already_applied": payload["event_already_applied"],
                "event_claim_pending": payload["event_claim_pending"],
                "event_id": signal.event_id,
                "signal_date": signal.date,
                "state_key": state_key,
                "strategy": signal.strategy,
            },
            json_artifacts={
                "ibkr_paper_status": payload,
                "signal_payload": signal_raw,
            },
            text_artifacts={"summary_text": render_ibkr_paper_status_text(payload)},
        )
        status_record = {
            "archive_error": archive_error,
            "archive_manifest_path": archive_manifest_path,
            "drift_present": payload["drift_present"],
            "event_already_applied": payload["event_already_applied"],
            "event_claim_pending": payload["event_claim_pending"],
            "event_id": signal.event_id,
            "generated_at_chicago": resolved_timestamp.isoformat(),
            "signal_action": signal.action,
            "signal_date": signal.date,
            "signal_symbol": signal.symbol,
        }
        state_after = _with_state_update(
            state=state_before,
            strategy=signal.strategy,
            account_id=config.account_id,
            allowed_symbols=normalized_allowed_symbols,
            timestamp=resolved_timestamp,
            last_status=status_record,
        )
        _write_ibkr_paper_state(paths, state_after)
        _append_jsonl_record(
            paths.ledger_path,
            record={
                "account_id": config.account_id,
                "archive_error": archive_error,
                "archive_manifest_path": archive_manifest_path,
                "drift_present": payload["drift_present"],
                "entry_kind": "status",
                "event_already_applied": payload["event_already_applied"],
                "event_claim_pending": payload["event_claim_pending"],
                "event_id": signal.event_id,
                "generated_at_chicago": resolved_timestamp.isoformat(),
                "schema_name": IBKR_PAPER_LEDGER_SCHEMA_NAME,
                "schema_version": IBKR_PAPER_LEDGER_SCHEMA_VERSION,
                "signal_action": signal.action,
                "signal_date": signal.date,
                "signal_symbol": signal.symbol,
                "state_key": state_key,
                "strategy": signal.strategy,
            },
        )
        output = dict(payload)
        output["archive_manifest_path"] = archive_manifest_path
        output["archive_error"] = archive_error
        output["local_state"] = _state_to_dict(state_after)
        return output


def _submission_candidates(
    *,
    plan_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates = [
        {
            "classification": str(item["classification"]),
            "current_broker_shares": int(item["current_broker_shares"]),
            "desired_target_shares": int(item["desired_target_shares"]),
            "quantity": abs(int(item["delta_shares"])),
            "reference_price": item["reference_price"],
            "side": "SELL" if str(item["classification"]) in SELL_CLASSIFICATIONS else "BUY",
            "symbol": str(item["symbol"]),
        }
        for item in plan_payload["items"]
        if int(item["delta_shares"]) != 0 and str(item["classification"]) in BUY_CLASSIFICATIONS | SELL_CLASSIFICATIONS
    ]
    return sorted(candidates, key=lambda item: (0 if item["side"] == "SELL" else 1, item["symbol"]))


def _broker_position_by_symbol(snapshot: BrokerSnapshot) -> dict[str, BrokerPosition]:
    return {symbol.upper(): position for symbol, position in snapshot.positions.items()}


def _resolve_order_contract(
    *,
    symbol: str,
    snapshot: BrokerSnapshot,
    client: IbkrPaperClient,
) -> dict[str, Any]:
    current_positions = _broker_position_by_symbol(snapshot)
    if symbol in current_positions:
        raw_conid = current_positions[symbol].raw.get("conid")
        if _is_int_like(raw_conid):
            return {
                "conid": int(float(raw_conid)),
                "symbol": symbol,
                "source": "broker_snapshot",
            }
    resolved = client.resolve_stock_contract(symbol=symbol)
    conid = resolved.get("conid")
    if not _is_int_like(conid):
        raise ValueError(f"IBKR contract lookup for {symbol!r} did not return a usable conid.")
    return {
        "conid": int(float(conid)),
        "symbol": symbol,
        "source": "trsrv_stocks",
        "raw": resolved,
    }


def _normalize_order_submission_response(raw: object) -> dict[str, Any]:
    if isinstance(raw, dict):
        if isinstance(raw.get("error"), str) and raw.get("error"):
            raise ValueError(str(raw["error"]))
        if _is_int_like(raw.get("order_id")) or _is_int_like(raw.get("orderId")):
            order_id = raw.get("order_id", raw.get("orderId"))
            return {
                "broker_order_id": str(int(float(order_id))),
                "broker_status": str(raw.get("order_status", raw.get("msg", "")) or "").strip() or None,
                "reply_required": False,
                "raw_response": raw,
                "reply_messages": [],
            }
    if isinstance(raw, list) and raw:
        first = raw[0]
        if not isinstance(first, dict):
            raise ValueError("IBKR order response entries must be JSON objects.")
        if isinstance(first.get("error"), str) and first.get("error"):
            raise ValueError(str(first["error"]))
        if _is_int_like(first.get("order_id")) or _is_int_like(first.get("orderId")):
            order_id = first.get("order_id", first.get("orderId"))
            return {
                "broker_order_id": str(int(float(order_id))),
                "broker_status": str(first.get("order_status", first.get("msg", "")) or "").strip() or None,
                "reply_required": False,
                "raw_response": raw,
                "reply_messages": [],
            }
        if isinstance(first.get("id"), str) and first["id"].strip():
            messages = first.get("message")
            rendered_messages = [
                str(item).strip()
                for item in (messages if isinstance(messages, list) else [])
                if str(item).strip()
            ]
            return {
                "broker_order_id": None,
                "broker_status": None,
                "reply_required": True,
                "raw_response": raw,
                "reply_id": first["id"].strip(),
                "reply_messages": rendered_messages,
            }
    raise ValueError("IBKR order response did not match a known success or warning shape.")


def _submit_order_with_replies(
    *,
    account_id: str,
    payload: dict[str, Any],
    client: IbkrPaperClient,
    confirm_replies: bool,
) -> dict[str, Any]:
    raw_response = client.place_order(account_id=account_id, payload=payload)
    normalized = _normalize_order_submission_response(raw_response)
    if not normalized["reply_required"]:
        return normalized

    if not confirm_replies:
        return normalized

    reply_messages: list[dict[str, Any]] = []
    current = normalized
    for _ in range(8):
        reply_id = current.get("reply_id")
        if not isinstance(reply_id, str) or not reply_id.strip():
            break
        raw_reply = client.confirm_order_reply(reply_id=reply_id, confirmed=True)
        reply_messages.append(
            {
                "reply_id": reply_id,
                "response": raw_reply,
            }
        )
        current = _normalize_order_submission_response(raw_reply)
        if not current["reply_required"]:
            current["reply_confirmations"] = reply_messages
            return current
    raise ValueError("IBKR order reply confirmation did not resolve to a submitted order within 8 confirmations.")


def render_ibkr_paper_status_text(payload: dict[str, Any]) -> str:
    signal = payload["signal"]
    lines = [
        f"IBKR paper lane {payload['local_state']['state_key']}",
        f"Account: {payload['broker_account']['account_id']}",
        f"Signal: {signal['date']} {signal['strategy']} {signal['action']} {signal['symbol']} event_id={signal['event_id']}",
        f"Latest event already applied: {'yes' if payload['event_already_applied'] else 'no'}",
        f"Pending submit claim: {'yes' if payload['event_claim_pending'] else 'no'}",
        f"Current: {_render_positions_summary(payload['current_positions'])}",
        (
            f"Target: {_render_positions_summary(payload['scaled_target_positions'])} "
            f"(raw signal target_shares={payload['raw_signal_target_shares']})"
        ),
        f"Drift present: {'yes' if payload['drift_present'] else 'no'}",
    ]
    trades = payload["trade_required"]
    if trades:
        lines.append("Orders needed:")
        for trade in trades:
            price = "-" if trade["reference_price"] is None else f"{float(trade['reference_price']):.2f}"
            notional = "-" if trade["estimated_notional"] is None else f"{float(trade['estimated_notional']):.2f}"
            lines.append(
                f"- {trade['side'] or trade['classification']} {trade['quantity']} {trade['symbol']} "
                f"@ {price} est={notional} [{trade['classification']}]"
            )
    else:
        lines.append("Orders needed: none")
    blockers = payload["execution_plan"]["blockers"]
    lines.append(f"Plan blockers: {', '.join(blockers) if blockers else 'none'}")
    lines.append(f"State path: {payload['paths']['state_path']}")
    lines.append(f"Ledger path: {payload['paths']['ledger_path']}")
    return "\n".join(lines)


def render_ibkr_paper_apply_text(payload: dict[str, Any]) -> str:
    lines = [
        f"IBKR paper lane {payload['local_state_after']['state_key']}",
        f"Account: {payload['broker_account']['account_id']}",
        f"Result: {payload['result']}",
        f"Event: {payload['signal']['event_id']}",
        f"Duplicate blocked: {'yes' if payload['duplicate_event_blocked'] else 'no'}",
        f"Pending submit claim: {'yes' if payload['event_claim_pending'] else 'no'}",
    ]
    submitted_orders = payload["submitted_orders"]
    if submitted_orders:
        lines.append("Submitted orders:")
        for order in submitted_orders:
            broker_status = order["broker_status"] or "-"
            broker_order_id = order["broker_order_id"] or "-"
            lines.append(
                f"- {order['side']} {order['quantity']} {order['symbol']} "
                f"broker_order_id={broker_order_id} status={broker_status}"
            )
    else:
        lines.append("Submitted orders: none")
    lines.append(f"State path: {payload['paths']['state_path']}")
    lines.append(f"Ledger path: {payload['paths']['ledger_path']}")
    if payload.get("event_receipt_path"):
        lines.append(f"Event receipt: {payload['event_receipt_path']}")
    if payload.get("event_claim_path"):
        lines.append(f"Event claim: {payload['event_claim_path']}")
    return "\n".join(lines)


def _render_positions_summary(positions: dict[str, int]) -> str:
    if not positions:
        return "CASH"
    return ", ".join(f"{symbol} {shares}" for symbol, shares in sorted(positions.items()))


def apply_ibkr_paper_signal(
    *,
    client: IbkrPaperClient,
    config: IbkrPaperClientConfig,
    allowed_symbols: set[str] | list[str] | tuple[str, ...],
    state_key: str = DEFAULT_IBKR_PAPER_STATE_KEY,
    base_dir: Path | None = None,
    signal_raw: dict[str, Any],
    source_kind: str,
    source_label: str,
    source_ref: str | None = None,
    data_dir: Path | None = None,
    timestamp: str | None = None,
    confirm_replies: bool = False,
) -> dict[str, Any]:
    resolved_timestamp = _timestamp(timestamp)
    normalized_allowed_symbols = _normalize_allowed_symbols(allowed_symbols)
    paths = resolve_ibkr_paper_paths(state_key=state_key, base_dir=base_dir, create=True)
    with _state_lock(paths):
        state_before = _load_or_create_state(paths=paths, state_key=state_key, timestamp=resolved_timestamp)
        status_before = _build_status_payload(
            timestamp=resolved_timestamp,
            paths=paths,
            state=state_before,
            signal_raw=signal_raw,
            source_kind=source_kind,
            source_label=source_label,
            source_ref=source_ref,
            data_dir=data_dir,
            allowed_symbols=normalized_allowed_symbols,
            config=config,
            client=client,
        )
        signal = parse_signal_payload(signal_raw)
        if status_before["execution_plan"]["blockers"]:
            blockers = ", ".join(status_before["execution_plan"]["blockers"])
            raise ValueError(f"IBKR paper apply refused because the reconciliation plan is blocked: {blockers}")

        duplicate = bool(status_before["event_already_applied"])
        pending_claim = status_before["pending_event_claim"]
        claim_path = _pending_claim_path(paths, signal.event_id)
        receipt_path = _event_receipt_path(paths, signal.event_id)
        local_state_after = state_before
        submitted_orders: list[dict[str, Any]] = []
        result = "applied"
        event_receipt_path: str | None = None
        event_claim_path: str | None = None

        if duplicate:
            result = "duplicate_event_refused"
        elif pending_claim is not None:
            result = "claim_pending_manual_clearance_required"
            event_claim_path = str(claim_path)
        else:
            initial_claim = {
                "account_id": config.account_id,
                "allowed_symbols": list(normalized_allowed_symbols),
                "broker_account": {
                    "account_id": config.account_id,
                    "base_url": config.base_url,
                    "timeout_seconds": config.timeout_seconds,
                    "verify_ssl": config.verify_ssl,
                },
                "created_at_chicago": resolved_timestamp.isoformat(),
                "event_id": signal.event_id,
                "result": "submit_claim_pending_manual_clearance_required",
                "schema_name": IBKR_PAPER_CLAIM_SCHEMA_NAME,
                "schema_version": IBKR_PAPER_CLAIM_SCHEMA_VERSION,
                "signal_action": signal.action,
                "signal_date": signal.date,
                "signal_symbol": signal.symbol,
                "source": {
                    "kind": source_kind,
                    "label": source_label,
                    "ref": source_ref,
                },
                "state_key": state_key,
                "strategy": signal.strategy,
                "submitted_orders": [],
            }
            _write_event_claim(paths, initial_claim)
            event_claim_path = str(claim_path)

            submission_candidates = _submission_candidates(plan_payload=status_before["execution_plan"])
            if not submission_candidates:
                result = "applied_noop"
                receipt_payload = {
                    "account_id": config.account_id,
                    "applied_at_chicago": resolved_timestamp.isoformat(),
                    "event_id": signal.event_id,
                    "orders": [],
                    "result": result,
                    "schema_name": IBKR_PAPER_EVENT_RECEIPT_SCHEMA_NAME,
                    "schema_version": IBKR_PAPER_EVENT_RECEIPT_SCHEMA_VERSION,
                    "signal_action": signal.action,
                    "signal_date": signal.date,
                    "signal_symbol": signal.symbol,
                    "state_key": state_key,
                    "strategy": signal.strategy,
                }
                _write_event_receipt(paths, receipt_payload)
                _remove_event_claim(paths, signal.event_id)
                event_receipt_path = str(receipt_path)
                event_claim_path = None
            else:
                try:
                    snapshot = BrokerSnapshot(
                        broker_name=status_before["broker_snapshot"]["broker_name"],
                        account_id=status_before["broker_snapshot"]["account_id"],
                        as_of=status_before["broker_snapshot"]["as_of"],
                        cash=status_before["broker_snapshot"]["cash"],
                        buying_power=status_before["broker_snapshot"]["buying_power"],
                        positions={
                            item["symbol"]: BrokerPosition(
                                symbol=item["symbol"],
                                shares=int(item["shares"]),
                                price=None if item["price"] is None else float(item["price"]),
                                instrument_type=item["instrument_type"],
                                underlying_symbol=item["underlying_symbol"],
                                raw={},
                            )
                            for item in status_before["broker_snapshot"]["positions"]
                        },
                        raw={},
                    )
                    for index, candidate in enumerate(submission_candidates, start=1):
                        contract = _resolve_order_contract(
                            symbol=candidate["symbol"],
                            snapshot=snapshot,
                            client=client,
                        )
                        order_ref = f"{signal.event_id}:{index}:{candidate['side']}:{candidate['symbol']}"
                        order_payload = {
                            "orders": [
                                {
                                    "acctId": config.account_id,
                                    "cOID": order_ref,
                                    "conid": int(contract["conid"]),
                                    "orderType": "MKT",
                                    "quantity": int(candidate["quantity"]),
                                    "side": candidate["side"],
                                    "tif": "DAY",
                                }
                            ]
                        }
                        normalized_response = _submit_order_with_replies(
                            account_id=config.account_id,
                            payload=order_payload,
                            client=client,
                            confirm_replies=confirm_replies,
                        )
                        if normalized_response["reply_required"]:
                            current_claim = _load_event_claim(paths, signal.event_id) or dict(initial_claim)
                            current_claim["reply_required"] = True
                            current_claim["result"] = "claim_pending_manual_clearance_required"
                            current_claim["submitted_orders"] = list(submitted_orders)
                            current_claim["pending_reply"] = {
                                "messages": normalized_response["reply_messages"],
                                "reply_id": normalized_response.get("reply_id"),
                            }
                            _write_event_claim(paths, current_claim)
                            result = "claim_pending_manual_clearance_required"
                            break

                        broker_order_id = normalized_response["broker_order_id"]
                        broker_status_payload = (
                            client.load_order_status(order_id=broker_order_id)
                            if broker_order_id is not None
                            else {}
                        )
                        submitted_order = {
                            "broker_order_id": broker_order_id,
                            "broker_status": normalized_response["broker_status"],
                            "classification": candidate["classification"],
                            "conid": int(contract["conid"]),
                            "contract_source": contract["source"],
                            "order_payload": order_payload["orders"][0],
                            "order_ref": order_ref,
                            "order_status": broker_status_payload,
                            "quantity": int(candidate["quantity"]),
                            "reference_price": candidate["reference_price"],
                            "side": candidate["side"],
                            "symbol": candidate["symbol"],
                        }
                        if normalized_response.get("reply_confirmations"):
                            submitted_order["reply_confirmations"] = normalized_response["reply_confirmations"]
                        submitted_orders.append(submitted_order)
                        current_claim = _load_event_claim(paths, signal.event_id) or dict(initial_claim)
                        current_claim["submitted_orders"] = submitted_orders
                        _write_event_claim(paths, current_claim)

                    if result != "claim_pending_manual_clearance_required":
                        receipt_payload = {
                            "account_id": config.account_id,
                            "applied_at_chicago": resolved_timestamp.isoformat(),
                            "event_id": signal.event_id,
                            "orders": submitted_orders,
                            "result": result,
                            "schema_name": IBKR_PAPER_EVENT_RECEIPT_SCHEMA_NAME,
                            "schema_version": IBKR_PAPER_EVENT_RECEIPT_SCHEMA_VERSION,
                            "signal_action": signal.action,
                            "signal_date": signal.date,
                            "signal_symbol": signal.symbol,
                            "state_key": state_key,
                            "strategy": signal.strategy,
                        }
                        _write_event_receipt(paths, receipt_payload)
                        _remove_event_claim(paths, signal.event_id)
                        event_receipt_path = str(receipt_path)
                        event_claim_path = None
                except Exception as exc:
                    current_claim = _load_event_claim(paths, signal.event_id) or dict(initial_claim)
                    current_claim["error"] = str(exc)
                    current_claim["result"] = "claim_pending_manual_clearance_required"
                    current_claim["submitted_orders"] = submitted_orders
                    _write_event_claim(paths, current_claim)
                    result = "claim_pending_manual_clearance_required"

        archive_manifest_path, archive_error = _archive_run(
            timestamp=resolved_timestamp,
            run_kind="ibkr_paper_lane_apply",
            mode="apply",
            label=state_key,
            identity_parts=[signal.event_id, config.account_id, result],
            manifest_fields={
                "account_id": config.account_id,
                "action": signal.action,
                "duplicate_event_blocked": duplicate,
                "event_claim_pending": result == "claim_pending_manual_clearance_required",
                "event_id": signal.event_id,
                "result": result,
                "signal_date": signal.date,
                "state_key": state_key,
                "strategy": signal.strategy,
            },
            json_artifacts={
                "ibkr_paper_apply_result": {
                    "archive_error": None,
                    "archive_manifest_path": None,
                    "broker_account": status_before["broker_account"],
                    "duplicate_event_blocked": duplicate,
                    "event_claim_path": event_claim_path,
                    "event_receipt_path": event_receipt_path,
                    "result": result,
                    "schema_name": IBKR_PAPER_APPLY_SCHEMA_NAME,
                    "schema_version": IBKR_PAPER_APPLY_SCHEMA_VERSION,
                    "signal": dict(signal_raw),
                    "source": {
                        "kind": source_kind,
                        "label": source_label,
                        "ref": source_ref,
                    },
                    "status_before_apply": status_before,
                    "submitted_orders": submitted_orders,
                },
                "ibkr_paper_status_before_apply": status_before,
                "signal_payload": signal_raw,
            },
            text_artifacts={
                "summary_text": render_ibkr_paper_apply_text(
                    {
                        "broker_account": status_before["broker_account"],
                        "duplicate_event_blocked": duplicate,
                        "event_claim_path": event_claim_path,
                        "event_claim_pending": result == "claim_pending_manual_clearance_required",
                        "event_receipt_path": event_receipt_path,
                        "local_state_after": _state_to_dict(state_before),
                        "paths": status_before["paths"],
                        "result": result,
                        "signal": dict(signal_raw),
                        "submitted_orders": submitted_orders,
                    }
                )
            },
        )

        last_attempt = {
            "archive_error": archive_error,
            "archive_manifest_path": archive_manifest_path,
            "event_id": signal.event_id,
            "event_receipt_path": event_receipt_path,
            "generated_at_chicago": resolved_timestamp.isoformat(),
            "result": result,
            "submitted_order_ids": [item["broker_order_id"] for item in submitted_orders if item["broker_order_id"]],
        }
        last_applied = (
            {
                "archive_error": archive_error,
                "archive_manifest_path": archive_manifest_path,
                "event_id": signal.event_id,
                "event_receipt_path": event_receipt_path,
                "generated_at_chicago": resolved_timestamp.isoformat(),
                "result": result,
                "submitted_order_ids": [item["broker_order_id"] for item in submitted_orders if item["broker_order_id"]],
            }
            if result in {"applied", "applied_noop"}
            else state_before.last_applied
        )
        local_state_after = _with_state_update(
            state=state_before,
            strategy=signal.strategy,
            account_id=config.account_id,
            allowed_symbols=normalized_allowed_symbols,
            timestamp=resolved_timestamp,
            last_attempt=last_attempt,
            last_applied=last_applied,
        )
        _write_ibkr_paper_state(paths, local_state_after)
        _append_jsonl_record(
            paths.ledger_path,
            record={
                "account_id": config.account_id,
                "archive_error": archive_error,
                "archive_manifest_path": archive_manifest_path,
                "duplicate_event_blocked": duplicate,
                "entry_kind": (
                    "duplicate_refused"
                    if duplicate
                    else "claim_pending"
                    if result == "claim_pending_manual_clearance_required"
                    else "apply"
                ),
                "event_claim_path": event_claim_path,
                "event_receipt_path": event_receipt_path,
                "event_id": signal.event_id,
                "generated_at_chicago": resolved_timestamp.isoformat(),
                "result": result,
                "schema_name": IBKR_PAPER_LEDGER_SCHEMA_NAME,
                "schema_version": IBKR_PAPER_LEDGER_SCHEMA_VERSION,
                "signal_action": signal.action,
                "signal_date": signal.date,
                "signal_symbol": signal.symbol,
                "state_key": state_key,
                "strategy": signal.strategy,
                "submitted_order_ids": [item["broker_order_id"] for item in submitted_orders if item["broker_order_id"]],
            },
        )

        payload = {
            "archive_error": archive_error,
            "archive_manifest_path": archive_manifest_path,
            "broker_account": status_before["broker_account"],
            "duplicate_event_blocked": duplicate,
            "event_claim_path": event_claim_path,
            "event_claim_pending": result == "claim_pending_manual_clearance_required",
            "event_receipt_path": event_receipt_path,
            "generated_at_chicago": resolved_timestamp.isoformat(),
            "local_state_after": _state_to_dict(local_state_after),
            "local_state_before": _state_to_dict(state_before),
            "paths": status_before["paths"],
            "result": result,
            "schema_name": IBKR_PAPER_APPLY_SCHEMA_NAME,
            "schema_version": IBKR_PAPER_APPLY_SCHEMA_VERSION,
            "signal": dict(signal_raw),
            "source": {
                "kind": source_kind,
                "label": source_label,
                "ref": source_ref,
            },
            "status_before_apply": status_before,
            "submitted_orders": submitted_orders,
        }
        return payload
