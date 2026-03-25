from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, MutableMapping, Protocol
from urllib.parse import urlparse

from trading_codex.execution.broker import (
    RequestsTastytradeHttpClient,
    build_tastytrade_equity_order_payload,
    normalize_tastytrade_snapshot,
)
from trading_codex.execution.planner import (
    build_execution_plan,
    build_live_submission_preview,
    build_order_intent_export,
    build_simulated_submission_export,
)
from trading_codex.execution.secrets import load_tastytrade_sandbox_secrets
from trading_codex.execution.signals import expected_event_id, parse_signal_payload


TASTYTRADE_SANDBOX_CAPABILITY_SCHEMA_NAME = "tastytrade_sandbox_capability"
TASTYTRADE_SANDBOX_CAPABILITY_SCHEMA_VERSION = 1
DEFAULT_TASTYTRADE_SANDBOX_INSTRUMENT_PATHS = (
    "/instruments/equities?symbol={symbol}",
    "/instruments/equities/{symbol}",
)
DEFAULT_TASTYTRADE_SANDBOX_QUOTE_PATHS = (
    "/market-data/by-symbol/{symbol}",
    "/market-data/quotes/{symbol}",
    "/market-data/quotes?symbol={symbol}",
    "/market-data/quotes?symbols={symbol}",
)
DEFAULT_TASTYTRADE_SANDBOX_ACCOUNT_DISCOVERY_PATHS = ("/customers/me/accounts",)
DEFAULT_TASTYTRADE_SANDBOX_CANCEL_PATHS = (
    ("DELETE", "/accounts/{account_id}/orders/{order_id}"),
    ("POST", "/accounts/{account_id}/orders/{order_id}/cancel"),
)
CONFIRMED_TASTYTRADE_SANDBOX_HOSTS = frozenset({"api.cert.tastytrade.com"})
DEFAULT_TASTYTRADE_SANDBOX_DEFENSIVE_SYMBOLS = frozenset({"BIL", "IEF", "SGOV", "SHY", "TLT"})


class TastytradeSandboxCapabilityClient(Protocol):
    def get_balances(self, *, account_id: str) -> Any:
        ...

    def get_positions(self, *, account_id: str) -> Any:
        ...

    def place_order(self, *, account_id: str, payload: dict[str, Any]) -> Any:
        ...

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        ...

    def post_json(self, path: str, *, payload: dict[str, Any] | None = None) -> Any:
        ...

    def delete_json(self, path: str) -> Any:
        ...


@dataclass(frozen=True)
class TastytradeSandboxConfig:
    account_id: str | None
    access_token: str | None
    base_url: str
    challenge_code: str | None
    challenge_token: str | None
    password: str | None
    secrets_file_path: str | None
    session_token: str | None
    timeout_seconds: float
    username: str | None


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _step(
    *,
    status: str,
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "blockers": list(blockers or []),
        "warnings": list(warnings or []),
        "details": details or {},
    }


def _blocked_step(reason: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return _step(status="blocked", blockers=[reason], details=details)


def _json_excerpt(payload: object, *, limit: int = 600) -> str:
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if len(rendered) <= limit:
        return rendered
    return rendered[: limit - 3] + "..."


def _host_is_sandbox(base_url: str) -> bool:
    host = (urlparse(base_url).hostname or "").lower()
    return host in CONFIRMED_TASTYTRADE_SANDBOX_HOSTS


def load_tastytrade_sandbox_config(
    *,
    secrets_file: Path | None = None,
    environ: MutableMapping[str, str] | None = None,
) -> TastytradeSandboxConfig:
    target_env = dict(os.environ if environ is None else environ)
    loaded_path = load_tastytrade_sandbox_secrets(secrets_file=secrets_file, environ=target_env)

    account_id = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_ACCOUNT"))
    username = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_USERNAME"))
    password = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_PASSWORD"))
    session_token = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_SESSION_TOKEN"))
    access_token = _normalize_text(
        target_env.get("TASTYTRADE_SANDBOX_ACCESS_TOKEN") or target_env.get("TASTYTRADE_SANDBOX_API_TOKEN")
    )
    challenge_code = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_CHALLENGE_CODE"))
    challenge_token = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_CHALLENGE_TOKEN"))
    base_url = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_API_BASE_URL"))
    timeout_raw = _normalize_text(target_env.get("TASTYTRADE_SANDBOX_TIMEOUT_SECONDS"))

    missing: list[str] = []
    if base_url is None:
        missing.append("TASTYTRADE_SANDBOX_API_BASE_URL")

    has_auth = bool(access_token or session_token or (username and password))
    if not has_auth:
        missing.append(
            "one of TASTYTRADE_SANDBOX_ACCESS_TOKEN, TASTYTRADE_SANDBOX_SESSION_TOKEN, "
            "or TASTYTRADE_SANDBOX_USERNAME/TASTYTRADE_SANDBOX_PASSWORD"
        )
    if username and not password:
        missing.append("TASTYTRADE_SANDBOX_PASSWORD")
    if password and not username:
        missing.append("TASTYTRADE_SANDBOX_USERNAME")
    if missing:
        raise ValueError("Missing sandbox config: " + ", ".join(missing))

    timeout_seconds = 30.0
    if timeout_raw is not None:
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError as exc:
            raise ValueError("TASTYTRADE_SANDBOX_TIMEOUT_SECONDS must be numeric.") from exc
        if timeout_seconds <= 0:
            raise ValueError("TASTYTRADE_SANDBOX_TIMEOUT_SECONDS must be > 0.")

    assert base_url is not None
    return TastytradeSandboxConfig(
        account_id=account_id,
        access_token=access_token,
        base_url=base_url.rstrip("/"),
        challenge_code=challenge_code,
        challenge_token=challenge_token,
        password=password,
        secrets_file_path=None if loaded_path is None else str(loaded_path),
        session_token=session_token,
        timeout_seconds=timeout_seconds,
        username=username,
    )


def build_tastytrade_sandbox_client(
    config: TastytradeSandboxConfig,
) -> RequestsTastytradeHttpClient:
    return RequestsTastytradeHttpClient(
        access_token=config.access_token,
        base_url=config.base_url,
        challenge_code=config.challenge_code,
        challenge_token=config.challenge_token,
        password=config.password,
        session_token=config.session_token,
        timeout=config.timeout_seconds,
        username=config.username,
    )


def _extract_account_ids(payload: object) -> list[str]:
    found: set[str] = set()

    def walk(value: object) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                lowered = key.lower().replace("_", "-")
                if lowered in {"account-id", "account-number", "account-number-short"}:
                    normalized = _normalize_text(item)
                    if normalized is not None:
                        found.add(normalized)
                walk(item)
            return
        if isinstance(value, list):
            for item in value:
                walk(item)

    walk(payload)
    return sorted(found)


def _payload_has_content(payload: object) -> bool:
    if isinstance(payload, dict):
        if not payload:
            return False
        return any(_payload_has_content(value) for value in payload.values())
    if isinstance(payload, list):
        return any(_payload_has_content(item) for item in payload)
    if payload is None:
        return False
    if isinstance(payload, str):
        return payload.strip() != ""
    return True


def _payload_mentions_symbol(payload: object, symbol: str) -> bool:
    target = symbol.strip().upper()
    if target == "":
        return False

    def walk(value: object) -> bool:
        if isinstance(value, dict):
            for key, item in value.items():
                lowered = key.lower().replace("_", "-")
                if lowered in {"symbol", "underlying-symbol", "ticker"}:
                    normalized = _normalize_text(item)
                    if normalized is not None and normalized.upper() == target:
                        return True
                if walk(item):
                    return True
            return False
        if isinstance(value, list):
            return any(walk(item) for item in value)
        return False

    return walk(payload)


def _quote_price_from_payload(payload: object) -> float | None:
    quote_keys = {
        "ask",
        "ask-price",
        "bid",
        "bid-price",
        "last",
        "last-price",
        "mark",
        "mark-price",
        "mid",
        "mid-price",
        "price",
    }

    def walk(value: object) -> float | None:
        if isinstance(value, dict):
            for key, item in value.items():
                lowered = key.lower().replace("_", "-")
                if lowered in quote_keys:
                    normalized = _normalize_text(item)
                    if normalized is None:
                        continue
                    try:
                        return float(normalized)
                    except ValueError:
                        continue
                nested = walk(item)
                if nested is not None:
                    return nested
            return None
        if isinstance(value, list):
            for item in value:
                nested = walk(item)
                if nested is not None:
                    return nested
        return None

    return walk(payload)


def _extract_order_submission_fields(payload: object) -> tuple[str | None, str | None]:
    order_id: str | None = None
    status: str | None = None

    def walk(value: object) -> None:
        nonlocal order_id, status
        if isinstance(value, dict):
            for key, item in value.items():
                lowered = key.lower().replace("_", "-")
                if order_id is None and lowered in {"id", "order-id"}:
                    order_id = _normalize_text(item)
                if status is None and lowered in {"status", "order-status"}:
                    status = _normalize_text(item)
                walk(item)
            return
        if isinstance(value, list):
            for item in value:
                walk(item)

    walk(payload)
    return order_id, status


def _probe_accounts(
    client: TastytradeSandboxCapabilityClient,
) -> tuple[list[str], list[dict[str, Any]], str | None]:
    attempts: list[dict[str, Any]] = []
    for path in DEFAULT_TASTYTRADE_SANDBOX_ACCOUNT_DISCOVERY_PATHS:
        try:
            payload = client.get_json(path)
        except Exception as exc:
            attempts.append(
                {
                    "method": "GET",
                    "path": path,
                    "status": "fail",
                    "error": str(exc),
                }
            )
            continue
        discovered = _extract_account_ids(payload)
        attempt = {
            "method": "GET",
            "path": path,
            "status": "pass" if discovered else "fail",
            "account_ids": discovered,
            "response_excerpt": _json_excerpt(payload),
        }
        attempts.append(attempt)
        if discovered:
            return discovered, attempts, None
    if not attempts:
        return [], [], "sandbox_account_discovery_not_attempted"
    last_error = attempts[-1].get("error")
    return [], attempts, None if not last_error else str(last_error)


def _select_account(
    *,
    configured_account_id: str | None,
    discovered_account_ids: list[str],
    discovery_error: str | None,
) -> tuple[str | None, dict[str, Any]]:
    blockers: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {
        "configured_account_id": configured_account_id,
        "discovered_account_ids": list(discovered_account_ids),
    }
    selected_account_id: str | None = None
    source: str | None = None

    if configured_account_id is not None:
        source = "explicit_config"
        if discovered_account_ids:
            if configured_account_id in discovered_account_ids:
                selected_account_id = configured_account_id
            else:
                blockers.append(f"sandbox_account_not_found_in_discovery:{configured_account_id}")
        elif discovery_error is None:
            blockers.append(f"sandbox_account_not_found_in_discovery:{configured_account_id}")
        else:
            selected_account_id = configured_account_id
            warnings.append("sandbox_account_discovery_unavailable_using_explicit_account")
            details["discovery_error"] = discovery_error
    else:
        if not discovered_account_ids:
            if discovery_error is None:
                blockers.append("sandbox_account_discovery_empty")
            else:
                blockers.append("sandbox_account_discovery_required_without_explicit_account")
                details["discovery_error"] = discovery_error
        elif len(discovered_account_ids) > 1:
            blockers.append("sandbox_account_discovery_ambiguous:" + ",".join(discovered_account_ids))
        else:
            selected_account_id = discovered_account_ids[0]
            source = "discovered_single_account"

    details["selection_source"] = source
    details["selected_account_id"] = selected_account_id
    return selected_account_id, _step(
        status="pass" if not blockers and selected_account_id is not None else "fail",
        blockers=blockers,
        warnings=warnings,
        details=details,
    )


def _probe_symbol_lookup(
    *,
    client: TastytradeSandboxCapabilityClient,
    symbols: list[str],
    path_templates: tuple[str, ...],
    capability: str,
    quote_mode: bool,
) -> tuple[dict[str, Any], dict[str, float | None]]:
    symbol_results: list[dict[str, Any]] = []
    blockers: list[str] = []
    prices: dict[str, float | None] = {}

    for symbol in symbols:
        attempts: list[dict[str, Any]] = []
        resolved_path: str | None = None
        response_excerpt: str | None = None
        resolved_price: float | None = None
        for path_template in path_templates:
            path = path_template.format(symbol=symbol)
            try:
                payload = client.get_json(path)
            except Exception as exc:
                attempts.append(
                    {
                        "method": "GET",
                        "path": path,
                        "status": "fail",
                        "error": str(exc),
                    }
                )
                continue

            resolved_price = _quote_price_from_payload(payload) if quote_mode else None
            success = _payload_mentions_symbol(payload, symbol) or _payload_has_content(payload)
            if quote_mode and resolved_price is None:
                success = False
            attempt = {
                "method": "GET",
                "path": path,
                "status": "pass" if success else "fail",
                "response_excerpt": _json_excerpt(payload),
            }
            if resolved_price is not None:
                attempt["resolved_price"] = resolved_price
            attempts.append(attempt)
            if success:
                resolved_path = path
                response_excerpt = attempt["response_excerpt"]
                break

        symbol_status = "pass" if resolved_path is not None else "fail"
        if resolved_path is None:
            blockers.append(f"{capability}_lookup_failed:{symbol}")
        prices[symbol] = resolved_price
        symbol_results.append(
            {
                "symbol": symbol,
                "status": symbol_status,
                "resolved_path": resolved_path,
                "response_excerpt": response_excerpt,
                "resolved_price": resolved_price,
                "attempts": attempts,
            }
        )

    return (
        _step(
            status="pass" if not blockers else "fail",
            blockers=blockers,
            details={
                "path_templates": list(path_templates),
                "symbols": symbol_results,
            },
        ),
        prices,
    )


def _resolve_probe_symbol(
    *,
    configured_symbol: str | None,
    symbols: list[str],
    current_positions: dict[str, Any],
) -> str:
    if configured_symbol is not None:
        return configured_symbol

    preferred = [
        symbol
        for symbol in symbols
        if symbol not in DEFAULT_TASTYTRADE_SANDBOX_DEFENSIVE_SYMBOLS
    ]
    candidates = preferred or list(symbols)
    for symbol in candidates:
        position = current_positions.get(symbol)
        if position is None or getattr(position, "shares", 0) == 0:
            return symbol
    return candidates[0]


def _build_probe_signal(
    *,
    symbol: str,
    current_shares: int,
    price: float | None,
    quantity: int,
    timestamp: datetime,
) -> dict[str, Any]:
    if quantity <= 0:
        raise ValueError("probe_order_qty must be > 0.")
    signal = {
        "schema_name": "next_action",
        "date": timestamp.date().isoformat(),
        "strategy": "tastytrade_sandbox_capability",
        "action": "BUY",
        "symbol": symbol,
        "price": price,
        "target_shares": current_shares + quantity,
        "resize_prev_shares": current_shares,
        "resize_new_shares": None,
        "next_rebalance": timestamp.date().isoformat(),
    }
    signal["event_id"] = expected_event_id(signal)
    return signal


def _build_order_steps(
    *,
    broker_snapshot: Any,
    symbols: list[str],
    preset_name: str | None,
    probe_order_symbol: str | None,
    probe_order_qty: int,
    quote_prices: dict[str, float | None],
    timestamp: datetime,
) -> tuple[dict[str, Any], dict[str, Any], Any | None]:
    selected_symbol = _resolve_probe_symbol(
        configured_symbol=probe_order_symbol,
        symbols=symbols,
        current_positions=broker_snapshot.positions,
    )
    current_shares = broker_snapshot.positions.get(selected_symbol).shares if selected_symbol in broker_snapshot.positions else 0
    price = quote_prices.get(selected_symbol)
    if price is None and selected_symbol in broker_snapshot.positions:
        price = broker_snapshot.positions[selected_symbol].price

    signal_raw = _build_probe_signal(
        symbol=selected_symbol,
        current_shares=current_shares,
        price=price,
        quantity=probe_order_qty,
        timestamp=timestamp,
    )
    signal = parse_signal_payload(signal_raw)
    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker_snapshot,
        account_scope="managed_sleeve",
        managed_symbols=set(symbols),
        ack_unmanaged_holdings=True,
        source_kind="sandbox_capability",
        source_label=preset_name or "explicit_symbols",
        source_ref=None,
        broker_source_ref=f"tastytrade:{broker_snapshot.account_id}",
        data_dir=None,
        generated_at=timestamp,
    )
    preview = build_live_submission_preview(plan)
    candidate_orders = list(preview.get("candidate_orders", []))
    order_construction_step = _step(
        status="pass" if candidate_orders else "fail",
        blockers=[] if candidate_orders else ["sandbox_order_construction_candidate_orders_empty"],
        details={
            "plan_blockers": list(plan.blockers),
            "plan_warnings": list(plan.warnings),
            "probe_signal": signal_raw,
            "selected_symbol": selected_symbol,
            "preview": preview,
        },
    )

    try:
        order_intent_export = build_order_intent_export(plan)
        simulated_export = build_simulated_submission_export(order_intent_export)
        if not simulated_export.orders:
            raise ValueError("sandbox_order_preview_orders_empty")
    except Exception as exc:
        return (
            order_construction_step,
            _step(
                status="fail",
                blockers=[str(exc)],
                details={
                    "plan_blockers": list(plan.blockers),
                    "plan_warnings": list(plan.warnings),
                    "preview": preview,
                },
            ),
            None,
        )

    return (
        order_construction_step,
        _step(
            status="pass",
            details={
                "preview": preview,
                "simulated_order": asdict(simulated_export.orders[0]),
            },
        ),
        simulated_export,
    )


def _submit_guard_blockers(
    *,
    config: TastytradeSandboxConfig,
    selected_account_id: str | None,
    sandbox_submit_account: str | None,
    simulated_export: Any | None,
) -> list[str]:
    blockers: list[str] = []
    if not _host_is_sandbox(config.base_url):
        blockers.append("sandbox_submit_requires_sandbox_host")
    if selected_account_id is None:
        blockers.append("sandbox_submit_requires_selected_account")
    if sandbox_submit_account is None:
        blockers.append("sandbox_submit_requires_account_confirmation")
    elif selected_account_id is not None and sandbox_submit_account != selected_account_id:
        blockers.append(
            f"sandbox_submit_account_confirmation_mismatch:{sandbox_submit_account}:{selected_account_id}"
        )
    if simulated_export is None or not getattr(simulated_export, "orders", None):
        blockers.append("sandbox_submit_requires_preview_order")
    return blockers


def _submit_sandbox_order(
    *,
    client: TastytradeSandboxCapabilityClient,
    config: TastytradeSandboxConfig,
    selected_account_id: str | None,
    sandbox_submit_account: str | None,
    simulated_export: Any | None,
    enable_submit: bool,
) -> tuple[dict[str, Any], str | None]:
    if not enable_submit:
        return _blocked_step(
            "sandbox_submit_disabled_by_default",
            details={"base_url": config.base_url},
        ), None

    blockers = _submit_guard_blockers(
        config=config,
        selected_account_id=selected_account_id,
        sandbox_submit_account=sandbox_submit_account,
        simulated_export=simulated_export,
    )
    if blockers:
        return _step(
            status="blocked",
            blockers=blockers,
            details={"base_url": config.base_url},
        ), None

    assert simulated_export is not None
    first_order = simulated_export.orders[0]
    request_payload = build_tastytrade_equity_order_payload(first_order)
    try:
        raw_response = client.place_order(account_id=selected_account_id or "", payload=request_payload)
    except Exception as exc:
        return _step(
            status="fail",
            blockers=[str(exc)],
            details={
                "request_payload": request_payload,
            },
        ), None

    order_id, broker_status = _extract_order_submission_fields(raw_response)
    return (
        _step(
            status="pass",
            details={
                "submitted_order": asdict(first_order),
                "request_payload": request_payload,
                "broker_order_id": order_id,
                "broker_status": broker_status,
                "response_excerpt": _json_excerpt(raw_response),
            },
        ),
        order_id,
    )


def _cancel_sandbox_order(
    *,
    client: TastytradeSandboxCapabilityClient,
    account_id: str | None,
    order_id: str | None,
    cancel_after_submit: bool,
) -> dict[str, Any]:
    if not cancel_after_submit:
        return _blocked_step("sandbox_cancel_not_requested")
    if account_id is None or order_id is None:
        return _blocked_step(
            "sandbox_cancel_requires_submitted_order",
            details={"account_id": account_id, "order_id": order_id},
        )

    attempts: list[dict[str, Any]] = []
    for method, path_template in DEFAULT_TASTYTRADE_SANDBOX_CANCEL_PATHS:
        path = path_template.format(account_id=account_id, order_id=order_id)
        try:
            if method == "DELETE":
                payload = client.delete_json(path)
            else:
                payload = client.post_json(path, payload={})
        except Exception as exc:
            attempts.append(
                {
                    "method": method,
                    "path": path,
                    "status": "fail",
                    "error": str(exc),
                }
            )
            continue
        return _step(
            status="pass",
            details={
                "attempts": attempts
                + [
                    {
                        "method": method,
                        "path": path,
                        "status": "pass",
                        "response_excerpt": _json_excerpt(payload),
                    }
                ],
                "broker_order_id": order_id,
            },
        )

    return _step(
        status="fail",
        blockers=[f"sandbox_cancel_failed:{order_id}"],
        details={
            "attempts": attempts,
            "broker_order_id": order_id,
        },
    )


def _pre_submit_status(capability_matrix: dict[str, dict[str, Any]]) -> str:
    required = (
        "auth",
        "account_discovery_selection",
        "balances",
        "positions",
        "instrument_lookup",
        "quote_lookup",
        "order_construction",
        "order_preview",
    )
    statuses = [capability_matrix[name]["status"] for name in required]
    if any(status == "fail" for status in statuses):
        return "fail"
    if any(status == "blocked" for status in statuses):
        return "blocked"
    return "pass"


def _mutation_status(capability_matrix: dict[str, dict[str, Any]]) -> str:
    statuses = [
        capability_matrix["sandbox_submit"]["status"],
        capability_matrix["sandbox_cancel"]["status"],
    ]
    if any(status == "fail" for status in statuses):
        return "fail"
    if any(status == "pass" for status in statuses):
        if all(status in {"pass", "blocked"} for status in statuses):
            return "pass"
    return "blocked"


def _build_tastytrade_sandbox_capability_report(
    *,
    generated_at: datetime,
    preset_name: str | None,
    normalized_symbols: list[str],
    config: TastytradeSandboxConfig,
    probe_order_qty: int,
    probe_order_symbol: str | None,
    enable_submit: bool,
    cancel_after_submit: bool,
    sandbox_submit_account: str | None,
    capability_matrix: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    pre_submit_status = _pre_submit_status(capability_matrix)
    mutation_status = _mutation_status(capability_matrix)
    overall_status = "pass" if pre_submit_status == "pass" and mutation_status in {"pass", "blocked"} else (
        "fail" if "fail" in {pre_submit_status, mutation_status} else "blocked"
    )

    return {
        "schema_name": TASTYTRADE_SANDBOX_CAPABILITY_SCHEMA_NAME,
        "schema_version": TASTYTRADE_SANDBOX_CAPABILITY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "preset": preset_name,
        "symbols": normalized_symbols,
        "config": {
            "account_id": config.account_id,
            "base_url": config.base_url,
            "host_is_sandbox": _host_is_sandbox(config.base_url),
            "secrets_file_path": config.secrets_file_path,
            "timeout_seconds": config.timeout_seconds,
            "uses_access_token": config.access_token is not None,
            "uses_session_token": config.session_token is not None,
            "uses_username_password": bool(config.username and config.password),
        },
        "controls": {
            "enable_sandbox_submit": enable_submit,
            "cancel_after_submit": cancel_after_submit,
            "sandbox_submit_account": _normalize_text(sandbox_submit_account),
            "probe_order_qty": probe_order_qty,
            "probe_order_symbol": _normalize_text(probe_order_symbol),
        },
        "summary": {
            "overall_status": overall_status,
            "pre_submit_status": pre_submit_status,
            "mutation_status": mutation_status,
            "passing_capabilities": [
                name for name, payload in capability_matrix.items() if payload["status"] == "pass"
            ],
            "blocked_capabilities": [
                name for name, payload in capability_matrix.items() if payload["status"] == "blocked"
            ],
            "failing_capabilities": [
                name for name, payload in capability_matrix.items() if payload["status"] == "fail"
            ],
        },
        "capability_matrix": capability_matrix,
    }


def run_tastytrade_sandbox_capability(
    *,
    symbols: list[str],
    preset_name: str | None = None,
    secrets_file: Path | None = None,
    explicit_account_id: str | None = None,
    probe_order_symbol: str | None = None,
    probe_order_qty: int = 1,
    enable_submit: bool = False,
    sandbox_submit_account: str | None = None,
    cancel_after_submit: bool = False,
    timestamp: datetime | None = None,
    client: TastytradeSandboxCapabilityClient | None = None,
    environ: MutableMapping[str, str] | None = None,
    challenge_code: str | None = None,
    challenge_token: str | None = None,
) -> dict[str, Any]:
    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()})
    if not normalized_symbols:
        raise ValueError("At least one sandbox capability symbol is required.")

    generated_at = (timestamp or datetime.now()).replace(microsecond=0)
    config = load_tastytrade_sandbox_config(secrets_file=secrets_file, environ=environ)
    if explicit_account_id is not None:
        config = TastytradeSandboxConfig(
            account_id=_normalize_text(explicit_account_id),
            access_token=config.access_token,
            base_url=config.base_url,
            challenge_code=challenge_code or config.challenge_code,
            challenge_token=challenge_token or config.challenge_token,
            password=config.password,
            secrets_file_path=config.secrets_file_path,
            session_token=config.session_token,
            timeout_seconds=config.timeout_seconds,
            username=config.username,
        )
    elif challenge_code is not None or challenge_token is not None:
        config = TastytradeSandboxConfig(
            account_id=config.account_id,
            access_token=config.access_token,
            base_url=config.base_url,
            challenge_code=challenge_code or config.challenge_code,
            challenge_token=challenge_token or config.challenge_token,
            password=config.password,
            secrets_file_path=config.secrets_file_path,
            session_token=config.session_token,
            timeout_seconds=config.timeout_seconds,
            username=config.username,
        )

    if not _host_is_sandbox(config.base_url):
        host_details = {"base_url": config.base_url}
        capability_matrix = {
            "account_discovery_selection": _step(
                status="fail",
                blockers=["sandbox_host_not_confirmed"],
                details={
                    "configured_account_id": config.account_id,
                    "discovered_account_ids": [],
                    "discovery_attempts": [],
                    "selection_source": None,
                    "selected_account_id": None,
                    **host_details,
                },
            ),
            "auth": _step(
                status="fail",
                blockers=["sandbox_host_not_confirmed"],
                details=host_details,
            ),
            "balances": _blocked_step("sandbox_host_not_confirmed", details=host_details),
            "positions": _blocked_step("sandbox_host_not_confirmed", details=host_details),
            "instrument_lookup": _blocked_step("sandbox_host_not_confirmed", details=host_details),
            "quote_lookup": _blocked_step("sandbox_host_not_confirmed", details=host_details),
            "order_construction": _blocked_step("sandbox_host_not_confirmed", details=host_details),
            "order_preview": _blocked_step("sandbox_host_not_confirmed", details=host_details),
            "sandbox_submit": _step(
                status="blocked",
                blockers=["sandbox_submit_requires_sandbox_host"]
                if enable_submit
                else ["sandbox_submit_disabled_by_default"],
                details=host_details,
            ),
            "sandbox_cancel": _blocked_step(
                "sandbox_cancel_requires_submitted_order"
                if cancel_after_submit
                else "sandbox_cancel_not_requested",
                details={"account_id": None, "order_id": None, **host_details}
                if cancel_after_submit
                else host_details,
            ),
        }
        return _build_tastytrade_sandbox_capability_report(
            generated_at=generated_at,
            preset_name=preset_name,
            normalized_symbols=normalized_symbols,
            config=config,
            probe_order_qty=probe_order_qty,
            probe_order_symbol=probe_order_symbol,
            enable_submit=enable_submit,
            cancel_after_submit=cancel_after_submit,
            sandbox_submit_account=sandbox_submit_account,
            capability_matrix=capability_matrix,
        )

    sandbox_client = client or build_tastytrade_sandbox_client(config)
    discovered_account_ids, account_attempts, discovery_error = _probe_accounts(sandbox_client)
    selected_account_id, account_step = _select_account(
        configured_account_id=config.account_id,
        discovered_account_ids=discovered_account_ids,
        discovery_error=discovery_error,
    )
    account_step["details"]["discovery_attempts"] = account_attempts

    capability_matrix: dict[str, dict[str, Any]] = {}
    capability_matrix["account_discovery_selection"] = account_step

    balances_payload: object | None = None
    positions_payload: object | None = None
    broker_snapshot: Any | None = None

    if selected_account_id is None:
        capability_matrix["auth"] = _step(
            status="fail",
            blockers=["sandbox_auth_not_attempted_without_selected_account"],
            details={"base_url": config.base_url},
        )
        capability_matrix["balances"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["positions"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["instrument_lookup"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["quote_lookup"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["order_construction"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["order_preview"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["sandbox_submit"] = _blocked_step("sandbox_account_selection_failed")
        capability_matrix["sandbox_cancel"] = _blocked_step("sandbox_account_selection_failed")
    else:
        auth_error: str | None = None
        try:
            balances_payload = sandbox_client.get_balances(account_id=selected_account_id)
            capability_matrix["balances"] = _step(
                status="pass",
                details={"response_excerpt": _json_excerpt(balances_payload)},
            )
        except Exception as exc:
            auth_error = str(exc)
            capability_matrix["balances"] = _step(status="fail", blockers=[str(exc)])

        try:
            positions_payload = sandbox_client.get_positions(account_id=selected_account_id)
            capability_matrix["positions"] = _step(
                status="pass",
                details={"response_excerpt": _json_excerpt(positions_payload)},
            )
            auth_error = None
        except Exception as exc:
            if auth_error is None:
                auth_error = str(exc)
            capability_matrix["positions"] = _step(status="fail", blockers=[str(exc)])

        if capability_matrix["balances"]["status"] == "pass" or capability_matrix["positions"]["status"] == "pass":
            capability_matrix["auth"] = _step(
                status="pass",
                details={
                    "account_id": selected_account_id,
                    "base_url": config.base_url,
                },
            )
        else:
            capability_matrix["auth"] = _step(
                status="fail",
                blockers=[] if auth_error is None else [auth_error],
                details={"base_url": config.base_url},
            )

        if capability_matrix["balances"]["status"] == "pass" and capability_matrix["positions"]["status"] == "pass":
            try:
                broker_snapshot = normalize_tastytrade_snapshot(
                    account_id=selected_account_id,
                    balances_payload=balances_payload,
                    positions_payload=positions_payload,
                )
            except Exception as exc:
                error = str(exc)
                capability_matrix["balances"] = _step(status="fail", blockers=[error])
                capability_matrix["positions"] = _step(status="fail", blockers=[error])

        instrument_step, _instrument_prices = _probe_symbol_lookup(
            client=sandbox_client,
            symbols=normalized_symbols,
            path_templates=DEFAULT_TASTYTRADE_SANDBOX_INSTRUMENT_PATHS,
            capability="instrument",
            quote_mode=False,
        )
        capability_matrix["instrument_lookup"] = instrument_step

        quote_step, quote_prices = _probe_symbol_lookup(
            client=sandbox_client,
            symbols=normalized_symbols,
            path_templates=DEFAULT_TASTYTRADE_SANDBOX_QUOTE_PATHS,
            capability="quote",
            quote_mode=True,
        )
        capability_matrix["quote_lookup"] = quote_step

        if broker_snapshot is None:
            capability_matrix["order_construction"] = _blocked_step("sandbox_broker_snapshot_unavailable")
            capability_matrix["order_preview"] = _blocked_step("sandbox_broker_snapshot_unavailable")
            capability_matrix["sandbox_submit"] = _blocked_step("sandbox_broker_snapshot_unavailable")
            capability_matrix["sandbox_cancel"] = _blocked_step("sandbox_broker_snapshot_unavailable")
        else:
            capability_matrix["balances"] = _step(
                status=capability_matrix["balances"]["status"],
                blockers=capability_matrix["balances"]["blockers"],
                warnings=capability_matrix["balances"]["warnings"],
                details={
                    "cash": broker_snapshot.cash,
                    "buying_power": broker_snapshot.buying_power,
                    "response_excerpt": capability_matrix["balances"]["details"].get("response_excerpt"),
                },
            )
            capability_matrix["positions"] = _step(
                status=capability_matrix["positions"]["status"],
                blockers=capability_matrix["positions"]["blockers"],
                warnings=capability_matrix["positions"]["warnings"],
                details={
                    "account_id": broker_snapshot.account_id,
                    "as_of": broker_snapshot.as_of,
                    "position_count": len(broker_snapshot.positions),
                    "positions": [
                        {
                            "symbol": position.symbol,
                            "shares": position.shares,
                            "price": position.price,
                            "instrument_type": position.instrument_type,
                            "underlying_symbol": position.underlying_symbol,
                        }
                        for _, position in sorted(broker_snapshot.positions.items())
                    ],
                },
            )

            order_construction_step, order_preview_step, simulated_export = _build_order_steps(
                broker_snapshot=broker_snapshot,
                symbols=normalized_symbols,
                preset_name=preset_name,
                probe_order_symbol=_normalize_text(probe_order_symbol),
                probe_order_qty=probe_order_qty,
                quote_prices=quote_prices,
                timestamp=generated_at,
            )
            capability_matrix["order_construction"] = order_construction_step
            capability_matrix["order_preview"] = order_preview_step

            submit_step, submitted_order_id = _submit_sandbox_order(
                client=sandbox_client,
                config=config,
                selected_account_id=selected_account_id,
                sandbox_submit_account=_normalize_text(sandbox_submit_account),
                simulated_export=simulated_export,
                enable_submit=enable_submit,
            )
            capability_matrix["sandbox_submit"] = submit_step
            capability_matrix["sandbox_cancel"] = _cancel_sandbox_order(
                client=sandbox_client,
                account_id=selected_account_id,
                order_id=submitted_order_id,
                cancel_after_submit=cancel_after_submit,
            )

    return _build_tastytrade_sandbox_capability_report(
        generated_at=generated_at,
        preset_name=preset_name,
        normalized_symbols=normalized_symbols,
        config=config,
        probe_order_qty=probe_order_qty,
        probe_order_symbol=probe_order_symbol,
        enable_submit=enable_submit,
        cancel_after_submit=cancel_after_submit,
        sandbox_submit_account=sandbox_submit_account,
        capability_matrix=capability_matrix,
    )


def render_tastytrade_sandbox_capability_report(report: dict[str, Any]) -> str:
    lines = [
        "Tastytrade Sandbox Capability",
        f"Generated: {report.get('generated_at')}",
        f"Preset: {report.get('preset') or '-'}",
        f"Symbols: {', '.join(report.get('symbols', []))}",
        f"Base URL: {report.get('config', {}).get('base_url') or '-'}",
        f"Selected account: {report.get('capability_matrix', {}).get('account_discovery_selection', {}).get('details', {}).get('selected_account_id') or '-'}",
        f"Pre-submit status: {report.get('summary', {}).get('pre_submit_status')}",
        f"Mutation status: {report.get('summary', {}).get('mutation_status')}",
        f"Overall status: {report.get('summary', {}).get('overall_status')}",
        "",
    ]
    for name in (
        "auth",
        "account_discovery_selection",
        "balances",
        "positions",
        "instrument_lookup",
        "quote_lookup",
        "order_construction",
        "order_preview",
        "sandbox_submit",
        "sandbox_cancel",
    ):
        payload = report.get("capability_matrix", {}).get(name, {})
        status = str(payload.get("status", "-")).upper()
        blockers = list(payload.get("blockers") or [])
        warnings = list(payload.get("warnings") or [])
        line = f"{name}: {status}"
        if blockers:
            line += " | blockers=" + "; ".join(blockers)
        elif warnings:
            line += " | warnings=" + "; ".join(warnings)
        lines.append(line)
    return "\n".join(lines)
