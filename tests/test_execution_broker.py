from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import pytest

from trading_codex.execution import (
    RequestsTastytradeHttpClient,
    TastytradeBrokerExecutionAdapter,
    TastytradeBrokerPositionAdapter,
    build_execution_plan,
    build_order_intent_export,
    build_simulated_submission_export,
    normalize_tastytrade_snapshot,
    parse_broker_snapshot,
    parse_signal_payload,
)
from trading_codex.execution.broker import _live_submission_fingerprint
from trading_codex.execution.broker import parse_broker_order_statuses


def _tastytrade_positions_payload(*items: dict[str, object]) -> dict[str, object]:
    return {"data": {"items": list(items)}}


def _tastytrade_balances_payload(
    *,
    account_id: str = "5WT00001",
    cash: str = "1234.56",
    buying_power: str = "5432.10",
) -> dict[str, object]:
    return {
        "data": {
            "account-number": account_id,
            "cash-balance": cash,
            "equity-buying-power": buying_power,
        }
    }


def _signal_payload() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "date": "2026-03-09",
        "strategy": "dual_mom",
        "action": "RESIZE",
        "symbol": "EFA",
        "price": 99.16,
        "target_shares": 100,
        "resize_prev_shares": 82,
        "resize_new_shares": 100,
        "next_rebalance": "2026-03-31",
    }
    payload["event_id"] = "2026-03-09:dual_mom:RESIZE:EFA:100:100:2026-03-31"
    return payload


def _managed_sleeve_simulated_export():
    signal = parse_signal_payload(_signal_payload())
    broker = parse_broker_snapshot(
        {
            "broker_name": "tastytrade",
            "account_id": "5WT00001",
            "buying_power": 20_000.0,
            "positions": [{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
        }
    )
    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="live_submit",
        source_ref="signal.json",
        broker_source_ref="tastytrade:5WT00001",
        data_dir=None,
    )
    return build_simulated_submission_export(build_order_intent_export(plan))


def test_normalize_tastytrade_snapshot_reads_signed_positions_and_balances() -> None:
    snapshot = normalize_tastytrade_snapshot(
        account_id="5WT00001",
        positions_payload=_tastytrade_positions_payload(
            {
                "symbol": "AAA",
                "quantity": "10",
                "quantity-direction": "Long",
                "instrument-type": "Equity",
                "close-price": "101.25",
                "updated-at": "2026-03-09T12:00:00Z",
            },
            {
                "symbol": "BBB  260417C00100000",
                "underlying-symbol": "BBB",
                "instrument-type": "Equity Option",
                "quantity": "4.0",
                "quantity-direction": "Short",
                "close-price": "5.50",
                "updated-at": "2026-03-09T12:05:00Z",
            },
        ),
        balances_payload=_tastytrade_balances_payload(),
    )

    assert snapshot.broker_name == "tastytrade"
    assert snapshot.account_id == "5WT00001"
    assert snapshot.cash == 1234.56
    assert snapshot.buying_power == 5432.1
    assert snapshot.as_of == "2026-03-09T12:05:00Z"
    assert snapshot.positions["AAA"].shares == 10
    assert snapshot.positions["AAA"].price == 101.25
    assert snapshot.positions["AAA"].instrument_type == "Equity"
    assert snapshot.positions["BBB  260417C00100000"].shares == -4
    assert snapshot.positions["BBB  260417C00100000"].price == 5.5
    assert snapshot.positions["BBB  260417C00100000"].instrument_type == "Equity Option"
    assert snapshot.positions["BBB  260417C00100000"].underlying_symbol == "BBB"


@pytest.mark.parametrize(
    ("positions_payload", "balances_payload", "match"),
    [
        ({"data": {}}, _tastytrade_balances_payload(), "data.items"),
        (_tastytrade_positions_payload({"symbol": "AAA", "quantity": "oops"}), _tastytrade_balances_payload(), "quantity"),
        (_tastytrade_positions_payload({"symbol": "AAA", "quantity": "1"}), {"data": []}, "data object"),
    ],
)
def test_normalize_tastytrade_snapshot_rejects_malformed_payloads(
    positions_payload: object,
    balances_payload: object,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        normalize_tastytrade_snapshot(
            account_id="5WT00001",
            positions_payload=positions_payload,
            balances_payload=balances_payload,
        )


def test_tastytrade_adapter_only_uses_read_methods() -> None:
    class FakeReadOnlyClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def get_positions(self, *, account_id: str) -> object:
            self.calls.append(("get_positions", account_id))
            return _tastytrade_positions_payload(
                {
                    "symbol": "AAA",
                    "quantity": "3",
                    "quantity-direction": "Long",
                    "close-price": "99.50",
                }
            )

        def get_balances(self, *, account_id: str) -> object:
            self.calls.append(("get_balances", account_id))
            return _tastytrade_balances_payload(account_id=account_id)

        def place_order(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("Execution planner must not call order submission methods.")

    client = FakeReadOnlyClient()
    snapshot = TastytradeBrokerPositionAdapter(account_id="5WT00001", client=client).load_snapshot()

    assert client.calls == [("get_positions", "5WT00001"), ("get_balances", "5WT00001")]
    assert snapshot.positions["AAA"].shares == 3


def test_parse_broker_order_statuses_reads_file_order_status_payload() -> None:
    statuses = parse_broker_order_statuses(
        {
            "account_id": "5WT00001",
            "orders": [
                {
                    "order_id": "order-123",
                    "status": "filled",
                    "filled_quantity": 1,
                    "remaining_quantity": 0,
                }
            ],
        }
    )

    assert statuses["order-123"].account_id == "5WT00001"
    assert statuses["order-123"].status == "filled"
    assert statuses["order-123"].filled_quantity == 1
    assert statuses["order-123"].remaining_quantity == 0


def test_tastytrade_adapter_loads_single_order_status_with_read_only_client() -> None:
    class FakeReadOnlyClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Order-status unit test should not call get_positions.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Order-status unit test should not call get_balances.")

        def get_order(self, *, account_id: str, order_id: str) -> object:
            self.calls.append(("get_order", account_id, order_id))
            return {
                "data": {
                    "account-number": account_id,
                    "id": order_id,
                    "status": "filled",
                    "filled-quantity": "1",
                    "remaining-quantity": "0",
                }
            }

    client = FakeReadOnlyClient()
    status = TastytradeBrokerPositionAdapter(account_id="5WT00001", client=client).load_order_status(order_id="order-123")

    assert client.calls == [("get_order", "5WT00001", "order-123")]
    assert status.order_id == "order-123"
    assert status.status == "filled"
    assert status.filled_quantity == 1


def test_tastytrade_http_client_surfaces_device_challenge_error_when_code_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASTYTRADE_USERNAME", "user@example.com")
    monkeypatch.setenv("TASTYTRADE_PASSWORD", "secret")

    class FakeResponse:
        ok = False
        status_code = 403
        headers = {"X-Tastyworks-Challenge-Token": "challenge-token"}

        def json(self) -> object:
            return {
                "error": {
                    "code": "device_challenge_required",
                    "message": "Device authentication challenge required",
                    "redirect": {
                        "method": "POST",
                        "url": "/device-challenge",
                        "required_headers": ["X-Tastyworks-Challenge-Token"],
                    },
                }
            }

        def raise_for_status(self) -> None:
            raise AssertionError("Expected adapter to raise from parsed API payload before raise_for_status().")

    class FakeSession:
        def request(self, **_kwargs: object) -> FakeResponse:
            return FakeResponse()

    client = RequestsTastytradeHttpClient(session=FakeSession(), base_url="https://api.tastytrade.com")
    with pytest.raises(ValueError, match="challenge code"):
        client.get_positions(account_id="5WT00001")


def test_tastytrade_http_client_completes_device_challenge_and_retries_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASTYTRADE_USERNAME", "user@example.com")
    monkeypatch.setenv("TASTYTRADE_PASSWORD", "secret")
    monkeypatch.setenv("TASTYTRADE_CHALLENGE_CODE", "123456")

    class FakeResponse:
        def __init__(
            self,
            *,
            ok: bool,
            status_code: int,
            payload: object,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.ok = ok
            self.status_code = status_code
            self._payload = payload
            self.headers = headers or {}

        def json(self) -> object:
            return self._payload

        def raise_for_status(self) -> None:
            raise AssertionError("Expected adapter to handle mocked auth flow without raise_for_status().")

    class FakeSession:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, object, dict[str, str]]] = []
            self.responses = [
                FakeResponse(
                    ok=False,
                    status_code=403,
                    payload={
                        "error": {
                            "code": "device_challenge_required",
                            "message": "Device authentication challenge required",
                            "redirect": {
                                "method": "POST",
                                "url": "/device-challenge",
                                "required_headers": ["X-Tastyworks-Challenge-Token"],
                            },
                        }
                    },
                    headers={"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
                ),
                FakeResponse(
                    ok=True,
                    status_code=200,
                    payload={
                        "data": {
                            "step": "otp_verification",
                            "redirect": {
                                "method": "POST",
                                "url": "/sessions",
                                "required-headers": ["X-Tastyworks-Challenge-Token", "X-Tastyworks-OTP"],
                            },
                        }
                    },
                ),
                FakeResponse(ok=True, status_code=201, payload={"data": {"session-token": "session-123"}}),
                FakeResponse(ok=True, status_code=200, payload=_tastytrade_positions_payload()),
            ]

        def request(
            self,
            *,
            method: str,
            url: str,
            json: object = None,
            headers: dict[str, str] | None = None,
            timeout: object = None,
        ) -> FakeResponse:
            del timeout
            self.calls.append((method, url, json, dict(headers or {})))
            return self.responses.pop(0)

    session = FakeSession()
    client = RequestsTastytradeHttpClient(session=session, base_url="https://api.tastytrade.com")

    payload = client.get_positions(account_id="5WT00001")

    assert payload == {"data": {"items": []}}
    assert session.calls == [
        (
            "POST",
            "https://api.tastytrade.com/sessions",
            {"login": "user@example.com", "password": "secret", "rememberMe": True},
            {},
        ),
        (
            "POST",
            "https://api.tastytrade.com/device-challenge",
            {},
            {"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
        ),
        (
            "POST",
            "https://api.tastytrade.com/sessions",
            {"login": "user@example.com", "password": "secret", "rememberMe": True},
            {
                "X-Tastyworks-Challenge-Token": "challenge-token-from-header",
                "X-Tastyworks-OTP": "123456",
            },
        ),
        (
            "GET",
            "https://api.tastytrade.com/accounts/5WT00001/positions",
            None,
            {"Authorization": "session-123"},
        ),
    ]
    assert all("/orders" not in url for _, url, _, _ in session.calls)


def test_tastytrade_http_client_prompts_for_challenge_code_on_interactive_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASTYTRADE_USERNAME", "user@example.com")
    monkeypatch.setenv("TASTYTRADE_PASSWORD", "secret")

    class FakeStdin:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr("sys.stdin", FakeStdin())
    monkeypatch.setattr("getpass.getpass", lambda _prompt: "654321")

    class FakeResponse:
        def __init__(
            self,
            *,
            ok: bool,
            status_code: int,
            payload: object,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.ok = ok
            self.status_code = status_code
            self._payload = payload
            self.headers = headers or {}

        def json(self) -> object:
            return self._payload

        def raise_for_status(self) -> None:
            raise AssertionError("Expected adapter to handle mocked auth flow without raise_for_status().")

    class FakeSession:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, object, dict[str, str]]] = []
            self.responses = [
                FakeResponse(
                    ok=False,
                    status_code=403,
                    payload={
                        "error": {
                            "code": "device_challenge_required",
                            "message": "Device authentication challenge required",
                            "redirect": {
                                "method": "POST",
                                "url": "/device-challenge",
                                "required_headers": ["X-Tastyworks-Challenge-Token"],
                            },
                        }
                    },
                    headers={"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
                ),
                FakeResponse(
                    ok=True,
                    status_code=200,
                    payload={
                        "data": {
                            "step": "otp_verification",
                            "redirect": {
                                "method": "POST",
                                "url": "/sessions",
                                "required-headers": ["X-Tastyworks-Challenge-Token", "X-Tastyworks-OTP"],
                            },
                        }
                    },
                ),
                FakeResponse(ok=True, status_code=201, payload={"data": {"session-token": "session-123"}}),
                FakeResponse(ok=True, status_code=200, payload=_tastytrade_positions_payload()),
            ]

        def request(
            self,
            *,
            method: str,
            url: str,
            json: object = None,
            headers: dict[str, str] | None = None,
            timeout: object = None,
        ) -> FakeResponse:
            del timeout
            self.calls.append((method, url, json, dict(headers or {})))
            return self.responses.pop(0)

    session = FakeSession()
    client = RequestsTastytradeHttpClient(session=session, base_url="https://api.tastytrade.com")

    payload = client.get_positions(account_id="5WT00001")

    assert payload == {"data": {"items": []}}
    assert session.calls[1] == (
        "POST",
        "https://api.tastytrade.com/device-challenge",
        {},
        {"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
    )
    assert session.calls[2] == (
        "POST",
        "https://api.tastytrade.com/sessions",
        {"login": "user@example.com", "password": "secret", "rememberMe": True},
        {
            "X-Tastyworks-Challenge-Token": "challenge-token-from-header",
            "X-Tastyworks-OTP": "654321",
        },
    )
    assert all("/orders" not in url for _, url, _, _ in session.calls)


def test_tastytrade_http_client_normalizes_whitespace_in_challenge_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASTYTRADE_USERNAME", "user@example.com")
    monkeypatch.setenv("TASTYTRADE_PASSWORD", "secret")
    monkeypatch.setenv("TASTYTRADE_CHALLENGE_CODE", "229 416")

    class FakeResponse:
        def __init__(
            self,
            *,
            ok: bool,
            status_code: int,
            payload: object,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.ok = ok
            self.status_code = status_code
            self._payload = payload
            self.headers = headers or {}

        def json(self) -> object:
            return self._payload

        def raise_for_status(self) -> None:
            raise AssertionError("Expected adapter to handle mocked auth flow without raise_for_status().")

    class FakeSession:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, object, dict[str, str]]] = []
            self.responses = [
                FakeResponse(
                    ok=False,
                    status_code=403,
                    payload={
                        "error": {
                            "code": "device_challenge_required",
                            "message": "Device authentication challenge required",
                            "redirect": {
                                "method": "POST",
                                "url": "/device-challenge",
                                "required_headers": ["X-Tastyworks-Challenge-Token"],
                            },
                        }
                    },
                    headers={"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
                ),
                FakeResponse(
                    ok=True,
                    status_code=200,
                    payload={
                        "data": {
                            "step": "otp_verification",
                            "redirect": {
                                "method": "POST",
                                "url": "/sessions",
                                "required-headers": ["X-Tastyworks-Challenge-Token", "X-Tastyworks-OTP"],
                            },
                        }
                    },
                ),
                FakeResponse(ok=True, status_code=201, payload={"data": {"session-token": "session-123"}}),
                FakeResponse(ok=True, status_code=200, payload=_tastytrade_positions_payload()),
            ]

        def request(
            self,
            *,
            method: str,
            url: str,
            json: object = None,
            headers: dict[str, str] | None = None,
            timeout: object = None,
        ) -> FakeResponse:
            del timeout
            self.calls.append((method, url, json, dict(headers or {})))
            return self.responses.pop(0)

    session = FakeSession()
    client = RequestsTastytradeHttpClient(session=session, base_url="https://api.tastytrade.com")

    payload = client.get_positions(account_id="5WT00001")

    assert payload == {"data": {"items": []}}
    assert session.calls[2][3]["X-Tastyworks-OTP"] == "229416"


def test_tastytrade_execution_adapter_submits_supported_orders_with_mocked_client(tmp_path: Path) -> None:
    simulated = _managed_sleeve_simulated_export()
    ledger_path = tmp_path / "live_submit_state" / "live_submission_fingerprints.jsonl"
    live_submission_artifact_path = tmp_path / "artifacts" / "live_submission.json"

    class FakeLiveClient:
        def __init__(self) -> None:
            self.payloads: list[tuple[str, dict[str, object]]] = []

        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
            self.payloads.append((account_id, payload))
            return {"data": {"id": "order-123", "status": "received"}}

    client = FakeLiveClient()
    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=client).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=live_submission_artifact_path,
    )

    assert export.live_submit_attempted is True
    assert export.submission_succeeded is True
    assert export.submission_result == "submitted"
    assert export.manual_clearance_required is False
    assert export.refusal_reasons == []
    assert export.plan_sha256 == simulated.plan_sha256
    assert export.live_allowed_account == "5WT00001"
    assert export.durable_state is not None
    assert export.durable_state["ledger_path"] == str(ledger_path)
    assert len(export.orders) == 1
    assert export.orders[0].dry_run is False
    assert export.orders[0].succeeded is True
    assert export.orders[0].broker_order_id == "order-123"
    assert not Path(export.durable_state["claim_path"]).exists()
    ledger_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    assert len(ledger_lines) == 1
    assert json.loads(ledger_lines[0])["result"] == "submitted"
    assert client.payloads == [
        (
            "5WT00001",
            {
                "order-type": "Market",
                "time-in-force": "Day",
                "legs": [
                    {
                        "instrument-type": "Equity",
                        "symbol": "EFA",
                        "quantity": 18,
                        "action": "Buy to Open",
                    }
                ],
            },
        )
    ]


def test_tastytrade_execution_adapter_blocks_ambiguous_attempted_submit_retries(tmp_path: Path) -> None:
    simulated = _managed_sleeve_simulated_export()
    ledger_path = tmp_path / "live_submit_state" / "live_submission_fingerprints.jsonl"
    live_submission_artifact_path = tmp_path / "artifacts" / "live_submission.json"

    class AmbiguousLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
            assert account_id == "5WT00001"
            assert payload["legs"][0]["symbol"] == "EFA"
            return {"data": {"foo": "bar"}}

    first_export = TastytradeBrokerExecutionAdapter(
        account_id="5WT00001",
        client=AmbiguousLiveClient(),
    ).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=live_submission_artifact_path,
    )

    assert first_export.live_submit_attempted is True
    assert first_export.submission_succeeded is False
    assert first_export.manual_clearance_required is True
    assert first_export.submission_result == "ambiguous_attempted_submit_manual_clearance_required"
    ledger_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    assert len(ledger_lines) == 1
    assert json.loads(ledger_lines[0])["result"] == "ambiguous_attempted_submit_manual_clearance_required"

    class SuccessClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Ambiguous prior submits must block a retry before any new submit call.")

    second_export = TastytradeBrokerExecutionAdapter(
        account_id="5WT00001",
        client=SuccessClient(),
    ).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=live_submission_artifact_path,
    )

    assert second_export.live_submit_attempted is False
    assert second_export.submission_result == "refused_duplicate"
    assert "live_submit_duplicate_fingerprint" in second_export.refusal_reasons
    assert second_export.duplicate_submit_refusal is not None
    assert (
        second_export.duplicate_submit_refusal["prior_record"]["result"]
        == "ambiguous_attempted_submit_manual_clearance_required"
    )


def test_tastytrade_execution_adapter_refuses_when_durable_ledger_is_corrupt(tmp_path: Path) -> None:
    simulated = _managed_sleeve_simulated_export()
    ledger_path = tmp_path / "live_submit_state" / "live_submission_fingerprints.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text('{"broken":\n', encoding="utf-8")

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Corrupt durable ledger state must refuse before any submit call.")

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=tmp_path / "artifacts" / "live_submission.json",
    )

    assert export.live_submit_attempted is False
    assert export.submission_result == "refused_pre_submit"
    assert "live_submit_duplicate_ledger_unreadable" in export.refusal_reasons
    assert export.duplicate_submit_refusal is not None
    assert export.duplicate_submit_refusal["ledger_path"] == str(ledger_path)
    assert "malformed" in export.duplicate_submit_refusal["error"]
    assert ledger_path.read_text(encoding="utf-8") == '{"broken":\n'


def test_tastytrade_execution_adapter_refuses_when_pending_claim_exists(tmp_path: Path) -> None:
    simulated = _managed_sleeve_simulated_export()
    ledger_path = tmp_path / "live_submit_state" / "live_submission_fingerprints.jsonl"
    fingerprint = _live_submission_fingerprint(
        broker_account_id="5WT00001",
        plan_sha256=simulated.plan_sha256,
    )
    claim_path = ledger_path.parent / "claims" / f"{fingerprint}.json"
    claim_path.parent.mkdir(parents=True, exist_ok=True)
    claim_path.write_text(
        json.dumps(
            {
                "claim_path": str(claim_path),
                "generated_at_chicago": "2026-03-09T12:00:00-05:00",
                "live_submission_fingerprint": fingerprint,
                "manual_clearance_required": True,
                "plan_sha256": simulated.plan_sha256,
                "result": "claim_pending_manual_clearance_required",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Pending claim state must block before any new submit call.")

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=tmp_path / "artifacts" / "live_submission.json",
    )

    assert export.live_submit_attempted is False
    assert export.submission_result == "refused_duplicate"
    assert "live_submit_duplicate_fingerprint" in export.refusal_reasons
    assert export.duplicate_submit_refusal is not None
    assert export.duplicate_submit_refusal["prior_record"]["result"] == "claim_pending_manual_clearance_required"
    assert claim_path.exists()


def test_tastytrade_execution_adapter_refuses_unsupported_instrument_type() -> None:
    simulated = _managed_sleeve_simulated_export()
    simulated = replace(simulated, orders=[replace(simulated.orders[0], instrument_type="Equity Option")])

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Unsupported instruments must be refused before any submit call.")

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
    )

    assert export.live_submit_attempted is False
    assert export.submission_succeeded is False
    assert "live_submit_unsupported_instrument_type:EFA:Equity Option" in export.refusal_reasons


def test_tastytrade_execution_adapter_rejects_malformed_submission_response() -> None:
    simulated = _managed_sleeve_simulated_export()

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
            assert account_id == "5WT00001"
            assert payload["legs"][0]["symbol"] == "EFA"
            return {"data": {"foo": "bar"}}

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
    )

    assert export.live_submit_attempted is True
    assert export.submission_succeeded is False
    assert len(export.orders) == 1
    assert export.orders[0].attempted is True
    assert export.orders[0].succeeded is False
    assert "order id or status" in (export.orders[0].error or "")


def test_tastytrade_execution_adapter_refuses_order_qty_over_cap() -> None:
    simulated = _managed_sleeve_simulated_export()

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Quantity cap violations must be refused before any submit call.")

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=10,
    )

    assert export.live_submit_attempted is False
    assert "live_submit_order_qty_exceeds_cap:EFA:18:10" in export.refusal_reasons


def test_tastytrade_execution_adapter_refuses_order_notional_over_cap() -> None:
    simulated = _managed_sleeve_simulated_export()

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Notional cap violations must be refused before any submit call.")

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=1_000.0,
        live_max_order_qty=100,
    )

    assert export.live_submit_attempted is False
    assert "live_submit_order_notional_exceeds_cap:EFA:1784.88:1000.00" in export.refusal_reasons


@pytest.mark.parametrize("quantity", [1.5, 0])
def test_tastytrade_execution_adapter_refuses_invalid_live_quantities(quantity: object) -> None:
    simulated = _managed_sleeve_simulated_export()
    simulated = replace(simulated, orders=[replace(simulated.orders[0], quantity=quantity)])

    class FakeLiveClient:
        def get_positions(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def get_balances(self, *, account_id: str) -> object:
            raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("Invalid quantities must be refused before any submit call.")

    export = TastytradeBrokerExecutionAdapter(account_id="5WT00001", client=FakeLiveClient()).submit_live_orders(
        export=simulated,
        confirm_account_id="5WT00001",
        live_allowed_account="5WT00001",
        confirm_plan_sha256=simulated.plan_sha256,
        allowed_symbols={"EFA", "BIL", "SPY", "QQQ", "IWM"},
        live_max_order_notional=5_000.0,
        live_max_order_qty=100,
    )

    assert export.live_submit_attempted is False
    assert f"live_submit_invalid_quantity:EFA:{quantity}" in export.refusal_reasons
