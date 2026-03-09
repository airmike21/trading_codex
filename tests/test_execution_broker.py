from __future__ import annotations

import pytest

from trading_codex.execution import (
    RequestsTastytradeHttpClient,
    TastytradeBrokerPositionAdapter,
    normalize_tastytrade_snapshot,
)


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


def test_normalize_tastytrade_snapshot_reads_signed_positions_and_balances() -> None:
    snapshot = normalize_tastytrade_snapshot(
        account_id="5WT00001",
        positions_payload=_tastytrade_positions_payload(
            {
                "symbol": "AAA",
                "quantity": "10",
                "quantity-direction": "Long",
                "close-price": "101.25",
                "updated-at": "2026-03-09T12:00:00Z",
            },
            {
                "symbol": "BBB",
                "quantity": "4.0",
                "quantity-direction": "Short",
                "close-price": "55.50",
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
    assert snapshot.positions["BBB"].shares == -4
    assert snapshot.positions["BBB"].price == 55.5


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
                FakeResponse(ok=True, status_code=200, payload={"data": {"status": "ok"}}),
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
            {"login": "user@example.com", "password": "secret", "remember-me": True},
            {},
        ),
        (
            "POST",
            "https://api.tastytrade.com/device-challenge",
            {"code": "123456"},
            {"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
        ),
        (
            "POST",
            "https://api.tastytrade.com/sessions",
            {"login": "user@example.com", "password": "secret", "remember-me": True},
            {},
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
                FakeResponse(ok=True, status_code=200, payload={"data": {"status": "ok"}}),
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
        {"code": "654321"},
        {"X-Tastyworks-Challenge-Token": "challenge-token-from-header"},
    )
    assert all("/orders" not in url for _, url, _, _ in session.calls)
