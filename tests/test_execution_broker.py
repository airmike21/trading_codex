from __future__ import annotations

import pytest

from trading_codex.execution import TastytradeBrokerPositionAdapter, normalize_tastytrade_snapshot


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
