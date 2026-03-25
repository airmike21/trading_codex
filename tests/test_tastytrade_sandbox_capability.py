from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from trading_codex.execution.tastytrade_sandbox import (
    load_tastytrade_sandbox_config,
    run_tastytrade_sandbox_capability,
)


def _sandbox_env(
    *,
    account_id: str | None = "5WT00001",
    base_url: str = "https://api.cert.tastytrade.com",
) -> dict[str, str]:
    env = {
        "TASTYTRADE_SANDBOX_USERNAME": "sandbox-user@example.com",
        "TASTYTRADE_SANDBOX_PASSWORD": "sandbox-password",
        "TASTYTRADE_SANDBOX_API_BASE_URL": base_url,
    }
    if account_id is not None:
        env["TASTYTRADE_SANDBOX_ACCOUNT"] = account_id
    return env


def _tastytrade_positions_payload(*items: dict[str, object]) -> dict[str, object]:
    return {"data": {"items": list(items)}}


def _tastytrade_balances_payload(
    *,
    account_id: str = "5WT00001",
    cash: str = "20000.00",
    buying_power: str = "20000.00",
) -> dict[str, object]:
    return {
        "data": {
            "account-number": account_id,
            "cash-balance": cash,
            "equity-buying-power": buying_power,
        }
    }


class FakeSandboxClient:
    def __init__(
        self,
        *,
        accounts: list[str] | None = None,
        quote_prices: dict[str, float] | None = None,
    ) -> None:
        self.accounts = accounts if accounts is not None else ["5WT00001"]
        self.quote_prices = quote_prices or {"BIL": 91.2, "EFA": 99.16}
        self.calls: list[tuple[str, str]] = []

    def get_balances(self, *, account_id: str) -> object:
        self.calls.append(("GET", f"/accounts/{account_id}/balances"))
        return _tastytrade_balances_payload(account_id=account_id)

    def get_positions(self, *, account_id: str) -> object:
        self.calls.append(("GET", f"/accounts/{account_id}/positions"))
        return _tastytrade_positions_payload()

    def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
        self.calls.append(("POST", f"/accounts/{account_id}/orders"))
        return {
            "data": {
                "account-number": account_id,
                "id": "sandbox-order-123",
                "status": "received",
                "request": payload,
            }
        }

    def get_json(self, path: str, *, params: dict[str, object] | None = None) -> object:
        del params
        self.calls.append(("GET", path))
        if path == "/customers/me/accounts":
            return {"data": {"items": [{"account-number": account_id} for account_id in self.accounts]}}
        if path.startswith("/instruments/equities"):
            symbol = path.split("=")[-1] if "=" in path else path.rsplit("/", 1)[-1]
            return {"data": {"items": [{"symbol": symbol, "instrument-type": "Equity"}]}}
        if path.startswith("/market-data/by-symbol/"):
            symbol = path.rsplit("/", 1)[-1]
            return {"data": {"symbol": symbol, "last-price": f"{self.quote_prices.get(symbol, 100.0):.2f}"}}
        raise ValueError(f"Unexpected GET path {path!r}")

    def post_json(self, path: str, *, payload: dict[str, object] | None = None) -> object:
        self.calls.append(("POST", path))
        return {"data": {"path": path, "payload": payload or {}, "status": "cancelled"}}

    def delete_json(self, path: str) -> object:
        self.calls.append(("DELETE", path))
        return {"data": {"path": path, "status": "cancelled"}}


def test_load_tastytrade_sandbox_config_requires_explicit_base_url_and_auth() -> None:
    with pytest.raises(ValueError, match="TASTYTRADE_SANDBOX_API_BASE_URL"):
        load_tastytrade_sandbox_config(environ={})


def test_tastytrade_sandbox_capability_fails_when_account_discovery_is_ambiguous() -> None:
    report = run_tastytrade_sandbox_capability(
        symbols=["EFA", "BIL"],
        preset_name="dual_mom_vol10_cash_core",
        client=FakeSandboxClient(accounts=["5WT00001", "5WT00002"]),
        environ=_sandbox_env(account_id=None),
        probe_order_symbol="EFA",
    )

    account_step = report["capability_matrix"]["account_discovery_selection"]
    assert account_step["status"] == "fail"
    assert account_step["blockers"] == ["sandbox_account_discovery_ambiguous:5WT00001,5WT00002"]
    assert report["capability_matrix"]["balances"]["status"] == "blocked"
    assert report["summary"]["overall_status"] == "fail"


def test_tastytrade_sandbox_capability_report_marks_pre_submit_steps_and_disabled_submit() -> None:
    report = run_tastytrade_sandbox_capability(
        symbols=["EFA", "BIL"],
        preset_name="dual_mom_vol10_cash_core",
        client=FakeSandboxClient(),
        environ=_sandbox_env(),
        probe_order_symbol="EFA",
    )

    assert report["summary"]["pre_submit_status"] == "pass"
    assert report["summary"]["overall_status"] == "pass"
    assert report["capability_matrix"]["auth"]["status"] == "pass"
    assert report["capability_matrix"]["account_discovery_selection"]["status"] == "pass"
    assert report["capability_matrix"]["balances"]["status"] == "pass"
    assert report["capability_matrix"]["positions"]["status"] == "pass"
    assert report["capability_matrix"]["instrument_lookup"]["status"] == "pass"
    assert report["capability_matrix"]["quote_lookup"]["status"] == "pass"
    assert report["capability_matrix"]["order_construction"]["status"] == "pass"
    assert report["capability_matrix"]["order_preview"]["status"] == "pass"
    assert report["capability_matrix"]["sandbox_submit"]["status"] == "blocked"
    assert report["capability_matrix"]["sandbox_submit"]["blockers"] == ["sandbox_submit_disabled_by_default"]
    assert report["capability_matrix"]["sandbox_cancel"]["status"] == "blocked"
    assert report["capability_matrix"]["sandbox_cancel"]["blockers"] == ["sandbox_cancel_not_requested"]


def test_tastytrade_sandbox_capability_preview_uses_whole_share_order_from_mocked_quote() -> None:
    report = run_tastytrade_sandbox_capability(
        symbols=["EFA", "BIL"],
        preset_name="dual_mom_vol10_cash_core",
        client=FakeSandboxClient(),
        environ=_sandbox_env(),
        probe_order_symbol="EFA",
        probe_order_qty=1,
    )

    preview = report["capability_matrix"]["order_construction"]["details"]["preview"]
    simulated_order = report["capability_matrix"]["order_preview"]["details"]["simulated_order"]
    assert preview["candidate_orders"] == [{"order_type": "MARKET", "qty": 1, "side": "BUY", "symbol": "EFA", "tif": "DAY"}]
    assert simulated_order["symbol"] == "EFA"
    assert simulated_order["quantity"] == 1
    assert simulated_order["instrument_type"] == "Equity"


def test_tastytrade_sandbox_capability_submit_guard_blocks_live_like_host() -> None:
    report = run_tastytrade_sandbox_capability(
        symbols=["EFA", "BIL"],
        preset_name="dual_mom_vol10_cash_core",
        client=FakeSandboxClient(),
        environ=_sandbox_env(base_url="https://api.tastytrade.com"),
        probe_order_symbol="EFA",
        enable_submit=True,
        sandbox_submit_account="5WT00001",
        cancel_after_submit=True,
    )

    assert report["capability_matrix"]["sandbox_submit"]["status"] == "blocked"
    assert "sandbox_submit_requires_sandbox_host" in report["capability_matrix"]["sandbox_submit"]["blockers"]
    assert report["capability_matrix"]["sandbox_cancel"]["status"] == "blocked"
    assert report["capability_matrix"]["sandbox_cancel"]["blockers"] == ["sandbox_cancel_requires_submitted_order"]


def test_tastytrade_sandbox_capability_cli_smoke_archives_json_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sandbox_module = importlib.import_module("trading_codex.execution.tastytrade_sandbox")
    monkeypatch.setattr(
        sandbox_module,
        "build_tastytrade_sandbox_client",
        lambda _config: FakeSandboxClient(),
    )

    secrets_path = tmp_path / "tastytrade_sandbox.env"
    secrets_path.write_text(
        "\n".join(
            [
                "export TASTYTRADE_SANDBOX_ACCOUNT='5WT00001'",
                "export TASTYTRADE_SANDBOX_USERNAME='sandbox-user@example.com'",
                "export TASTYTRADE_SANDBOX_PASSWORD='sandbox-password'",
                "export TASTYTRADE_SANDBOX_API_BASE_URL='https://api.cert.tastytrade.com'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = importlib.import_module("scripts.tastytrade_sandbox_capability")
    exit_code = script.main(
        [
            "--symbols",
            "EFA",
            "BIL",
            "--secrets-file",
            str(secrets_path),
            "--probe-order-symbol",
            "EFA",
            "--archive-root",
            str(tmp_path / "archive"),
            "--emit",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0, captured.err
    payload = json.loads(captured.out)
    assert payload["schema_name"] == "tastytrade_sandbox_capability"
    assert payload["summary"]["pre_submit_status"] == "pass"
    assert Path(payload["archive"]["manifest_path"]).exists()
    report_path = Path(payload["archive"]["capability_report_path"])
    assert report_path.exists()
    archived_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert archived_report["schema_name"] == "tastytrade_sandbox_capability"
