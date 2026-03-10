from __future__ import annotations

import csv
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_codex.data import LocalStore


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _bars_for_index(idx: pd.DatetimeIndex, close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {"open": close, "high": close, "low": close, "close": close, "volume": 1_000},
        index=idx,
    )


def _write_synth_store(base_dir: Path) -> None:
    idx = pd.date_range("2019-01-01", periods=520, freq="B")
    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_bil = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_bil), index=idx)))


def _write_dual_mom_presets(path: Path, data_dir: Path) -> None:
    payload = {
        "presets": {
            "dual_mom_core": {
                "description": "test base",
                "mode": "change_only",
                "emit": "text",
                "state_file": str(path.parent / "source_core_state.json"),
                "state_key": "dual_mom_core",
                "log_csv": str(path.parent / "source_core_alerts.csv"),
                "run_backtest_args": [
                    "--strategy",
                    "dual_mom",
                    "--symbols",
                    "AAA",
                    "BBB",
                    "CCC",
                    "--defensive",
                    "BIL",
                    "--mom-lookback",
                    "63",
                    "--rebalance",
                    "M",
                    "--start",
                    "2020-01-02",
                    "--end",
                    "2020-12-01",
                    "--data-dir",
                    str(data_dir),
                    "--no-plot",
                ],
            },
            "dual_mom_core_vt": {
                "description": "test vt",
                "mode": "change_only",
                "emit": "text",
                "state_file": str(path.parent / "source_vt_state.json"),
                "state_key": "dual_mom_core_vt",
                "log_csv": str(path.parent / "source_vt_alerts.csv"),
                "run_backtest_args": [
                    "--strategy",
                    "dual_mom",
                    "--symbols",
                    "AAA",
                    "BBB",
                    "CCC",
                    "--defensive",
                    "BIL",
                    "--mom-lookback",
                    "63",
                    "--rebalance",
                    "M",
                    "--vol-target",
                    "0.12",
                    "--vol-lookback",
                    "21",
                    "--min-leverage",
                    "0.0",
                    "--max-leverage",
                    "1.0",
                    "--start",
                    "2020-01-02",
                    "--end",
                    "2020-12-01",
                    "--data-dir",
                    str(data_dir),
                    "--no-plot",
                ],
            },
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


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


def test_plan_execution_cli_from_signal_file_writes_artifacts_and_preserves_inputs(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "dual_mom_signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_payload = {
        "broker_name": "mock",
        "account_id": "paper-1",
        "buying_power": 10_000.0,
        "positions": [{"symbol": "EFA", "shares": 82, "price": 99.16}],
    }
    positions_path.write_text(json.dumps(positions_payload), encoding="utf-8")
    before_positions = positions_path.read_bytes()

    base_dir = tmp_path / "execution_plans"
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T10:45:00-05:00",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert "# Dry-Run Execution Plan dual_mom_signal" in proc.stdout

    json_artifacts = list((base_dir / "plans" / "2026-03-09").glob("*.json"))
    markdown_artifacts = list((base_dir / "reviews" / "2026-03-09").glob("*.md"))
    assert len(json_artifacts) == 1
    assert len(markdown_artifacts) == 1

    payload = json.loads(json_artifacts[0].read_text(encoding="utf-8"))
    assert payload["schema_name"] == "execution_plan"
    assert payload["items"][0]["classification"] == "RESIZE_BUY"
    assert payload["items"][0]["delta_shares"] == 18
    assert payload["artifacts"]["markdown_path"] == str(markdown_artifacts[0])

    review_text = markdown_artifacts[0].read_text(encoding="utf-8")
    assert "RESIZE_BUY" in review_text
    assert "dual_mom_signal" in review_text

    with (base_dir / "logs" / "execution_plans.csv").open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    assert rows[0]["source_label"] == "dual_mom_signal"

    assert positions_path.read_bytes() == before_positions


def test_plan_execution_cli_from_preset_supports_dual_mom_core(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    presets_path = tmp_path / "presets.json"
    _write_dual_mom_presets(presets_path, data_dir)

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(json.dumps({"broker_name": "mock", "positions": []}), encoding="utf-8")
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--preset",
        "dual_mom_core",
        "--presets-file",
        str(presets_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T11:00:00-05:00",
        "--emit",
        "json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    payload = json.loads(proc.stdout)
    assert payload["source"]["kind"] == "preset"
    assert payload["source"]["label"] == "dual_mom_core"
    assert payload["signal"]["strategy"] == "dual_mom"
    assert payload["items"]
    assert Path(payload["artifacts"]["json_path"]).exists()
    assert Path(payload["artifacts"]["markdown_path"]).exists()


def test_plan_execution_cli_with_tastytrade_broker_reads_mocked_account(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root, _env = _repo_root_and_env()
    sys.path.insert(0, str(repo_root))
    plan_execution = importlib.import_module("scripts.plan_execution")

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    presets_path = tmp_path / "presets.json"
    _write_dual_mom_presets(presets_path, data_dir)
    base_dir = tmp_path / "execution_plans"

    class FakeReadOnlyClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def get_positions(self, *, account_id: str) -> object:
            self.calls.append(("get_positions", account_id))
            return _tastytrade_positions_payload()

        def get_balances(self, *, account_id: str) -> object:
            self.calls.append(("get_balances", account_id))
            return _tastytrade_balances_payload(account_id=account_id)

    client = FakeReadOnlyClient()
    monkeypatch.setattr(plan_execution, "RequestsTastytradeHttpClient", lambda **_kwargs: client)

    exit_code = plan_execution.main(
        [
            "--preset",
            "dual_mom_core",
            "--presets-file",
            str(presets_path),
            "--broker",
            "tastytrade",
            "--account-id",
            "5WT00001",
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T12:10:00-05:00",
            "--emit",
            "json",
        ]
    )

    assert exit_code == 0
    assert client.calls == [("get_positions", "5WT00001"), ("get_balances", "5WT00001")]

    json_artifacts = list((base_dir / "plans" / "2026-03-09").glob("*.json"))
    assert len(json_artifacts) == 1
    payload = json.loads(json_artifacts[0].read_text(encoding="utf-8"))
    assert payload["broker_snapshot"]["broker_name"] == "tastytrade"
    assert payload["broker_snapshot"]["account_id"] == "5WT00001"
    assert payload["blockers"] == []


def test_plan_execution_cli_with_tastytrade_blocks_unrelated_holdings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root, _env = _repo_root_and_env()
    sys.path.insert(0, str(repo_root))
    plan_execution = importlib.import_module("scripts.plan_execution")

    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")
    base_dir = tmp_path / "execution_plans"

    class FakeReadOnlyClient:
        def get_positions(self, *, account_id: str) -> object:
            assert account_id == "5WT00001"
            return _tastytrade_positions_payload(
                {
                    "symbol": "XYZ  260417P00050000",
                    "underlying-symbol": "XYZ",
                    "instrument-type": "Equity Option",
                    "quantity": "-1",
                    "quantity-direction": "Short",
                    "close-price": "1.25",
                }
            )

        def get_balances(self, *, account_id: str) -> object:
            return _tastytrade_balances_payload(account_id=account_id)

        def submit_order(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("Dry-run planner must not submit orders.")

    monkeypatch.setattr(plan_execution, "RequestsTastytradeHttpClient", lambda **_kwargs: FakeReadOnlyClient())

    exit_code = plan_execution.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "tastytrade",
            "--account-id",
            "5WT00001",
            "--allowed-symbols",
            "AAA,BBB,CCC,BIL",
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T12:15:00-05:00",
            "--emit",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "unmanaged positions: XYZ  260417P00050000" in captured.err

    payload = json.loads(captured.out)
    assert payload["account_scope"] == "full_account"
    assert "unmanaged_positions_present" in payload["blockers"]
    assert "full_account_scope_blocked_by_unmanaged_positions" in payload["blockers"]
    assert payload["unmanaged_positions"][0]["classification_reason"] == "derivative_position"

    json_artifacts = list((base_dir / "plans" / "2026-03-09").glob("*.json"))
    assert len(json_artifacts) == 1
    artifact_payload = json.loads(json_artifacts[0].read_text(encoding="utf-8"))
    assert artifact_payload["unmanaged_positions"][0]["symbol"] == "XYZ  260417P00050000"


def test_plan_execution_cli_managed_sleeve_ack_computes_sleeve_math_and_reports_unmanaged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root, _env = _repo_root_and_env()
    sys.path.insert(0, str(repo_root))
    plan_execution = importlib.import_module("scripts.plan_execution")

    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")
    base_dir = tmp_path / "execution_plans"

    class FakeReadOnlyClient:
        def get_positions(self, *, account_id: str) -> object:
            assert account_id == "5WT00001"
            return _tastytrade_positions_payload(
                {
                    "symbol": "EFA",
                    "quantity": "82",
                    "quantity-direction": "Long",
                    "instrument-type": "Equity",
                    "close-price": "99.16",
                },
                {
                    "symbol": "XYZ",
                    "quantity": "7",
                    "quantity-direction": "Long",
                    "instrument-type": "Equity",
                    "close-price": "77.00",
                },
            )

        def get_balances(self, *, account_id: str) -> object:
            return _tastytrade_balances_payload(account_id=account_id)

        def submit_order(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("Dry-run planner must not submit orders.")

    monkeypatch.setattr(plan_execution, "RequestsTastytradeHttpClient", lambda **_kwargs: FakeReadOnlyClient())

    exit_code = plan_execution.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "tastytrade",
            "--account-id",
            "5WT00001",
            "--allowed-symbols",
            "AAA,BBB,CCC,BIL,EFA",
            "--account-scope",
            "managed_sleeve",
            "--ack-unmanaged-holdings",
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T12:17:00-05:00",
            "--emit",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""

    payload = json.loads(captured.out)
    assert payload["account_scope"] == "managed_sleeve"
    assert payload["plan_math_scope"] == "managed_sleeve_only"
    assert payload["unmanaged_holdings_acknowledged"] is True
    assert payload["blockers"] == []
    assert payload["warnings"] == ["unmanaged_positions_acknowledged_for_managed_sleeve"]
    assert [item["symbol"] for item in payload["items"]] == ["EFA"]
    assert payload["unmanaged_positions"][0]["symbol"] == "XYZ"

    markdown_artifacts = list((base_dir / "reviews" / "2026-03-09").glob("*.md"))
    assert len(markdown_artifacts) == 1
    review_text = markdown_artifacts[0].read_text(encoding="utf-8")
    assert "Account scope" in review_text
    assert "managed_sleeve" in review_text
    assert "Unmanaged Positions" in review_text


def test_plan_execution_cli_managed_sleeve_ack_blocks_only_on_buying_power_not_unmanaged_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root, _env = _repo_root_and_env()
    sys.path.insert(0, str(repo_root))
    plan_execution = importlib.import_module("scripts.plan_execution")

    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")
    base_dir = tmp_path / "execution_plans"

    class FakeReadOnlyClient:
        def get_positions(self, *, account_id: str) -> object:
            assert account_id == "5WT00001"
            return _tastytrade_positions_payload(
                {
                    "symbol": "EFA",
                    "quantity": "0",
                    "quantity-direction": "Long",
                    "instrument-type": "Equity",
                    "close-price": "99.16",
                },
                {
                    "symbol": "XYZ",
                    "quantity": "7",
                    "quantity-direction": "Long",
                    "instrument-type": "Equity",
                    "close-price": "77.00",
                },
            )

        def get_balances(self, *, account_id: str) -> object:
            return _tastytrade_balances_payload(account_id=account_id, cash="500.00", buying_power="500.00")

    monkeypatch.setattr(plan_execution, "RequestsTastytradeHttpClient", lambda **_kwargs: FakeReadOnlyClient())

    exit_code = plan_execution.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "tastytrade",
            "--account-id",
            "5WT00001",
            "--allowed-symbols",
            "AAA,BBB,CCC,BIL,EFA",
            "--account-scope",
            "managed_sleeve",
            "--ack-unmanaged-holdings",
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T12:18:00-05:00",
            "--emit",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "buy notional exceeds buying power" in captured.err
    assert "unmanaged positions" not in captured.err

    payload = json.loads(captured.out)
    assert payload["blockers"] == ["buy_notional_exceeds_buying_power"]
    assert payload["warnings"] == [
        "unmanaged_positions_acknowledged_for_managed_sleeve",
    ]
    assert payload["unmanaged_positions"][0]["symbol"] == "XYZ"
    assert [item["symbol"] for item in payload["items"]] == ["EFA"]


def test_plan_execution_cli_passes_tastytrade_challenge_args(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, _env = _repo_root_and_env()
    sys.path.insert(0, str(repo_root))
    plan_execution = importlib.import_module("scripts.plan_execution")

    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")
    base_dir = tmp_path / "execution_plans"

    captured_kwargs: dict[str, object] = {}

    class FakeReadOnlyClient:
        def get_positions(self, *, account_id: str) -> object:
            assert account_id == "5WT00001"
            return _tastytrade_positions_payload()

        def get_balances(self, *, account_id: str) -> object:
            return _tastytrade_balances_payload(account_id=account_id)

    def _build_client(**kwargs: object) -> FakeReadOnlyClient:
        captured_kwargs.update(kwargs)
        return FakeReadOnlyClient()

    monkeypatch.setattr(plan_execution, "RequestsTastytradeHttpClient", _build_client)

    exit_code = plan_execution.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "tastytrade",
            "--account-id",
            "5WT00001",
            "--allowed-symbols",
            "AAA,BBB,CCC,BIL",
            "--tastytrade-challenge-code",
            "123456",
            "--tastytrade-challenge-token",
            "challenge-token",
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T12:20:00-05:00",
            "--emit",
            "json",
        ]
    )

    assert exit_code == 0
    assert captured_kwargs == {"challenge_code": "123456", "challenge_token": "challenge-token"}


def test_plan_execution_cli_loads_tastytrade_secrets_file_and_respects_shell_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, _env = _repo_root_and_env()
    sys.path.insert(0, str(repo_root))
    plan_execution = importlib.import_module("scripts.plan_execution")

    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")
    secrets_path = tmp_path / "tastytrade.env"
    secrets_path.write_text(
        "\n".join(
            [
                "export TASTYTRADE_ACCOUNT='5WT00001'",
                "export TASTYTRADE_USERNAME='file-user@example.com'",
                "export TASTYTRADE_PASSWORD='file-password'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    for key in (
        "TASTYTRADE_ACCOUNT",
        "TASTYTRADE_USERNAME",
        "TASTYTRADE_PASSWORD",
        "TASTYTRADE_SESSION_TOKEN",
        "TASTYTRADE_ACCESS_TOKEN",
        "TASTYTRADE_API_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("TASTYTRADE_USERNAME", "shell-user@example.com")
    captured_env: dict[str, str] = {}

    class FakeReadOnlyClient:
        def __init__(self) -> None:
            captured_env["account"] = os.environ.get("TASTYTRADE_ACCOUNT", "")
            captured_env["username"] = os.environ.get("TASTYTRADE_USERNAME", "")
            captured_env["password"] = os.environ.get("TASTYTRADE_PASSWORD", "")

        def get_positions(self, *, account_id: str) -> object:
            assert account_id == "5WT00001"
            return _tastytrade_positions_payload()

        def get_balances(self, *, account_id: str) -> object:
            return _tastytrade_balances_payload(account_id=account_id)

    monkeypatch.setattr(plan_execution, "RequestsTastytradeHttpClient", lambda **_kwargs: FakeReadOnlyClient())

    exit_code = plan_execution.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "tastytrade",
            "--allowed-symbols",
            "AAA,BBB,CCC,BIL",
            "--secrets-file",
            str(secrets_path),
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T12:25:00-05:00",
            "--emit",
            "json",
        ]
    )

    assert exit_code == 0
    assert captured_env == {
        "account": "5WT00001",
        "username": "shell-user@example.com",
        "password": "file-password",
    }


def test_plan_execution_cli_exports_order_intents_for_clean_plan(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            {
                "broker_name": "mock",
                "account_id": "paper-1",
                "buying_power": 20000.0,
                "positions": [{"symbol": "EFA", "shares": 82, "price": 99.16}],
            }
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T12:40:00-05:00",
        "--emit",
        "json",
        "--export-order-intents",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    order_intents_path = Path(payload["artifacts"]["order_intents_json_path"])
    checklist_path = Path(payload["artifacts"]["manual_order_checklist_path"])
    assert order_intents_path.exists()
    assert checklist_path.exists()

    export_payload = json.loads(order_intents_path.read_text(encoding="utf-8"))
    assert export_payload["schema_name"] == "order_intent_export"
    assert export_payload["artifacts"]["manual_order_checklist_path"] == str(checklist_path)
    assert len(export_payload["intents"]) == 1
    assert export_payload["intents"][0]["side"] == "BUY"
    assert export_payload["intents"][0]["quantity"] == 18
    checklist_text = checklist_path.read_text(encoding="utf-8")
    assert "Manual Order Checklist signal" in checklist_text
    assert "BUY 18 EFA" in checklist_text


def test_plan_execution_cli_exports_simulated_orders_for_clean_capital_sized_plan(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            {
                "broker_name": "mock",
                "account_id": "paper-1",
                "buying_power": 20_000.0,
                "positions": [{"symbol": "EFA", "shares": 32, "price": 99.16}],
            }
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T12:40:30-05:00",
        "--emit",
        "json",
        "--sleeve-capital",
        "5000",
        "--export-simulated-orders",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["sizing"]["mode"] == "sleeve_capital"
    assert payload["sizing"]["usable_capital"] == 5000.0
    assert payload["items"][0]["desired_target_shares"] == 50
    assert payload["items"][0]["delta_shares"] == 18
    assert Path(payload["artifacts"]["order_intents_json_path"]).exists()
    simulated_path = Path(payload["artifacts"]["simulated_order_requests_path"])
    assert simulated_path.exists()

    simulated_payload = json.loads(simulated_path.read_text(encoding="utf-8"))
    assert simulated_payload["schema_name"] == "simulated_submission_export"
    assert simulated_payload["orders"][0]["symbol"] == "EFA"
    assert simulated_payload["orders"][0]["side"] == "BUY"
    assert simulated_payload["orders"][0]["quantity"] == 18


def test_plan_execution_cli_refuses_blocked_order_intent_export_by_default(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            {
                "broker_name": "mock",
                "account_id": "paper-1",
                "buying_power": 500.0,
                "positions": [{"symbol": "EFA", "shares": 0, "price": 99.16}],
            }
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T12:41:00-05:00",
        "--emit",
        "json",
        "--export-order-intents",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 2
    assert "REFUSED ORDER INTENT EXPORT" in proc.stderr
    payload = json.loads(proc.stdout)
    assert "order_intents_json_path" not in payload["artifacts"]
    assert "manual_order_checklist_path" not in payload["artifacts"]
    order_intent_artifacts = list((base_dir / "plans" / "2026-03-09").glob("*_order_intents.json"))
    assert order_intent_artifacts == []
    checklist_artifacts = list((base_dir / "reviews" / "2026-03-09").glob("*_manual_order_checklist.md"))
    assert checklist_artifacts == []


def test_plan_execution_cli_refuses_simulated_orders_for_insufficient_capital(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            {
                "broker_name": "mock",
                "account_id": "paper-1",
                "buying_power": 20_000.0,
                "positions": [{"symbol": "EFA", "shares": 0, "price": 99.16}],
            }
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T12:41:30-05:00",
        "--emit",
        "json",
        "--sleeve-capital",
        "50",
        "--export-simulated-orders",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 2, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert "capital sizing yields zero affordable shares" in proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["blockers"] == ["capital_sizing_yields_zero_shares"]
    assert payload["items"] == []
    assert "simulated_order_requests_path" not in payload["artifacts"]
    assert "order_intents_json_path" not in payload["artifacts"]
    simulated_artifacts = list((base_dir / "plans" / "2026-03-09").glob("*_simulated_order_requests.json"))
    assert simulated_artifacts == []


def test_plan_execution_cli_exports_manual_ticket_csv_for_clean_plan(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            {
                "broker_name": "mock",
                "account_id": "paper-1",
                "buying_power": 20000.0,
                "cash": 20000.0,
                "positions": [{"symbol": "EFA", "shares": 82, "price": 99.16}],
            }
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T12:42:00-05:00",
        "--emit",
        "json",
        "--export-manual-ticket-csv",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    csv_path = Path(payload["artifacts"]["manual_ticket_csv_path"])
    assert csv_path.exists()
    assert Path(payload["artifacts"]["order_intents_json_path"]).exists()

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    assert len(rows) == 1
    assert rows[0]["symbol"] == "EFA"
    assert rows[0]["side"] == "BUY"
    assert rows[0]["quantity"] == "18"
    assert rows[0]["classification"] == "RESIZE_BUY"
    assert set(rows[0]) >= {
        "generated_at_chicago",
        "source_label",
        "event_id",
        "strategy",
        "account_scope",
        "plan_math_scope",
        "symbol",
        "side",
        "quantity",
        "reference_price",
        "estimated_notional",
        "classification",
        "current_broker_shares",
        "desired_target_shares",
        "warnings",
    }


def test_plan_execution_cli_refuses_manual_ticket_csv_for_blocked_plan(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            {
                "broker_name": "mock",
                "account_id": "paper-1",
                "buying_power": 500.0,
                "cash": 500.0,
                "positions": [{"symbol": "EFA", "shares": 0, "price": 99.16}],
            }
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "execution_plans"

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "plan_execution.py"),
        "--signal-json-file",
        str(signal_path),
        "--positions-file",
        str(positions_path),
        "--base-dir",
        str(base_dir),
        "--timestamp",
        "2026-03-09T12:43:00-05:00",
        "--emit",
        "json",
        "--export-manual-ticket-csv",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 2
    assert "REFUSED ORDER INTENT / MANUAL TICKET CSV EXPORT" in proc.stderr
    payload = json.loads(proc.stdout)
    assert "manual_ticket_csv_path" not in payload["artifacts"]
    csv_artifacts = list((base_dir / "plans" / "2026-03-09").glob("*_manual_ticket_export.csv"))
    assert csv_artifacts == []
