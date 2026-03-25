from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import live_canary_shadow_rehearsal
from trading_codex.data import LocalStore
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    DEFAULT_LIVE_CANARY_MAX_LONG_SHARES,
)


ACCOUNT_ID = "5WT00001"
TIMESTAMP = "2026-03-19T10:45:00-04:00"
POSITIONS_AS_OF = TIMESTAMP
SIGNAL_START = "2025-01-01"


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _price_series(index: pd.DatetimeIndex, returns: np.ndarray, base: float) -> pd.Series:
    return pd.Series(base * np.cumprod(1.0 + returns.astype(float)), index=index)


def _write_symbol_bars(store: LocalStore, symbol: str, close: pd.Series) -> None:
    store.write_bars(
        symbol,
        pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=close.index,
        ),
    )


def _write_validation_store(base_dir: Path, *, index: pd.DatetimeIndex) -> None:
    store = LocalStore(base_dir=base_dir)
    alt = np.arange(len(index))
    close_map = {
        "SPY": _price_series(index, np.full(len(index), 0.0005), 100.0),
        "QQQ": _price_series(index, np.where(alt % 2 == 0, 0.0015, -0.0010), 105.0),
        "IWM": _price_series(index, np.full(len(index), -0.0001), 95.0),
        "EFA": _price_series(index, np.where(alt % 2 == 0, 0.03, -0.01), 98.0),
        "BIL": _price_series(index, np.full(len(index), 0.0001), 100.0),
    }
    for symbol, close in close_map.items():
        _write_symbol_bars(store, symbol, close)


def _write_positions_file(path: Path) -> Path:
    payload = {
        "broker_name": "tastytrade",
        "account_id": ACCOUNT_ID,
        "as_of": POSITIONS_AS_OF,
        "buying_power": 20_000.0,
        "cash": 20_000.0,
        "positions": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _expected_event_id(payload: dict[str, object]) -> str:
    def g(key: str) -> str:
        value = payload.get(key, "")
        return "" if value is None else str(value)

    return ":".join(
        [
            g("date"),
            g("strategy"),
            g("action"),
            g("symbol"),
            g("target_shares"),
            g("resize_new_shares"),
            g("next_rebalance"),
        ]
    )


def test_live_canary_shadow_rehearsal_cli_smoke_creates_preview_bundle_with_file_broker_truth(
    tmp_path: Path,
) -> None:
    repo_root, env = _repo_root_and_env()
    index = pd.bdate_range(end=pd.Timestamp("2026-03-18"), periods=320)

    data_dir = tmp_path / "data"
    bundle_base_dir = tmp_path / "bundle_base"
    positions_path = _write_positions_file(tmp_path / "positions.json")
    _write_validation_store(data_dir, index=index)

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_shadow_rehearsal.py"),
            "--bundle-base-dir",
            str(bundle_base_dir),
            "--account-id",
            ACCOUNT_ID,
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--data-dir",
            str(data_dir),
            "--start",
            SIGNAL_START,
            "--end",
            index[-1].date().isoformat(),
            "--timestamp",
            TIMESTAMP,
            "--emit",
            "json",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    result = json.loads(proc.stdout)
    bundle_dir = Path(result["bundle_dir"])
    assert bundle_dir.exists()

    signal_path = Path(result["artifact_paths"]["signal_json"])
    readiness_path = Path(result["artifact_paths"]["readiness_json"])
    launch_path = Path(result["artifact_paths"]["launch_json"])
    reconcile_path = Path(result["artifact_paths"]["reconcile_json"])
    summary_path = Path(result["artifact_paths"]["summary_md"])
    live_canary_base_dir = Path(result["artifact_paths"]["live_canary_base_dir"])

    for artifact_path in (signal_path, readiness_path, launch_path, reconcile_path, summary_path):
        assert artifact_path.exists()

    signal_payload = json.loads(signal_path.read_text(encoding="utf-8"))
    readiness_payload = json.loads(readiness_path.read_text(encoding="utf-8"))
    launch_payload = json.loads(launch_path.read_text(encoding="utf-8"))
    reconcile_payload = json.loads(reconcile_path.read_text(encoding="utf-8"))
    summary_text = summary_path.read_text(encoding="utf-8")

    assert signal_payload["strategy"] == "dual_mom_vol10_cash"
    assert signal_payload["symbol"] in {"SPY", "QQQ", "IWM", "EFA", "BIL"}
    assert signal_payload["action"] in {"BUY", "ENTER", "ROTATE", "RESIZE"}
    assert signal_payload["event_id"] == _expected_event_id(signal_payload)

    assert readiness_payload["verdict"] == "ready"
    canary_gate = next(gate for gate in readiness_payload["gates"] if gate["gate"] == "canary_order_readiness")
    orders = canary_gate["details"]["orders"]
    assert orders
    assert all(order["symbol"] in DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS for order in orders)
    assert all(order["requested_qty"] <= DEFAULT_LIVE_CANARY_MAX_LONG_SHARES for order in orders)
    assert all(order["executable_qty"] <= DEFAULT_LIVE_CANARY_MAX_LONG_SHARES for order in orders)
    assert all(order["desired_canary_shares"] <= DEFAULT_LIVE_CANARY_MAX_LONG_SHARES for order in orders)

    assert launch_payload["requested_live_submit"] is False
    assert launch_payload["submit_path_invoked"] is False
    assert launch_payload["submit_outcome"] == "not_requested"
    assert launch_payload["submit_result"] is None

    assert reconcile_payload["verdict"] == "not_applicable"
    assert reconcile_payload["mode"] == "preview_only"

    assert signal_payload["event_id"] == readiness_payload["scope"]["event_id"]
    assert signal_payload["event_id"] == launch_payload["event_context"]["event_id"]
    assert signal_payload["event_id"] == reconcile_payload["context"]["event_id"]

    assert not (live_canary_base_dir / "events").exists()
    assert not (live_canary_base_dir / "sessions").exists()
    assert not (live_canary_base_dir / "claims").exists()
    assert not (live_canary_base_dir / "broker_live_submission_fingerprints.jsonl").exists()

    assert "Strategy: `dual_mom_vol10_cash`" in summary_text
    assert f"Event ID: `{signal_payload['event_id']}`" in summary_text
    assert "Readiness verdict: `ready`" in summary_text
    assert "Launch outcome: `not_requested`" in summary_text
    assert "Reconcile verdict: `not_applicable`" in summary_text


def test_live_canary_shadow_rehearsal_bundle_dir_is_deterministic_for_signal_scope(tmp_path: Path) -> None:
    signal_payload = {
        "date": "2026-03-20",
        "strategy": "dual_mom_vol10_cash",
        "action": "RESIZE",
        "symbol": "EFA",
        "target_shares": 100,
        "resize_new_shares": 100,
        "next_rebalance": "2026-03-31",
        "event_id": "2026-03-20:dual_mom_vol10_cash:RESIZE:EFA:100:100:2026-03-31",
    }

    first = live_canary_shadow_rehearsal.build_rehearsal_bundle_dir(
        tmp_path / "bundles",
        broker="file",
        account_id=ACCOUNT_ID,
        signal_payload=signal_payload,
    )
    second = live_canary_shadow_rehearsal.build_rehearsal_bundle_dir(
        tmp_path / "bundles",
        broker="file",
        account_id=ACCOUNT_ID,
        signal_payload=signal_payload,
    )

    assert first == second
    assert first == (
        tmp_path
        / "bundles"
        / "live_canary_shadow_rehearsals"
        / "file"
        / ACCOUNT_ID
        / "2026-03-20"
        / "2026-03-20_dual_mom_vol10_cash_RESIZE_EFA_100_100_2026-03-31"
    )
