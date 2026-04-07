from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.data import LocalStore
from trading_codex.shadow import (
    PRIMARY_LIVE_CANDIDATE_V1_ID,
    PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_PRESET,
    PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STATE_KEY,
    PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
    primary_live_candidate_v1_runtime_mapping,
    primary_live_candidate_v1_vol_managed_shadow_config,
)
from trading_codex.strategies.dual_mom_v1 import DualMomentumV1Strategy


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
                "volume": 1_000_000.0,
            },
            index=close.index,
        ),
    )


def test_primary_live_candidate_runtime_mapping_is_explicit() -> None:
    primary_mapping = primary_live_candidate_v1_runtime_mapping()
    assert primary_mapping.strategy_id == PRIMARY_LIVE_CANDIDATE_V1_ID
    assert primary_mapping.runtime_strategy == PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY
    assert primary_mapping.default_preset == PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_PRESET
    assert primary_mapping.default_state_key == PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STATE_KEY

    shadow_config = primary_live_candidate_v1_vol_managed_shadow_config()
    assert shadow_config.strategy_id == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
    assert shadow_config.primary_candidate_mapping == primary_mapping
    assert shadow_config.implementation_strategy == "dual_mom_v1"
    assert shadow_config.implementation_label == "dual_mom_v1_shadow_impl"
    assert shadow_config.risk_symbols == ("SPY", "QQQ", "IWM", "EFA")
    assert shadow_config.defensive_symbol == "BIL"
    assert shadow_config.momentum_lookback == 63
    assert shadow_config.top_n == 1
    assert shadow_config.rebalance == 21
    assert shadow_config.vol_target == 0.10
    assert shadow_config.vol_lookback == 20
    assert shadow_config.vol_update == "rebalance"

    strategy = shadow_config.build_strategy()
    assert isinstance(strategy, DualMomentumV1Strategy)
    assert strategy.symbols == ["SPY", "QQQ", "IWM", "EFA"]
    assert strategy.defensive_symbol == "BIL"
    assert strategy.lookback == 63
    assert strategy.rebalance == 21


def test_primary_live_candidate_vol_managed_shadow_is_backtestable_locally(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    periods = 320
    index = pd.date_range("2020-01-01", periods=periods, freq="B")
    alternating = np.arange(periods)

    store = LocalStore(base_dir=tmp_path)
    close_map = {
        "SPY": _price_series(index, np.full(periods, 0.0005), 100.0),
        "QQQ": _price_series(index, np.where(alternating % 2 == 0, 0.0012, -0.0008), 105.0),
        "IWM": _price_series(index, np.full(periods, -0.0001), 95.0),
        "EFA": _price_series(index, np.where(alternating % 2 == 0, 0.0180, -0.0075), 98.0),
        "BIL": _price_series(index, np.full(periods, 0.0001), 100.0),
    }
    for symbol, close in close_map.items():
        _write_symbol_bars(store, symbol, close)

    shadow_dir = tmp_path / "shadow"
    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "run_backtest.py"),
            "--strategy",
            PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
            "--start",
            index[0].date().isoformat(),
            "--end",
            index[-1].date().isoformat(),
            "--no-plot",
            "--next-action-json",
            "--shadow-artifacts-dir",
            str(shadow_dir),
            "--data-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(lines[0])
    assert payload["strategy"] == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
    assert PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID in str(payload["event_id"])
    assert payload["next_rebalance"] is not None

    artifact_dir = shadow_dir / "plans" / index[-1].date().isoformat()
    bundle = json.loads(next(artifact_dir.glob("*_shadow_review.json")).read_text(encoding="utf-8"))

    assert bundle["strategy"] == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
    assert bundle["shadow_strategy_id"] == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
    assert bundle["vol_target"] == 0.10
    assert bundle["risk_invariants"]["checks"]["position_caps"]["status"] == "pass"
    assert bundle["risk_invariants"]["checks"]["turnover_caps"]["status"] == "pass"
    assert bundle["risk_invariants"]["checks"]["regime_guardrails"]["status"] != "disabled"
    assert bundle["risk_invariants"]["checks"]["drawdown_kill_switch"]["status"] != "disabled"
