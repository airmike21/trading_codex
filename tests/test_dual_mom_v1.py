from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_codex.data import LocalStore
from trading_codex.strategies.dual_mom_v1 import DualMomentumV1Strategy


def make_panel(close_map: dict[str, pd.Series]) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for symbol, close in close_map.items():
        frames[symbol] = pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=close.index,
        )
    return pd.concat(frames, axis=1)


def _active_rows(weights: pd.DataFrame) -> pd.Series:
    return weights.sum(axis=1) > 0.0


def test_dual_mom_v1_case_a_single_positive_winner():
    idx = pd.date_range("2020-01-01", periods=30, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 130.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(120.0, 90.0, len(idx)), index=idx),
            "SHY": pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx),
        }
    )

    strat = DualMomentumV1Strategy(
        symbols=["A", "B", "SHY"],
        lookback=5,
        top_n=1,
        rebalance=5,
        defensive_symbol="SHY",
    )
    weights = strat.generate_signals(bars)
    risk_active = (weights[["A", "B"]].sum(axis=1) > 0.0)

    assert bool(risk_active.any())
    assert bool(np.isclose(weights.loc[risk_active, "A"], 1.0, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[risk_active, "B"], 0.0, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[risk_active, "SHY"], 0.0, atol=1e-12).all())


def test_dual_mom_v1_case_b_all_negative_goes_defensive():
    idx = pd.date_range("2020-01-01", periods=30, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(120.0, 80.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 70.0, len(idx)), index=idx),
            "SHY": pd.Series(np.linspace(90.0, 95.0, len(idx)), index=idx),
        }
    )

    strat = DualMomentumV1Strategy(
        symbols=["A", "B", "SHY"],
        lookback=5,
        top_n=1,
        rebalance=5,
        defensive_symbol="SHY",
    )
    weights = strat.generate_signals(bars)
    active = _active_rows(weights)

    assert bool(active.any())
    assert bool(np.isclose(weights.loc[active, "SHY"], 1.0, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[active, ["A", "B"]], 0.0, atol=1e-12).all().all())


def test_dual_mom_v1_case_c_top_n_two_equal_weight_top_two():
    idx = pd.date_range("2020-01-01", periods=30, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 150.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 135.0, len(idx)), index=idx),
            "C": pd.Series(np.linspace(100.0, 120.0, len(idx)), index=idx),
            "SHY": pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx),
        }
    )

    strat = DualMomentumV1Strategy(
        symbols=["A", "B", "C", "SHY"],
        lookback=5,
        top_n=2,
        rebalance=5,
        defensive_symbol="SHY",
    )
    weights = strat.generate_signals(bars)
    risk_active = (weights[["A", "B", "C"]].sum(axis=1) > 0.0)

    assert bool(risk_active.any())
    assert bool(np.isclose(weights.loc[risk_active, "A"], 0.5, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[risk_active, "B"], 0.5, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[risk_active, "C"], 0.0, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[risk_active, "SHY"], 0.0, atol=1e-12).all())


def test_dual_mom_v1_case_d_missing_defensive_symbol_raises():
    idx = pd.date_range("2020-01-01", periods=30, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 150.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 120.0, len(idx)), index=idx),
        }
    )

    strat = DualMomentumV1Strategy(
        symbols=["A", "B"],
        lookback=5,
        top_n=1,
        rebalance=5,
        defensive_symbol="SHY",
    )

    with pytest.raises(ValueError, match="defensive symbol: SHY"):
        strat.generate_signals(bars)


def test_dual_mom_v1_rebalance_every_five_bars_and_ffill_between():
    idx = pd.date_range("2020-01-01", periods=40, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 150.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 90.0, len(idx)), index=idx),
            "SHY": pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx),
        }
    )

    rebalance = 5
    strat = DualMomentumV1Strategy(
        symbols=["A", "B", "SHY"],
        lookback=1,
        top_n=1,
        rebalance=rebalance,
        defensive_symbol="SHY",
    )
    weights = strat.generate_signals(bars)

    changed = weights.ne(weights.shift(1)).any(axis=1)
    changed.iloc[0] = False
    changed_dates = set(weights.index[changed].tolist())
    expected_dates = {
        weights.index[i + 1]
        for i in range(rebalance - 1, len(weights.index), rebalance)
        if i + 1 < len(weights.index)
    }

    assert changed_dates.issubset(expected_dates)
    assert len(changed_dates) > 0

    sorted_updates = sorted(expected_dates)
    for idx_pos in range(len(sorted_updates) - 1):
        start = sorted_updates[idx_pos]
        end = sorted_updates[idx_pos + 1]
        span = weights.loc[start:end]
        for row_pos in range(1, len(span)):
            assert bool(np.isclose(span.iloc[row_pos], span.iloc[row_pos - 1], atol=1e-12).all())


def test_dual_mom_v1_cli_next_action_json_smoke_one_line(tmp_path):
    idx = pd.date_range("2020-01-01", periods=320, freq="B")
    close_a = pd.Series(np.linspace(100.0, 170.0, len(idx)), index=idx)
    close_b = pd.Series(np.linspace(100.0, 90.0, len(idx)), index=idx)
    close_def = pd.Series(np.linspace(90.0, 95.0, len(idx)), index=idx)

    def _bars(close: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=idx,
        )

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars(close_a))
    store.write_bars("BBB", _bars(close_b))
    store.write_bars("SHY", _bars(close_def))

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        "--strategy",
        "dual_mom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "--dm-defensive-symbol",
        "SHY",
        "--dm-lookback",
        "63",
        "--dm-top-n",
        "1",
        "--dm-rebalance",
        "21",
        "--start",
        idx[0].date().isoformat(),
        "--end",
        idx[-1].date().isoformat(),
        "--no-plot",
        "--next-action-json",
        "--data-dir",
        str(tmp_path),
    ]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"Expected 1 line, got {len(lines)}: stdout={proc.stdout!r} stderr={proc.stderr!r}"
    obj = json.loads(lines[0])

    required_keys = {
        "schema_name",
        "schema_version",
        "schema_minor",
        "date",
        "strategy",
        "action",
        "symbol",
        "target_shares",
        "event_id",
    }
    assert required_keys.issubset(obj.keys())
    assert obj["strategy"] == "dual_mom_v1"
    assert "dual_mom_v1" in str(obj["event_id"])
