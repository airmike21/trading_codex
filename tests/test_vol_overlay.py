import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pdt

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.backtest.engine import run_backtest
from trading_codex.backtest.vol_overlay import (
    apply_vol_target_overlay,
    compute_leverage_series,
    compute_portfolio_returns_1x,
)
from trading_codex.data import LocalStore
from trading_codex.strategies.base import Strategy


class StepSignal(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        signal = pd.Series(0.0, index=bars.index)
        signal.iloc[3:10] = 1.0
        signal.iloc[10:] = -1.0
        return pd.DataFrame({"signal": signal}, index=bars.index)


class AlwaysLong(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"signal": 1.0}, index=bars.index)


class AlwaysHoldA(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=bars.index, columns=["A"])


def make_single_bars(close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000,
        },
        index=close.index,
    )


def make_panel(close_map: dict[str, pd.Series]) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for symbol, close in close_map.items():
        frames[symbol] = make_single_bars(close)
    return pd.concat(frames, axis=1)


def _expected_event_id(obj: dict[str, object]) -> str:
    def g(key: str) -> str:
        value = obj.get(key, "")
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


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _write_valmom_cli_store(base_dir: Path, idx: pd.DatetimeIndex) -> None:
    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", make_single_bars(pd.Series(np.linspace(100.0, 190.0, len(idx)), index=idx)))
    store.write_bars("BBB", make_single_bars(pd.Series(np.linspace(140.0, 95.0, len(idx)), index=idx)))
    store.write_bars("CCC", make_single_bars(pd.Series(np.linspace(90.0, 150.0, len(idx)), index=idx)))
    store.write_bars("SHY", make_single_bars(pd.Series(np.linspace(95.0, 100.0, len(idx)), index=idx)))


def test_compute_portfolio_returns_1x_uses_lagged_weights_without_lookahead():
    idx = pd.date_range("2020-01-01", periods=4, freq="B")
    raw_weights = pd.DataFrame(
        {
            "A": [1.0, 0.5, 0.5, 0.0],
            "B": [0.0, 0.5, 0.5, 1.0],
        },
        index=idx,
    )
    asset_returns = pd.DataFrame(
        {
            "A": [0.10, 0.01, 0.02, 0.03],
            "B": [0.00, 0.04, -0.01, 0.05],
        },
        index=idx,
    )

    got = compute_portfolio_returns_1x(raw_weights, asset_returns)
    expected = pd.Series([0.0, 0.01, 0.005, 0.04], index=idx, dtype=float)
    pdt.assert_series_equal(got, expected)


def test_compute_leverage_series_clamps_and_handles_zero_vol_deterministically():
    realized_vol = pd.Series([0.20, 0.05, 0.0, np.nan], dtype=float)

    got = compute_leverage_series(
        realized_vol,
        target_vol=0.10,
        min_leverage=0.25,
        max_leverage=1.5,
    )

    expected = pd.Series([0.5, 1.5, 1.5, 1.0], dtype=float)
    pdt.assert_series_equal(got, expected)


def test_compute_leverage_series_zero_target_zero_vol_uses_min_leverage():
    realized_vol = pd.Series([0.0, 0.0], dtype=float)

    got = compute_leverage_series(
        realized_vol,
        target_vol=0.0,
        min_leverage=0.25,
        max_leverage=1.5,
    )

    expected = pd.Series([0.25, 0.25], dtype=float)
    pdt.assert_series_equal(got, expected)


def test_apply_vol_target_overlay_scales_raw_weights_by_leverage_series():
    idx = pd.date_range("2020-01-01", periods=6, freq="B")
    raw_weights = pd.DataFrame(
        {
            "A": [0.6] * len(idx),
            "B": [0.4] * len(idx),
        },
        index=idx,
        dtype=float,
    )
    asset_returns = pd.DataFrame(
        {
            "A": [0.0, 0.04, -0.03, 0.05, -0.04, 0.05],
            "B": [0.0, 0.01, -0.015, 0.02, -0.01, 0.015],
        },
        index=idx,
        dtype=float,
    )

    scaled_weights, leverage, _ = apply_vol_target_overlay(
        raw_weights,
        asset_returns,
        target_vol=0.10,
        lookback=3,
        min_leverage=0.0,
        max_leverage=1.0,
    )

    expected = raw_weights.mul(leverage, axis=0)
    pdt.assert_frame_equal(scaled_weights, expected)
    assert float(leverage.iloc[-1]) < 1.0


def test_vol_overlay_disabled_matches_baseline_exact():
    idx = pd.date_range("2020-01-01", periods=15, freq="B")
    close = pd.Series(np.linspace(100.0, 112.0, len(idx)), index=idx)
    bars = make_single_bars(close)

    baseline = run_backtest(bars, StepSignal(), slippage_bps=0.0, commission_bps=0.0)
    disabled_overlay = run_backtest(
        bars,
        StepSignal(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=None,
        vol_lookback=5,
        vol_min=0.2,
        vol_max=0.8,
    )

    pdt.assert_series_equal(disabled_overlay.returns, baseline.returns)
    pdt.assert_series_equal(disabled_overlay.weights, baseline.weights)
    pdt.assert_series_equal(disabled_overlay.turnover, baseline.turnover)
    pdt.assert_series_equal(disabled_overlay.equity, baseline.equity)
    assert disabled_overlay.leverage is None
    assert disabled_overlay.realized_vol is None


def test_vol_overlay_scales_down_when_realized_vol_above_target():
    idx = pd.date_range("2020-01-01", periods=80, freq="B")
    alternating = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    close = pd.Series(100.0 * np.cumprod(1.0 + alternating), index=idx)
    bars = make_single_bars(close)

    result = run_backtest(
        bars,
        AlwaysLong(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.0,
        vol_max=1.0,
        vol_update="daily",
    )
    assert result.leverage is not None
    assert result.realized_vol is not None

    high_vol_mask = result.realized_vol > 0.10
    assert bool(high_vol_mask.any())
    assert bool((result.leverage.loc[high_vol_mask] < 1.0).any())


def test_vol_overlay_capped_and_no_lookahead_leverage():
    idx = pd.date_range("2020-01-01", periods=45, freq="B")
    close = pd.Series(
        100.0
        + np.cumsum(
            np.array(
                [
                    0.8,
                    -0.5,
                    1.1,
                    -0.7,
                    0.9,
                    -0.6,
                    1.2,
                    -0.8,
                    0.7,
                ]
                * 5
            )
        ),
        index=idx,
    )
    bars = make_single_bars(close)

    base = run_backtest(
        bars,
        AlwaysLong(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.0,
        vol_max=0.7,
        vol_update="daily",
    )
    assert base.leverage is not None
    assert base.realized_vol is not None
    mature = base.realized_vol.notna()
    assert bool(mature.any())
    assert float(base.leverage.loc[mature].max()) <= 0.7 + 1e-12
    assert float(base.leverage.loc[mature].min()) >= 0.0

    probe_pos = 20
    altered_pos = probe_pos + 1
    altered_dt = idx[altered_pos]
    altered_bars = bars.copy()
    altered_bars.loc[altered_dt, ["open", "high", "low", "close"]] *= 1.5

    altered = run_backtest(
        altered_bars,
        AlwaysLong(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.0,
        vol_max=0.7,
        vol_update="daily",
    )
    assert altered.leverage is not None

    assert np.isclose(base.leverage.iloc[probe_pos], altered.leverage.iloc[probe_pos], atol=1e-12)


def test_vol_overlay_zero_vol_uses_max_leverage_without_crash():
    idx = pd.date_range("2020-01-01", periods=30, freq="B")
    close = pd.Series(100.0, index=idx, dtype=float)
    bars = make_single_bars(close)

    result = run_backtest(
        bars,
        AlwaysLong(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.25,
        vol_max=1.0,
        vol_update="daily",
    )
    assert result.leverage is not None
    assert result.realized_vol is not None

    assert np.isclose(float(result.leverage.iloc[0]), 1.0, atol=1e-12)
    mature = result.realized_vol.notna()
    assert bool(mature.any())
    assert bool((result.realized_vol.loc[mature] <= 1e-12).all())
    assert bool(np.isclose(result.leverage.loc[mature], 1.0, atol=1e-12).all())


def test_apply_vol_target_overlay_rebalance_updates_only_change_on_mask():
    idx = pd.date_range("2020-01-01", periods=10, freq="B")
    raw_weights = pd.Series(1.0, index=idx, dtype=float)
    asset_returns = pd.Series([0.0, 0.01, -0.01, 0.02, -0.02, 0.03, -0.03, 0.01, -0.01, 0.02], index=idx)
    update_mask = pd.Series(False, index=idx, dtype=bool)
    update_mask.iloc[[3, 6, 9]] = True

    _, leverage, realized_vol = apply_vol_target_overlay(
        raw_weights,
        asset_returns,
        target_vol=0.10,
        lookback=3,
        min_leverage=0.0,
        max_leverage=1.0,
        update_mask=update_mask,
    )

    assert realized_vol.notna().any()
    leverage_changes = leverage.diff().abs().fillna(0.0) > 1e-12
    unexpected_changes = leverage_changes & ~update_mask
    assert not bool(unexpected_changes.any())


def test_vol_overlay_rebalance_update_constant_weights_has_no_daily_turnover():
    idx = pd.date_range("2020-01-01", periods=40, freq="B")
    close = pd.Series(np.linspace(100.0, 110.0, len(idx)), index=idx)
    bars = make_single_bars(close)

    result = run_backtest(
        bars,
        AlwaysLong(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.0,
        vol_max=1.0,
        vol_update="rebalance",
        rebalance_cadence="W",
    )
    assert result.leverage is not None

    expected_update_mask = pd.Series(False, index=idx, dtype=bool)
    for idx_pos in range(len(idx) - 1):
        if idx[idx_pos].weekday() == 4:
            expected_update_mask.iloc[idx_pos + 1] = True

    leverage_changes = result.leverage.diff().abs().fillna(0.0) > 1e-12
    unexpected_changes = leverage_changes & ~expected_update_mask
    assert not bool(unexpected_changes.any())

    assert float(result.turnover.sum()) == 0.0


def test_vol_overlay_changes_target_shares_through_pipeline():
    idx = pd.date_range("2020-01-01", periods=90, freq="B")
    alternating = np.where(np.arange(len(idx)) % 2 == 0, 0.03, -0.025)
    close = pd.Series(100.0 * np.cumprod(1.0 + alternating), index=idx)
    bars = make_panel({"A": close})

    plain = run_backtest(
        bars,
        AlwaysHoldA(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=None,
    )
    overlay = run_backtest(
        bars,
        AlwaysHoldA(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.0,
        vol_max=1.0,
        vol_update="daily",
        rebalance_cadence="M",
    )

    assert isinstance(plain.weights, pd.DataFrame)
    assert isinstance(overlay.weights, pd.DataFrame)
    assert overlay.leverage is not None
    assert overlay.realized_vol is not None

    plain_actions = build_dual_actions(bars, plain.weights)
    overlay_actions = build_dual_actions(
        bars,
        overlay.weights,
        vol_target=0.10,
        vol_update="daily",
        rebalance="M",
    )

    plain_payload = build_next_action_payload(
        strategy_label="dual_mom",
        bars=bars,
        weights=plain.weights,
        actions=plain_actions,
        resize_rebalance="M",
        next_rebalance="M",
    )
    overlay_payload = build_next_action_payload(
        strategy_label="dual_mom",
        bars=bars,
        weights=overlay.weights,
        actions=overlay_actions,
        resize_rebalance="M",
        next_rebalance="M",
        vol_target=0.10,
        vol_lookback=5,
        vol_update="daily",
        latest_realized_vol=float(overlay.realized_vol.iloc[-1]),
        latest_leverage=float(overlay.leverage.iloc[-1]),
        leverage_last_update_date=idx[-1].date().isoformat(),
    )

    assert isinstance(plain_payload["target_shares"], int)
    assert isinstance(overlay_payload["target_shares"], int)
    assert overlay_payload["target_shares"] < plain_payload["target_shares"]
    assert overlay_payload["vol_lookback"] == 5
    assert overlay_payload["lookback"] == 5
    assert overlay_payload["realized_vol"] is not None
    assert overlay_payload["event_id"] == _expected_event_id(overlay_payload)


def test_run_backtest_cli_vol_target_flag_enables_default_overlay_and_preserves_event_id(tmp_path: Path):
    idx = pd.date_range("2020-01-01", periods=420, freq="B")
    _write_valmom_cli_store(tmp_path, idx)
    repo_root, env = _repo_root_and_env()

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        "--strategy",
        "valmom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "CCC",
        "--vm-defensive-symbol",
        "SHY",
        "--vm-mom-lookback",
        "63",
        "--vm-val-lookback",
        "126",
        "--vm-top-n",
        "2",
        "--vm-rebalance",
        "21",
        "--vol-target",
        "--vol-lookback",
        "63",
        "--start",
        idx[0].date().isoformat(),
        "--end",
        idx[-1].date().isoformat(),
        "--no-plot",
        "--next-action-json",
        "--data-dir",
        str(tmp_path),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    lines = proc.stdout.splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["vol_target"] == 0.10
    assert obj["vol_lookback"] == 63
    assert obj["lookback"] == 63
    assert obj["leverage"] is not None
    assert obj["realized_vol"] is not None
    assert obj["event_id"] == _expected_event_id(obj)
