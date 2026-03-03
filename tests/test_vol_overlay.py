import numpy as np
import pandas as pd
import pandas.testing as pdt

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.backtest.engine import run_backtest
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


def test_vol_overlay_zero_vol_uses_min_leverage_without_crash():
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
    assert bool(np.isclose(result.leverage.loc[mature], 0.25, atol=1e-12).all())


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
    assert overlay_payload["realized_vol"] is not None
