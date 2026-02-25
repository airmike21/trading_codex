import numpy as np
import pandas as pd
import pandas.testing as pdt

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
    assert float(base.leverage.max()) <= 0.7 + 1e-12
    assert float(base.leverage.min()) >= 0.0

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
