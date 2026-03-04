import numpy as np
import pandas as pd

from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.base import Strategy


class ConstantTwoLegSignals(Strategy):
    def __init__(self, start_pos: int = 40) -> None:
        self.start_pos = int(start_pos)

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        idx = bars.index
        out = pd.DataFrame(0.0, index=idx, columns=["A", "B"], dtype=float)
        out.loc[idx[self.start_pos :], ["A", "B"]] = 0.5
        return out


def _ohlcv(close: pd.Series) -> pd.DataFrame:
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


def make_multi_bars(index: pd.DatetimeIndex) -> pd.DataFrame:
    ret_a = np.full(len(index), 0.001)
    ret_b = np.where(np.arange(len(index)) % 2 == 0, 0.03, -0.025)

    close_a = pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=index)
    close_b = pd.Series(100.0 * np.cumprod(1.0 + ret_b), index=index)

    return pd.concat(
        {
            "A": _ohlcv(close_a),
            "B": _ohlcv(close_b),
        },
        axis=1,
    )


def test_engine_ivol_applies_without_vol_target_and_changes_two_leg_weights():
    idx = pd.date_range("2020-01-01", periods=80, freq="B")
    first_active = idx[40]
    bars = make_multi_bars(idx)
    strategy = ConstantTwoLegSignals(start_pos=40)

    no_ivol = run_backtest(
        bars,
        strategy,
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=None,
        ivol=False,
    )
    with_ivol = run_backtest(
        bars,
        strategy,
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=None,
        ivol=True,
        ivol_lookback=20,
        ivol_eps=1e-8,
    )

    assert isinstance(no_ivol.weights, pd.DataFrame)
    assert isinstance(with_ivol.weights, pd.DataFrame)
    assert no_ivol.leverage is None
    assert with_ivol.leverage is None

    assert bool(np.isclose(no_ivol.weights.loc[first_active, "A"], 0.5, atol=1e-12))
    assert bool(np.isclose(no_ivol.weights.loc[first_active, "B"], 0.5, atol=1e-12))

    w_a = float(with_ivol.weights.loc[first_active, "A"])
    w_b = float(with_ivol.weights.loc[first_active, "B"])
    assert w_a > w_b
    assert bool(np.isclose(w_a + w_b, 1.0, atol=1e-12))

    active = with_ivol.weights.sum(axis=1) > 0.0
    assert bool(np.isclose(with_ivol.weights.loc[active].sum(axis=1), 1.0, atol=1e-12).all())

    changed = with_ivol.weights.ne(with_ivol.weights.shift(1)).any(axis=1)
    changed.iloc[0] = False
    changed_dates = list(with_ivol.weights.index[changed])
    assert changed_dates == [first_active]
