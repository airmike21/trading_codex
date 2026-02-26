import numpy as np
import pandas as pd

from scripts.run_backtest import build_dual_actions, build_dual_tracker_actions
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.base import Strategy


def make_panel(close_map: dict[str, pd.Series]) -> pd.DataFrame:
    frames = {}
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


class AlwaysHoldA(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=bars.index, columns=["A"])


def make_regime_shift_close(index: pd.DatetimeIndex) -> pd.Series:
    returns = pd.Series(0.0005, index=index, dtype=float)
    volatile_mask = index >= pd.Timestamp("2020-04-01")
    alt = np.where(np.arange(int(volatile_mask.sum())) % 2 == 0, 0.03, -0.025)
    returns.loc[volatile_mask] = alt
    return 100.0 * (1.0 + returns).cumprod()


def test_resize_actions_emitted_for_overlay_updates():
    idx = pd.date_range("2020-01-01", "2020-08-31", freq="B")
    bars = make_panel({"A": make_regime_shift_close(idx)})

    result = run_backtest(
        bars,
        AlwaysHoldA(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.10,
        vol_max=2.0,
        vol_update="rebalance",
        rebalance_cadence="M",
    )
    assert isinstance(result.weights, pd.DataFrame)

    actions = build_dual_actions(
        bars,
        result.weights,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    resize_rows = actions[actions["action"] == "RESIZE"]
    assert not resize_rows.empty
    assert (resize_rows["from_symbol"] == "A").all()
    assert (resize_rows["to_symbol"] == "A").all()

    tracker_actions = build_dual_tracker_actions(bars, actions)
    resize_tracker_rows = tracker_actions[tracker_actions["action"] == "RESIZE"]
    assert not resize_tracker_rows.empty
    assert resize_tracker_rows["notes"].str.contains(r"Target shares \d+->\d+").all()


def test_resize_actions_not_emitted_when_overlay_disabled():
    idx = pd.date_range("2020-01-01", "2020-08-31", freq="B")
    bars = make_panel({"A": make_regime_shift_close(idx)})

    result = run_backtest(
        bars,
        AlwaysHoldA(),
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=None,
    )
    assert isinstance(result.weights, pd.DataFrame)

    actions = build_dual_actions(bars, result.weights)
    assert "RESIZE" not in set(actions["action"])


def test_enter_exit_rotate_unchanged():
    idx = pd.date_range("2020-01-01", periods=6, freq="B")
    close_a = pd.Series(np.linspace(100.0, 105.0, len(idx)), index=idx)
    close_b = pd.Series(np.linspace(90.0, 95.0, len(idx)), index=idx)
    bars = make_panel({"A": close_a, "B": close_b})

    weights = pd.DataFrame(0.0, index=idx, columns=["A", "B"], dtype=float)
    weights.loc[idx[1:3], "A"] = 1.0
    weights.loc[idx[3], "B"] = 1.0

    actions = build_dual_actions(bars, weights)
    assert actions["action"].tolist() == ["ENTER", "ROTATE", "EXIT"]
    assert actions["from_symbol"].tolist() == ["CASH", "A", "B"]
    assert actions["to_symbol"].tolist() == ["A", "B", "CASH"]
