import json

import numpy as np
import pandas as pd

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.base import Strategy


class AlwaysHoldA(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=bars.index, columns=["A"])


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


def make_regime_shift_close(index: pd.DatetimeIndex) -> pd.Series:
    returns = pd.Series(0.0005, index=index, dtype=float)
    volatile_mask = index >= pd.Timestamp("2020-04-01")
    alt = np.where(np.arange(int(volatile_mask.sum())) % 2 == 0, 0.03, -0.025)
    returns.loc[volatile_mask] = alt
    return 100.0 * (1.0 + returns).cumprod()


def test_next_action_json_is_single_line_and_parseable_and_has_resize_fields():
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
    assert result.leverage is not None

    actions = build_dual_actions(
        bars,
        result.weights,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    resize_rows = actions[actions["action"] == "RESIZE"]
    assert not resize_rows.empty

    resize_dt = pd.to_datetime(resize_rows.iloc[0]["date"])
    bars_upto = bars.loc[:resize_dt]
    weights_upto = result.weights.loc[:resize_dt]
    actions_upto = build_dual_actions(
        bars_upto,
        weights_upto,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    payload = build_next_action_payload(
        strategy_label="dual_mom",
        bars=bars_upto,
        weights=weights_upto,
        actions=actions_upto,
        resize_rebalance="M",
        next_rebalance="M",
        vol_target=0.10,
        vol_update="rebalance",
        latest_leverage=float(result.leverage.loc[resize_dt]),
        leverage_last_update_date=resize_dt.date().isoformat(),
    )
    output = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    assert "\n" not in output

    obj = json.loads(output)
    assert obj["action"] == "RESIZE"
    assert isinstance(obj["resize_prev_shares"], int)
    assert isinstance(obj["resize_new_shares"], int)
    assert obj["resize_prev_shares"] != obj["resize_new_shares"]
    assert obj["next_rebalance"] is not None
    assert obj["leverage"] is not None


def test_next_action_json_no_stale_resize_on_next_day():
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
    assert result.leverage is not None

    actions = build_dual_actions(
        bars,
        result.weights,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    resize_rows = actions[actions["action"] == "RESIZE"]
    assert not resize_rows.empty
    resize_dt = pd.to_datetime(resize_rows.iloc[0]["date"])
    resize_pos = idx.get_loc(resize_dt)
    if isinstance(resize_pos, slice):
        resize_pos = int(resize_pos.start or 0)
    assert int(resize_pos) + 1 < len(idx)
    next_dt = idx[int(resize_pos) + 1]

    bars_after = bars.loc[:next_dt]
    weights_after = result.weights.loc[:next_dt]
    actions_after = build_dual_actions(
        bars_after,
        weights_after,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    payload = build_next_action_payload(
        strategy_label="dual_mom",
        bars=bars_after,
        weights=weights_after,
        actions=actions_after,
        resize_rebalance="M",
        next_rebalance="M",
        vol_target=0.10,
        vol_update="rebalance",
        latest_leverage=float(result.leverage.loc[next_dt]),
        leverage_last_update_date=resize_dt.date().isoformat(),
    )
    output = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    obj = json.loads(output)
    assert obj["action"] == "HOLD"
    assert obj["resize_prev_shares"] is None
    assert obj["resize_new_shares"] is None



def test_next_action_flags_mutually_exclusive(monkeypatch, capsys):
    # Passing both flags should cause argparse to exit with an error.
    import pytest
    from scripts.run_backtest import main

    argv = [
        "run_backtest.py",
        "--strategy", "dual_mom",
        "--symbols", "SPY", "QQQ", "IWM", "EFA",
        "--defensive", "TLT",
        "--start", "2005-01-01",
        "--end", "2005-05-02",
        "--no-plot",
        "--next-action",
        "--next-action-json",
        "--vol-target", "0.10",
        "--vol-update", "rebalance",
    ]

    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code != 0
    out = capsys.readouterr()
    # argparse typically emits to stderr
    assert "not allowed with argument" in (out.err + out.out)

