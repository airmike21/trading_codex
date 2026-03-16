import numpy as np
import pandas as pd
import pandas.testing as pdt

from scripts.run_backtest import compute_extended_metrics
from trading_codex.backtest.costs import compute_trade_count, compute_turnover, estimate_transaction_costs
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.base import Strategy


class RotateWeights(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        idx = bars.index
        out = pd.DataFrame(0.0, index=idx, columns=["A", "B"])
        out.loc[idx[1:3], "A"] = 1.0
        out.loc[idx[3], "B"] = 1.0
        return out


def make_multi_bars() -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=4, freq="B")
    close_a = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
    close_b = pd.Series([100.0, 100.0, 100.0, 100.0], index=idx)

    def ohlcv(close: pd.Series) -> pd.DataFrame:
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

    return pd.concat({"A": ohlcv(close_a), "B": ohlcv(close_b)}, axis=1)


def test_compute_turnover_and_trade_count_for_portfolio_weights() -> None:
    idx = pd.date_range("2020-01-01", periods=4, freq="B")
    weights = pd.DataFrame(
        {
            "A": [0.0, 1.0, 1.0, 0.0],
            "B": [0.0, 0.0, 0.0, 1.0],
        },
        index=idx,
        dtype=float,
    )

    turnover = compute_turnover(weights)
    trade_count = compute_trade_count(weights)

    expected_turnover = pd.Series([0.0, 1.0, 0.0, 2.0], index=idx, dtype=float)
    expected_trade_count = pd.Series([0, 1, 0, 2], index=idx, dtype=int)

    pdt.assert_series_equal(turnover, expected_turnover)
    pdt.assert_series_equal(trade_count, expected_trade_count)


def test_estimate_transaction_costs_matches_slippage_and_commission_math() -> None:
    idx = pd.date_range("2020-01-01", periods=3, freq="B")
    turnover = pd.Series([0.0, 0.5, 2.0], index=idx, dtype=float)
    trade_count = pd.Series([0, 1, 2], index=idx, dtype=int)

    estimate = estimate_transaction_costs(
        turnover,
        trade_count,
        slippage_bps=5.0,
        commission_bps=1.0,
        commission_per_trade=1.25,
    )

    pdt.assert_series_equal(estimate.traded_notional, pd.Series([0.0, 5_000.0, 20_000.0], index=idx))
    pdt.assert_series_equal(estimate.slippage_cost, pd.Series([0.0, 2.5, 10.0], index=idx))
    pdt.assert_series_equal(estimate.commission_cost, pd.Series([0.0, 1.75, 4.5], index=idx))
    pdt.assert_series_equal(estimate.total_cost, pd.Series([0.0, 4.25, 14.5], index=idx))
    pdt.assert_series_equal(estimate.cost_return, pd.Series([0.0, 0.000425, 0.00145], index=idx))


def test_zero_turnover_produces_zero_costs() -> None:
    idx = pd.date_range("2020-01-01", periods=3, freq="B")
    turnover = pd.Series(0.0, index=idx, dtype=float)

    estimate = estimate_transaction_costs(turnover, slippage_bps=5.0, commission_per_trade=2.0)

    assert float(estimate.total_cost.sum()) == 0.0
    assert float(estimate.cost_return.sum()) == 0.0
    assert int(estimate.trade_count.sum()) == 0


def test_run_backtest_exposes_gross_and_net_cost_metrics_for_portfolios() -> None:
    bars = make_multi_bars()
    result = run_backtest(
        bars,
        RotateWeights(),
        slippage_bps=5.0,
        commission_bps=0.0,
        commission_per_trade=1.0,
    )

    expected_gross = pd.Series(
        [0.0, 0.01, (102.0 / 101.0) - 1.0, 0.0],
        index=bars.index,
        dtype=float,
    )
    expected_cost_returns = pd.Series([0.0, 0.0006, 0.0, 0.0012], index=bars.index, dtype=float)
    expected_net = expected_gross - expected_cost_returns
    expected_cost_dollars = pd.Series([0.0, 6.0, 0.0, 12.0], index=bars.index, dtype=float)
    expected_trade_count = pd.Series([0, 1, 0, 2], index=bars.index, dtype=int)

    pdt.assert_series_equal(result.gross_returns, expected_gross)
    pdt.assert_series_equal(result.cost_returns, expected_cost_returns)
    pdt.assert_series_equal(result.returns, expected_net)
    pdt.assert_series_equal(result.estimated_costs, expected_cost_dollars)
    pdt.assert_series_equal(result.trade_count, expected_trade_count)

    extended = compute_extended_metrics(result)
    assert np.isclose(extended["gross_cagr"], float((1.0 + expected_gross).prod() ** (252.0 / len(expected_gross)) - 1.0))
    assert np.isclose(extended["net_cagr"], float((1.0 + expected_net).prod() ** (252.0 / len(expected_net)) - 1.0))
    assert extended["gross_sharpe"] > extended["net_sharpe"]
    assert np.isclose(extended["annual_turnover"], 189.0)
    assert np.isclose(extended["total_estimated_cost"], 18.0)
    assert np.isclose(extended["average_rebalance_cost"], 9.0)
