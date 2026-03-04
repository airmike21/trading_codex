import pandas as pd

from trading_codex.backtest.next_rebalance import compute_next_rebalance_date


def test_compute_next_rebalance_trading_days_within_index_window():
    idx = pd.date_range("2020-01-01", periods=50, freq="B")
    p = 17
    r = 10
    current = idx[p]
    next_pos = ((p // r) + 1) * r

    got = compute_next_rebalance_date(idx, current, trading_days=r)
    assert got == idx[next_pos].date().isoformat()


def test_compute_next_rebalance_trading_days_beyond_end_uses_bday_offset():
    idx = pd.date_range("2020-01-01", periods=50, freq="B")
    p = 47
    r = 10
    current = idx[p]
    bars_remaining = r - ((p + 1) % r)
    if bars_remaining == 0:
        bars_remaining = r

    got = compute_next_rebalance_date(idx, current, trading_days=r)
    expected = (current + pd.offsets.BDay(bars_remaining)).date().isoformat()
    assert got == expected


def test_compute_next_rebalance_monthly_cadence():
    idx = pd.date_range("2020-01-01", periods=50, freq="B")
    current = pd.Timestamp("2020-01-15")
    got = compute_next_rebalance_date(idx, current, cadence="M")
    expected = (current + pd.offsets.BMonthEnd(1)).date().isoformat()
    assert got == expected


def test_compute_next_rebalance_weekly_cadence():
    idx = pd.date_range("2020-01-01", periods=50, freq="B")
    current = pd.Timestamp("2020-01-08")
    got = compute_next_rebalance_date(idx, current, cadence="W")
    expected = (current + pd.offsets.Week(weekday=4)).date().isoformat()
    assert got == expected

