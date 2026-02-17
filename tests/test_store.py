import pandas as pd
import pandas.testing as pdt

from trading_codex.data.store import LocalStore


def _make_bars(start: str, periods: int) -> pd.DataFrame:
    idx = pd.date_range(start=start, periods=periods, freq="D")
    return pd.DataFrame(
        {
            "open": range(1, periods + 1),
            "high": range(2, periods + 2),
            "low": range(0, periods),
            "close": range(1, periods + 1),
            "volume": range(10, 10 + periods),
        },
        index=idx,
    )


def test_roundtrip_parquet(tmp_path):
    store = LocalStore(base_dir=tmp_path)
    df = _make_bars("2023-01-01", 3)
    store.write_bars("AAPL", df)
    read_df = store.read_bars("AAPL")
    pdt.assert_frame_equal(read_df, df)


def test_panel_alignment_mismatched_dates(tmp_path):
    store = LocalStore(base_dir=tmp_path)

    df_a = _make_bars("2023-01-01", 3)  # 01,02,03
    df_b = _make_bars("2023-01-02", 3)  # 02,03,04

    store.write_bars("AAA", df_a)
    store.write_bars("BBB", df_b)

    panel = store.build_panel(["AAA", "BBB"], start="2023-01-01", end="2023-01-04")

    expected_index = pd.to_datetime(
        ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]
    )
    assert panel.index.equals(expected_index)

    expected_columns = pd.MultiIndex.from_product(
        [["AAA", "BBB"], ["open", "high", "low", "close", "volume"]]
    )
    assert panel.columns.equals(expected_columns)

    assert pd.isna(panel.loc["2023-01-01", ("BBB", "open")])
    assert pd.isna(panel.loc["2023-01-04", ("AAA", "open")])
