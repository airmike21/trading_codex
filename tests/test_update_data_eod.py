from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import requests

from scripts import update_data_eod
from trading_codex.data import LocalStore
from trading_codex.data.providers import StooqDataSource
from trading_codex.data.providers.tastytrade import TastytradeDataSource


def _df_for_dates(dates: list[str], base: float = 100.0) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    closes = [base + i for i in range(len(idx))]
    return pd.DataFrame(
        {
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": [1000.0] * len(idx),
        },
        index=idx,
    )


def test_extract_symbols_from_args() -> None:
    args = [
        "--strategy",
        "dual_mom_vol10_cash",
        "--symbols",
        "SPY",
        "QQQ",
        "IWM",
        "--dmv-defensive-symbol",
        "BIL",
    ]
    assert update_data_eod._extract_symbols_from_args(args) == ["SPY", "QQQ", "IWM", "BIL"]


def test_load_presets_symbols(tmp_path: Path) -> None:
    p = tmp_path / "presets.json"
    p.write_text(
        """
{
  "presets": {
    "a": {"run_backtest_args": ["--symbols", "spy", "qqq", "--vm-defensive-symbol", "shy"]},
    "b": {"run_backtest_args": ["--symbols", "QQQ", "IWM", "--defensive", "tlt"]},
    "c": {"run_backtest_args": ["--symbols", "EFA", "--dmv-defensive-symbol", "bil"]}
  }
}
""".strip(),
        encoding="utf-8",
    )
    assert update_data_eod._load_presets_symbols(p) == ["SPY", "QQQ", "SHY", "IWM", "TLT", "EFA", "BIL"]


def test_merge_existing_dedup_last_wins() -> None:
    old = _df_for_dates(["2024-01-01", "2024-01-02"], base=10.0)
    new = _df_for_dates(["2024-01-02", "2024-01-03"], base=50.0)
    merged = update_data_eod._merge_existing(old, new)
    assert list(merged.index.strftime("%Y-%m-%d")) == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert float(merged.loc[pd.Timestamp("2024-01-02"), "open"]) == 50.0


def test_main_requires_tiingo_key(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    rc = update_data_eod.main([
        "--provider",
        "tiingo",
        "--data-dir",
        str(tmp_path / "data"),
        "--symbols",
        "SPY",
    ])
    assert rc == 2


def test_fetch_tiingo_bars_uses_requests_and_returns_ohlcv(monkeypatch) -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, object]]:
            return [
                {
                    "date": "2024-01-02T00:00:00.000Z",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 12345.0,
                },
                {
                    "date": "2024-01-03T00:00:00.000Z",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 23456.0,
                },
            ]

    seen: dict[str, object] = {}

    def fake_get(url: str, *, params: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        seen["url"] = url
        seen["params"] = dict(params)
        seen["headers"] = dict(headers)
        seen["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(update_data_eod.requests, "get", fake_get)

    df = update_data_eod._fetch_tiingo_bars(
        "SPY",
        date(2024, 1, 2),
        date(2024, 1, 3),
        "test-tiingo-key",
        12.5,
    )

    assert seen["url"] == "https://api.tiingo.com/tiingo/daily/SPY/prices"
    assert seen["params"] == {
        "startDate": "2024-01-02",
        "endDate": "2024-01-03",
        "resampleFreq": "daily",
        "format": "json",
    }
    assert seen["headers"] == {"Authorization": "Token test-tiingo-key"}
    assert seen["timeout"] == 12.5
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert list(df.index.strftime("%Y-%m-%d")) == ["2024-01-02", "2024-01-03"]
    assert float(df.loc[pd.Timestamp("2024-01-03"), "close"]) == 101.5


def test_main_stooq_writes_localstore(monkeypatch, tmp_path: Path) -> None:
    def fake_fetch(symbol: str, start: date, end: date, suffix: str, timeout: float) -> pd.DataFrame:
        assert symbol == "AAA"
        return _df_for_dates(["2024-01-01", "2024-01-02", "2024-01-03"], base=100.0)

    monkeypatch.setattr(update_data_eod, "_fetch_stooq_bars", fake_fetch)

    data_dir = tmp_path / "data"
    rc = update_data_eod.main([
        "--provider",
        "stooq",
        "--data-dir",
        str(data_dir),
        "--symbols",
        "AAA",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-03",
    ])
    assert rc == 0

    store = LocalStore(base_dir=data_dir)
    df = store.read_bars("AAA")
    assert len(df) == 3


def test_fetch_stooq_bars_uses_current_stooq_download_export_and_not_tastytrade(monkeypatch) -> None:
    assert update_data_eod.StooqDataSource.__module__ == "trading_codex.data.providers.stooq"

    def fail_if_called(*args, **kwargs) -> pd.DataFrame:
        raise AssertionError("tastytrade provider should not be called for --provider stooq")

    class FakeResponse:
        def __init__(self, text: str) -> None:
            self.text = text
            self.status_code = 200
            self.headers = {"content-type": "text/csv"}
            self.url = "https://stooq.com/q/l/?s=spy.us&f=sd2t2ohlcv&h=&e=csv"

        def raise_for_status(self) -> None:
            return None

    def fake_get(
        self,  # noqa: ANN001
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
    ) -> FakeResponse:
        assert url == StooqDataSource.BASE_URL
        assert params == {"s": "spy.us", "f": "sd2t2ohlcv", "h": "", "e": "csv"}
        assert timeout == 9.5
        return FakeResponse(
            "Symbol,Date,Time,Open,High,Low,Close,Volume\n"
            "SPY.US,2024-01-02,22:00:21,100,101,99,100.5,12345\n"
            "SPY.US,2024-01-03,22:00:21,101,102,100,101.5,23456\n"
        )

    monkeypatch.setattr(TastytradeDataSource, "get_daily_bars", fail_if_called)
    monkeypatch.setattr(requests.sessions.Session, "get", fake_get)

    df = update_data_eod._fetch_stooq_bars(
        "SPY",
        date(2024, 1, 2),
        date(2024, 1, 3),
        ".us",
        9.5,
    )

    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert list(df.index.strftime("%Y-%m-%d")) == ["2024-01-02", "2024-01-03"]
    assert float(df.loc[pd.Timestamp("2024-01-03"), "close"]) == 101.5


def test_stooq_provider_empty_response_raises_explicit_error(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self) -> None:
            self.text = ""
            self.status_code = 200
            self.headers = {"content-type": "text/html"}
            self.url = "https://stooq.com/q/l/?s=spy.us&f=sd2t2ohlcv&h=&e=csv"

        def raise_for_status(self) -> None:
            return None

    provider = StooqDataSource(timeout=1.0)
    monkeypatch.setattr(provider.session, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(ValueError, match="empty response body"):
        provider.get_daily_bars(["SPY"], pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"))


def test_fetch_stooq_bars_empty_response_raises_explicit_error(monkeypatch) -> None:
    def fail_if_called(*args, **kwargs) -> pd.DataFrame:
        raise AssertionError("tastytrade provider should not be called for --provider stooq")

    class FakeResponse:
        def __init__(self) -> None:
            self.text = ""
            self.status_code = 200
            self.headers = {"content-type": "text/html"}
            self.url = "https://stooq.com/q/l/?s=spy.us&f=sd2t2ohlcv&h=&e=csv"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(TastytradeDataSource, "get_daily_bars", fail_if_called)
    monkeypatch.setattr(requests.sessions.Session, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(ValueError, match="empty response body"):
        update_data_eod._fetch_stooq_bars(
            "SPY",
            date(2024, 1, 1),
            date(2024, 1, 2),
            ".us",
            1.0,
        )


def test_main_stooq_fetch_error_is_explicit_and_nonzero(monkeypatch, tmp_path: Path, capsys) -> None:
    def fake_fetch(symbol: str, start: date, end: date, suffix: str, timeout: float) -> pd.DataFrame:
        raise ValueError(
            "Stooq returned an empty response body for symbol 'SPY' "
            "(url=https://stooq.com/q/l/?s=spy.us&f=sd2t2ohlcv&h=&e=csv, status=200, content_type=text/html)."
        )

    monkeypatch.setattr(update_data_eod, "_fetch_stooq_bars", fake_fetch)

    rc = update_data_eod.main([
        "--provider",
        "stooq",
        "--data-dir",
        str(tmp_path / "data"),
        "--symbols",
        "SPY",
    ])

    assert rc == 2
    captured = capsys.readouterr()
    assert "provider=stooq fetch failed" in captured.err
    assert "empty response body" in captured.err


def test_main_uses_presets_when_symbols_omitted(monkeypatch, tmp_path: Path) -> None:
    def fake_fetch(symbol: str, start: date, end: date, suffix: str, timeout: float) -> pd.DataFrame:
        return _df_for_dates(["2024-01-01"], base=200.0)

    monkeypatch.setattr(update_data_eod, "_fetch_stooq_bars", fake_fetch)

    presets = tmp_path / "presets.json"
    presets.write_text(
        """
{
  "presets": {
    "vm_core": {
      "run_backtest_args": ["--strategy", "valmom_v1", "--symbols", "AAA", "BBB", "--vm-defensive-symbol", "BIL"]
    }
  }
}
""".strip(),
        encoding="utf-8",
    )

    data_dir = tmp_path / "data"
    rc = update_data_eod.main([
        "--provider",
        "stooq",
        "--data-dir",
        str(data_dir),
        "--presets-file",
        str(presets),
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-01",
    ])
    assert rc == 0

    store = LocalStore(base_dir=data_dir)
    assert len(store.read_bars("AAA")) == 1
    assert len(store.read_bars("BBB")) == 1
    assert len(store.read_bars("BIL")) == 1
