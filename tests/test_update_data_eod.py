from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts import update_data_eod
from trading_codex.data import LocalStore


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
        "valmom_v1",
        "--symbols",
        "SPY",
        "QQQ",
        "IWM",
        "--vm-defensive-symbol",
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
    "b": {"run_backtest_args": ["--symbols", "QQQ", "IWM", "--defensive", "tlt"]}
  }
}
""".strip(),
        encoding="utf-8",
    )
    assert update_data_eod._load_presets_symbols(p) == ["SPY", "QQQ", "SHY", "IWM", "TLT"]


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
