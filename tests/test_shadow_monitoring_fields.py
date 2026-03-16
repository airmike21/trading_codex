"""Tests for shadow monitoring readiness fields v1.

Covers:
- Unit tests for each of the three warning helpers
- Unit test for ready_for_shadow_review derivation
- Integration smoke test confirming new fields appear in the written bundle
- Regression guard: existing next-action / event_id behavior unchanged
"""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_codex.backtest.shadow_artifacts import (
    _STALE_CALENDAR_DAYS,
    _compute_missing_price_warning,
    _compute_stale_data_warning,
    _compute_symbol_count_mismatch_warning,
    build_shadow_review_bundle,
)


# ---------------------------------------------------------------------------
# Unit tests: _compute_stale_data_warning
# ---------------------------------------------------------------------------


class TestStaleDataWarning:
    def _today_iso(self) -> str:
        return pd.Timestamp.now().normalize().date().isoformat()

    def test_today_is_not_stale(self) -> None:
        assert _compute_stale_data_warning(self._today_iso()) is False

    def test_yesterday_is_not_stale(self) -> None:
        yesterday = (pd.Timestamp.now().normalize() - pd.Timedelta(days=1)).date().isoformat()
        assert _compute_stale_data_warning(yesterday) is False

    def test_threshold_boundary_not_stale(self) -> None:
        # exactly _STALE_CALENDAR_DAYS old — NOT stale (strictly greater than)
        boundary = (
            pd.Timestamp.now().normalize() - pd.Timedelta(days=_STALE_CALENDAR_DAYS)
        ).date().isoformat()
        assert _compute_stale_data_warning(boundary) is False

    def test_just_over_threshold_is_stale(self) -> None:
        stale = (
            pd.Timestamp.now().normalize() - pd.Timedelta(days=_STALE_CALENDAR_DAYS + 1)
        ).date().isoformat()
        assert _compute_stale_data_warning(stale) is True

    def test_very_old_date_is_stale(self) -> None:
        assert _compute_stale_data_warning("2020-01-01") is True


# ---------------------------------------------------------------------------
# Unit tests: _compute_missing_price_warning
# ---------------------------------------------------------------------------


class TestMissingPriceWarning:
    def test_no_actions_no_warning(self) -> None:
        assert _compute_missing_price_warning([]) is False

    def test_cash_action_no_warning(self) -> None:
        assert _compute_missing_price_warning([{"action": "HOLD", "symbol": "CASH", "price": None}]) is False

    def test_non_cash_with_valid_price_no_warning(self) -> None:
        assert _compute_missing_price_warning([{"action": "BUY", "symbol": "SPY", "price": 450.0}]) is False

    def test_non_cash_with_none_price_raises_warning(self) -> None:
        # BUY action missing price
        assert _compute_missing_price_warning([{"action": "BUY", "symbol": "SPY", "price": None}]) is True

    def test_non_cash_with_nan_price_raises_warning(self) -> None:
        assert _compute_missing_price_warning([{"action": "HOLD", "symbol": "SPY", "price": float("nan")}]) is True

    def test_hold_with_none_price_no_warning(self) -> None:
        # HOLD on a non-CASH symbol with None price: acceptable per spec
        assert _compute_missing_price_warning([{"action": "HOLD", "symbol": "SPY", "price": None}]) is False

    def test_multiple_actions_one_missing_price_warns(self) -> None:
        actions = [
            {"action": "BUY", "symbol": "SPY", "price": 450.0},
            {"action": "BUY", "symbol": "QQQ", "price": None},
        ]
        assert _compute_missing_price_warning(actions) is True

    def test_multiple_valid_actions_no_warning(self) -> None:
        actions = [
            {"action": "BUY", "symbol": "SPY", "price": 450.0},
            {"action": "HOLD", "symbol": "QQQ", "price": 370.0},
        ]
        assert _compute_missing_price_warning(actions) is False


# ---------------------------------------------------------------------------
# Unit tests: _compute_symbol_count_mismatch_warning
# ---------------------------------------------------------------------------


class TestSymbolCountMismatchWarning:
    def test_none_expected_no_warning(self) -> None:
        assert _compute_symbol_count_mismatch_warning(None, 3) is False

    def test_none_actual_no_warning(self) -> None:
        assert _compute_symbol_count_mismatch_warning(3, None) is False

    def test_both_none_no_warning(self) -> None:
        assert _compute_symbol_count_mismatch_warning(None, None) is False

    def test_matching_counts_no_warning(self) -> None:
        assert _compute_symbol_count_mismatch_warning(4, 4) is False

    def test_mismatch_raises_warning(self) -> None:
        assert _compute_symbol_count_mismatch_warning(4, 3) is True

    def test_zero_actual_raises_warning(self) -> None:
        assert _compute_symbol_count_mismatch_warning(3, 0) is True


# ---------------------------------------------------------------------------
# Unit tests: ready_for_shadow_review derivation
# ---------------------------------------------------------------------------


class TestReadyForShadowReview:
    """ready_for_shadow_review must be True iff all three warnings are False."""

    def _bundle_from_flags(
        self,
        *,
        as_of_date: str,
        actions: list[dict],
        expected_symbol_count: int | None = None,
        actual_symbol_count: int | None = None,
    ) -> dict:
        return build_shadow_review_bundle(
            strategy="test",
            as_of_date=as_of_date,
            next_rebalance=None,
            actions=actions,
            cost_assumptions={"slippage_bps": 0.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
            metrics={},
            expected_symbol_count=expected_symbol_count,
            actual_symbol_count=actual_symbol_count,
        )

    def test_all_clear_ready(self) -> None:
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle_from_flags(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        assert bundle["stale_data_warning"] is False
        assert bundle["missing_price_warning"] is False
        assert bundle["symbol_count_mismatch_warning"] is False
        assert bundle["ready_for_shadow_review"] is True

    def test_stale_data_blocks_ready(self) -> None:
        bundle = self._bundle_from_flags(
            as_of_date="2020-01-01",
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        assert bundle["stale_data_warning"] is True
        assert bundle["ready_for_shadow_review"] is False

    def test_missing_price_blocks_ready(self) -> None:
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle_from_flags(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": None}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        assert bundle["missing_price_warning"] is True
        assert bundle["ready_for_shadow_review"] is False

    def test_symbol_mismatch_blocks_ready(self) -> None:
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle_from_flags(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=4,
            actual_symbol_count=3,
        )
        assert bundle["symbol_count_mismatch_warning"] is True
        assert bundle["ready_for_shadow_review"] is False

    def test_all_warnings_blocks_ready(self) -> None:
        bundle = self._bundle_from_flags(
            as_of_date="2020-01-01",
            actions=[{"action": "BUY", "symbol": "SPY", "price": None}],
            expected_symbol_count=4,
            actual_symbol_count=3,
        )
        assert bundle["stale_data_warning"] is True
        assert bundle["missing_price_warning"] is True
        assert bundle["symbol_count_mismatch_warning"] is True
        assert bundle["ready_for_shadow_review"] is False


# ---------------------------------------------------------------------------
# Integration smoke test: new fields present in written bundle artifact
# ---------------------------------------------------------------------------


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _bars_for_index(idx: pd.DatetimeIndex, close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {"open": close, "high": close, "low": close, "close": close, "volume": 1_000},
        index=idx,
    )


def _write_synth_store(base_dir: Path) -> None:
    from trading_codex.data import LocalStore

    idx = pd.date_range("2019-01-01", periods=520, freq="B")
    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_shy = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("SHY", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_shy), index=idx)))


def _rb_args(data_dir: Path) -> list[str]:
    return [
        "--strategy", "valmom_v1",
        "--symbols", "AAA", "BBB", "CCC",
        "--vm-defensive-symbol", "SHY",
        "--vm-mom-lookback", "63",
        "--vm-val-lookback", "126",
        "--vm-top-n", "2",
        "--vm-rebalance", "21",
        "--start", "2020-01-02",
        "--end", "2020-12-01",
        "--no-plot",
        "--data-dir", str(data_dir),
    ]


def test_shadow_bundle_contains_monitoring_fields(tmp_path: Path) -> None:
    """Integration smoke: written shadow bundle JSON has all four monitoring fields."""
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    shadow_dir = tmp_path / "shadow"
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--next-action-json",
        "--shadow-artifacts-dir", str(shadow_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    json_artifacts = list((shadow_dir / "plans" / "2020-12-01").glob("*_shadow_review.json"))
    assert len(json_artifacts) == 1, "Expected exactly one shadow review JSON artifact"

    payload = json.loads(json_artifacts[0].read_text(encoding="utf-8"))

    # New monitoring fields must be present
    assert "stale_data_warning" in payload, "stale_data_warning missing from bundle"
    assert "missing_price_warning" in payload, "missing_price_warning missing from bundle"
    assert "symbol_count_mismatch_warning" in payload, "symbol_count_mismatch_warning missing from bundle"
    assert "ready_for_shadow_review" in payload, "ready_for_shadow_review missing from bundle"

    # All must be bool
    assert isinstance(payload["stale_data_warning"], bool)
    assert isinstance(payload["missing_price_warning"], bool)
    assert isinstance(payload["symbol_count_mismatch_warning"], bool)
    assert isinstance(payload["ready_for_shadow_review"], bool)

    # Invariant: ready_for_shadow_review == not (any warning)
    any_warning = (
        payload["stale_data_warning"]
        or payload["missing_price_warning"]
        or payload["symbol_count_mismatch_warning"]
    )
    assert payload["ready_for_shadow_review"] == (not any_warning)

    # Historical backtest with clean synth data: no symbol mismatch, no missing price
    assert payload["missing_price_warning"] is False
    assert payload["symbol_count_mismatch_warning"] is False

    # The as_of_date for this backtest is 2020-12-01 which is years in the past — must be stale
    assert payload["stale_data_warning"] is True

    # Therefore not ready
    assert payload["ready_for_shadow_review"] is False


# ---------------------------------------------------------------------------
# Edge-path tests: missing as_of_date in actions_bars index
# ---------------------------------------------------------------------------


def _make_multiindex_bars(dates: list[str], symbols: list[str]) -> pd.DataFrame:
    """Return a MultiIndex-columns DataFrame (level-0=symbol, level-1=field).

    Columns are ordered (symbol, field) as produced by the multi-asset backtest path.
    Close values are filled with a constant 100.0.
    """
    idx = pd.DatetimeIndex(dates)
    arrays = (
        [s for s in symbols for _ in ("open", "high", "low", "close", "volume")],
        ["open", "high", "low", "close", "volume"] * len(symbols),
    )
    cols = pd.MultiIndex.from_arrays(arrays)
    data = {col: 100.0 for col in zip(*arrays)}
    df = pd.DataFrame(data, index=idx)
    df.columns = cols
    return df


def _symbol_counts_from_bars(
    bars: pd.DataFrame, as_of: str
) -> tuple[int | None, int | None]:
    """Mirror the exact derivation in maybe_write_shadow_artifacts."""
    expected: int | None = None
    actual: int | None = None
    if isinstance(bars.columns, pd.MultiIndex):
        all_symbols = bars.columns.get_level_values(0).unique().tolist()
        expected = len(all_symbols)
        try:
            last_row = bars.xs("close", axis=1, level=1).loc[as_of]
            actual = int(last_row.notna().sum())
        except KeyError:
            actual = 0
    return expected, actual


class TestSymbolCountMissingDate:
    """actions_bars has MultiIndex columns but as_of_date is not in the index."""

    _DATES = ["2020-11-27", "2020-11-30", "2020-12-01"]
    _SYMBOLS = ["SPY", "QQQ"]
    _MISSING_DATE = "2020-12-02"  # not in _DATES

    def _bars(self) -> pd.DataFrame:
        return _make_multiindex_bars(self._DATES, self._SYMBOLS)

    def test_missing_date_actual_count_is_zero(self) -> None:
        """When as_of_date is absent from the index, actual_symbol_count resolves to 0."""
        bars = self._bars()
        _, actual = _symbol_counts_from_bars(bars, self._MISSING_DATE)
        assert actual == 0

    def test_missing_date_with_expected_symbols_triggers_mismatch(self) -> None:
        """expected=2, actual=0 → mismatch=True, ready_for_shadow_review=False."""
        bars = self._bars()
        expected, actual = _symbol_counts_from_bars(bars, self._MISSING_DATE)
        assert expected == 2
        assert actual == 0
        assert _compute_symbol_count_mismatch_warning(expected, actual) is True

        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = build_shadow_review_bundle(
            strategy="test",
            as_of_date=today,
            next_rebalance=None,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            cost_assumptions={"slippage_bps": 0.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
            metrics={},
            expected_symbol_count=expected,
            actual_symbol_count=actual,
        )
        assert bundle["symbol_count_mismatch_warning"] is True
        assert bundle["ready_for_shadow_review"] is False

    def test_missing_date_zero_expected_no_mismatch(self) -> None:
        """Edge: expected=0, actual=0 → mismatch=False (counts agree)."""
        assert _compute_symbol_count_mismatch_warning(0, 0) is False

        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = build_shadow_review_bundle(
            strategy="test",
            as_of_date=today,
            next_rebalance=None,
            actions=[{"action": "HOLD", "symbol": "CASH", "price": None}],
            cost_assumptions={"slippage_bps": 0.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
            metrics={},
            expected_symbol_count=0,
            actual_symbol_count=0,
        )
        assert bundle["symbol_count_mismatch_warning"] is False


# ---------------------------------------------------------------------------
# Edge-path tests: non-MultiIndex actions_bars columns
# ---------------------------------------------------------------------------


class TestSymbolCountNonMultiIndex:
    """actions_bars has a flat (non-MultiIndex) columns Index."""

    def _flat_bars(self, symbols: list[str] | None = None) -> pd.DataFrame:
        """Return a flat-column OHLCV-style DataFrame (e.g. tsmom single-symbol)."""
        idx = pd.DatetimeIndex(["2020-11-27", "2020-11-30", "2020-12-01"])
        if symbols is None:
            # Single-symbol flat frame as produced by tsmom path
            return pd.DataFrame(
                {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000},
                index=idx,
            )
        # Flat multi-column frame (columns are symbol names, not a MultiIndex)
        data = {s: [100.0, 101.0, 102.0] for s in symbols}
        return pd.DataFrame(data, index=idx)

    def test_flat_columns_skips_derivation(self) -> None:
        """Non-MultiIndex columns → both counts stay None → mismatch=False."""
        bars = self._flat_bars()
        assert not isinstance(bars.columns, pd.MultiIndex)
        expected, actual = _symbol_counts_from_bars(bars, "2020-12-01")
        assert expected is None
        assert actual is None
        assert _compute_symbol_count_mismatch_warning(expected, actual) is False

    def test_flat_columns_no_exception(self) -> None:
        """_symbol_counts_from_bars must not raise on a flat-column DataFrame."""
        bars = self._flat_bars(symbols=["SPY", "QQQ"])
        assert not isinstance(bars.columns, pd.MultiIndex)
        # Should complete without raising KeyError or AttributeError
        expected, actual = _symbol_counts_from_bars(bars, "2020-12-01")
        assert expected is None
        assert actual is None

    def test_single_column_df_no_mismatch(self) -> None:
        """Single-column tsmom frame → no mismatch warning in the bundle."""
        bars = self._flat_bars()
        expected, actual = _symbol_counts_from_bars(bars, "2020-12-01")
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = build_shadow_review_bundle(
            strategy="test",
            as_of_date=today,
            next_rebalance=None,
            actions=[{"action": "HOLD", "symbol": "SPY", "price": 450.0}],
            cost_assumptions={"slippage_bps": 0.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
            metrics={},
            expected_symbol_count=expected,
            actual_symbol_count=actual,
        )
        assert bundle["symbol_count_mismatch_warning"] is False


# ---------------------------------------------------------------------------
# Unit tests: warning_reasons and blocking_reasons lists
# ---------------------------------------------------------------------------


class TestReasonLists:
    """Verify the new additive reason-list fields on the shadow review bundle."""

    def _bundle(
        self,
        *,
        as_of_date: str | None = None,
        actions: list[dict] | None = None,
        expected_symbol_count: int | None = None,
        actual_symbol_count: int | None = None,
    ) -> dict:
        today = pd.Timestamp.now().normalize().date().isoformat()
        return build_shadow_review_bundle(
            strategy="test",
            as_of_date=as_of_date or today,
            next_rebalance=None,
            actions=actions or [{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            cost_assumptions={"slippage_bps": 0.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
            metrics={},
            expected_symbol_count=expected_symbol_count,
            actual_symbol_count=actual_symbol_count,
        )

    def test_no_warnings_both_lists_empty(self) -> None:
        """All warnings False → both lists are empty."""
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        assert bundle["stale_data_warning"] is False
        assert bundle["missing_price_warning"] is False
        assert bundle["symbol_count_mismatch_warning"] is False
        assert bundle["warning_reasons"] == []
        assert bundle["blocking_reasons"] == []

    def test_stale_only_warning_reasons(self) -> None:
        """stale=True, others False → warning_reasons=['stale_data'], blocking_reasons=[]."""
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle(
            as_of_date="2020-01-01",
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        assert bundle["stale_data_warning"] is True
        assert bundle["missing_price_warning"] is False
        assert bundle["symbol_count_mismatch_warning"] is False
        assert bundle["warning_reasons"] == ["stale_data"]
        assert bundle["blocking_reasons"] == []

    def test_missing_price_only_blocking_reasons(self) -> None:
        """missing_price=True, others False → warning_reasons=[], blocking_reasons=['missing_price']."""
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": None}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        assert bundle["stale_data_warning"] is False
        assert bundle["missing_price_warning"] is True
        assert bundle["symbol_count_mismatch_warning"] is False
        assert bundle["warning_reasons"] == []
        assert bundle["blocking_reasons"] == ["missing_price"]

    def test_count_mismatch_only_blocking_reasons(self) -> None:
        """count_mismatch=True, others False → warning_reasons=[], blocking_reasons=['symbol_count_mismatch']."""
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=4,
            actual_symbol_count=3,
        )
        assert bundle["stale_data_warning"] is False
        assert bundle["missing_price_warning"] is False
        assert bundle["symbol_count_mismatch_warning"] is True
        assert bundle["warning_reasons"] == []
        assert bundle["blocking_reasons"] == ["symbol_count_mismatch"]

    def test_combined_deterministic_ordering(self) -> None:
        """All three True → both lists populated with correct codes in correct order."""
        bundle = self._bundle(
            as_of_date="2020-01-01",
            actions=[{"action": "BUY", "symbol": "SPY", "price": None}],
            expected_symbol_count=4,
            actual_symbol_count=3,
        )
        assert bundle["stale_data_warning"] is True
        assert bundle["missing_price_warning"] is True
        assert bundle["symbol_count_mismatch_warning"] is True
        assert bundle["warning_reasons"] == ["stale_data"]
        assert bundle["blocking_reasons"] == ["missing_price", "symbol_count_mismatch"]

    def test_integration_bundle_json_includes_reason_lists(self) -> None:
        """Smoke: build_shadow_review_bundle returns both new fields in the dict."""
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = build_shadow_review_bundle(
            strategy="integration_test",
            as_of_date=today,
            next_rebalance=None,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            cost_assumptions={"slippage_bps": 5.0, "commission_per_trade": 1.0, "commission_bps": 0.0},
            metrics={"gross_cagr": 0.12, "net_cagr": 0.10},
        )
        assert "warning_reasons" in bundle, "warning_reasons missing from bundle"
        assert "blocking_reasons" in bundle, "blocking_reasons missing from bundle"
        assert isinstance(bundle["warning_reasons"], list)
        assert isinstance(bundle["blocking_reasons"], list)

    def test_existing_booleans_unchanged(self) -> None:
        """Regression: existing boolean fields and ready_for_shadow_review still present and correct."""
        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = self._bundle(
            as_of_date=today,
            actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0}],
            expected_symbol_count=1,
            actual_symbol_count=1,
        )
        # All four original fields must be present
        assert "stale_data_warning" in bundle
        assert "missing_price_warning" in bundle
        assert "symbol_count_mismatch_warning" in bundle
        assert "ready_for_shadow_review" in bundle
        # All must be bool
        assert isinstance(bundle["stale_data_warning"], bool)
        assert isinstance(bundle["missing_price_warning"], bool)
        assert isinstance(bundle["symbol_count_mismatch_warning"], bool)
        assert isinstance(bundle["ready_for_shadow_review"], bool)
        # Invariant: ready == not any_warning
        any_warning = (
            bundle["stale_data_warning"]
            or bundle["missing_price_warning"]
            or bundle["symbol_count_mismatch_warning"]
        )
        assert bundle["ready_for_shadow_review"] == (not any_warning)


def test_shadow_bundle_monitoring_fields_do_not_alter_next_action_stdout(tmp_path: Path) -> None:
    """Regression: adding monitoring fields must not change next-action JSON output."""
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    base_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--next-action-json",
    ]

    # Run without shadow artifacts flag
    baseline = subprocess.run(base_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert baseline.returncode == 0
    assert len(baseline.stdout.splitlines()) == 1

    # Run with shadow artifacts flag — stdout must be identical
    shadow_dir = tmp_path / "shadow"
    with_shadow = subprocess.run(
        [*base_cmd, "--shadow-artifacts-dir", str(shadow_dir)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )
    assert with_shadow.returncode == 0
    assert with_shadow.stdout == baseline.stdout, (
        f"next-action stdout changed after adding shadow artifacts!\n"
        f"baseline: {baseline.stdout!r}\n"
        f"with shadow: {with_shadow.stdout!r}"
    )
    assert with_shadow.stderr == ""

    # Verify event_id is unchanged
    baseline_payload = json.loads(baseline.stdout)
    with_shadow_payload = json.loads(with_shadow.stdout)
    assert baseline_payload["event_id"] == with_shadow_payload["event_id"]
