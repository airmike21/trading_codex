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
