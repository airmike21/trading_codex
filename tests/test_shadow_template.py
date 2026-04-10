from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.data import LocalStore
from trading_codex.shadow import (
    LiquidityCheckConfig,
    PositionCapConfig,
    RiskInvariantConfig,
    TurnoverCapConfig,
    build_primary_live_candidate_v1_etf_rotation_shadow_template,
    build_primary_live_candidate_v1_vol_managed_shadow_template,
    evaluate_risk_invariants,
)


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _price_series(index: pd.DatetimeIndex, returns: np.ndarray, base: float) -> pd.Series:
    return pd.Series(base * np.cumprod(1.0 + returns.astype(float)), index=index)


def _panel_from_prices(
    close_map: dict[str, pd.Series],
    *,
    volume_map: dict[str, pd.Series] | None = None,
) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for symbol, close in close_map.items():
        volume = (
            volume_map[symbol]
            if volume_map is not None and symbol in volume_map
            else pd.Series(1_000_000.0, index=close.index)
        )
        frames[symbol] = pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": volume.astype(float),
            },
            index=close.index,
        )
    return pd.concat(frames, axis=1)


def _write_symbol_bars(store: LocalStore, symbol: str, close: pd.Series) -> None:
    store.write_bars(
        symbol,
        pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000_000.0,
            },
            index=close.index,
        ),
    )


def test_evaluate_risk_invariants_blocks_position_turnover_and_liquidity_breaches() -> None:
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=40)
    close = _price_series(index, np.full(len(index), 0.0005), 100.0)
    volume = pd.Series(50.0, index=index)
    bars = _panel_from_prices({"SPY": close}, volume_map={"SPY": volume})

    weights = pd.DataFrame({"SPY": np.zeros(len(index), dtype=float)}, index=index)
    weights.iloc[-1, 0] = 1.20
    turnover = pd.Series(0.0, index=index)
    turnover.iloc[-1] = 1.20
    equity = pd.Series(np.linspace(1.0, 1.1, len(index)), index=index)

    report = evaluate_risk_invariants(
        bars=bars,
        weights=weights,
        turnover=turnover,
        equity=equity,
        config=RiskInvariantConfig(
            position_caps=PositionCapConfig(max_abs_weight=1.0),
            turnover_caps=TurnoverCapConfig(max_turnover=0.50),
            liquidity_checks=LiquidityCheckConfig(
                lookback=20,
                min_avg_dollar_volume=10_000.0,
                max_target_adv_fraction=0.05,
            ),
        ),
        symbol_hint="SPY",
    )

    assert report.blocking_reasons == (
        "position_cap_breach",
        "turnover_cap_breach",
        "liquidity_guardrail_breach",
    )
    assert report.checks["position_caps"].status == "block"
    assert report.checks["turnover_caps"].status == "block"
    assert report.checks["liquidity_checks"].status == "block"
    assert report.checks["drawdown_kill_switch"].status == "disabled"
    assert report.checks["regime_guardrails"].status == "disabled"


def test_primary_live_candidate_shadow_template_standardizes_outputs_for_clean_near_path_case() -> None:
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=260)
    spy_close = _price_series(index, np.full(len(index), 0.0010), 100.0)
    shy_close = _price_series(index, np.full(len(index), 0.0001), 100.0)
    bars = _panel_from_prices({"SPY": spy_close, "SHY": shy_close})

    weights = pd.DataFrame({"SPY": 1.0, "SHY": 0.0}, index=index, dtype=float)
    turnover = pd.Series(0.0, index=index)
    equity = pd.Series(np.linspace(1.0, 1.4, len(index)), index=index)
    latest_price = float(spy_close.iloc[-1])
    target_shares = int(10_000.0 // latest_price)

    outputs = build_primary_live_candidate_v1_vol_managed_shadow_template(
        defensive_symbols=("SHY", "CASH")
    ).build_outputs(
        bars=bars,
        weights=weights,
        turnover=turnover,
        equity=equity,
        next_action_payload={
            "date": index[-1].date().isoformat(),
            "strategy": "dual_mom_v1_shadow_impl",
            "action": "HOLD",
            "symbol": "SPY",
            "price": latest_price,
            "target_shares": target_shares,
            "resize_prev_shares": None,
            "resize_new_shares": None,
            "next_rebalance": index[-1].date().isoformat(),
            "event_id": "shadow-event-clean",
        },
        metrics_summary={"gross_cagr": 0.12, "net_cagr": 0.10, "gross_sharpe": 1.0, "net_sharpe": 0.9},
        cost_assumptions={"slippage_bps": 5.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
    )

    assert outputs.signal["shadow_strategy_id"] == "primary_live_candidate_v1_vol_managed"
    assert outputs.target_weights["current"]["SPY"] == 1.0
    assert outputs.target_weights["active_symbols"] == ["SPY"]
    assert outputs.diagnostics["risk_invariants"]["blocking_reasons"] == []
    assert outputs.diagnostics["risk_invariants"]["checks"]["regime_guardrails"]["status"] == "pass"
    assert outputs.diagnostics["risk_invariants"]["checks"]["drawdown_kill_switch"]["status"] == "pass"
    assert outputs.reports["shadow_review_bundle"]["strategy"] == "primary_live_candidate_v1_vol_managed"
    assert outputs.reports["review_summary"]["automation_decision"] == "allow"
    assert "## Risk Invariants" in outputs.reports["shadow_review_markdown"]


def test_primary_live_candidate_shadow_template_blocks_when_risk_stays_on_during_drawdown_and_risk_off() -> None:
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=260)
    spy_close = _price_series(index, np.full(len(index), -0.0015), 100.0)
    shy_close = _price_series(index, np.full(len(index), 0.0001), 100.0)
    bars = _panel_from_prices({"SPY": spy_close, "SHY": shy_close})

    weights = pd.DataFrame({"SPY": 1.0, "SHY": 0.0}, index=index, dtype=float)
    turnover = pd.Series(0.0, index=index)
    equity = pd.Series(np.linspace(1.0, 0.72, len(index)), index=index)
    latest_price = float(spy_close.iloc[-1])
    target_shares = int(10_000.0 // latest_price)

    outputs = build_primary_live_candidate_v1_vol_managed_shadow_template(
        defensive_symbols=("SHY", "CASH")
    ).build_outputs(
        bars=bars,
        weights=weights,
        turnover=turnover,
        equity=equity,
        next_action_payload={
            "date": index[-1].date().isoformat(),
            "strategy": "dual_mom_v1_shadow_impl",
            "action": "HOLD",
            "symbol": "SPY",
            "price": latest_price,
            "target_shares": target_shares,
            "resize_prev_shares": None,
            "resize_new_shares": None,
            "next_rebalance": index[-1].date().isoformat(),
            "event_id": "shadow-event-blocked",
        },
        metrics_summary={"gross_cagr": -0.05, "net_cagr": -0.07, "gross_sharpe": -0.4, "net_sharpe": -0.5},
        cost_assumptions={"slippage_bps": 5.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
    )

    blocking_reasons = outputs.diagnostics["risk_invariants"]["blocking_reasons"]
    assert "drawdown_kill_switch_breach" in blocking_reasons
    assert "regime_guardrail_breach" in blocking_reasons
    assert outputs.reports["shadow_review_bundle"]["shadow_review_state"] == "blocked"
    assert outputs.reports["review_summary"]["automation_decision"] == "block"


def test_primary_live_candidate_etf_rotation_shadow_template_standardizes_outputs_for_clean_near_path_case() -> None:
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=260)
    spy_close = _price_series(index, np.full(len(index), 0.0010), 100.0)
    bil_close = _price_series(index, np.full(len(index), 0.0001), 100.0)
    bars = _panel_from_prices({"SPY": spy_close, "BIL": bil_close})

    weights = pd.DataFrame({"SPY": 1.0, "BIL": 0.0}, index=index, dtype=float)
    turnover = pd.Series(0.0, index=index)
    equity = pd.Series(np.linspace(1.0, 1.35, len(index)), index=index)
    latest_price = float(spy_close.iloc[-1])
    target_shares = int(10_000.0 // latest_price)

    outputs = build_primary_live_candidate_v1_etf_rotation_shadow_template(
        defensive_symbols=("BIL", "CASH")
    ).build_outputs(
        bars=bars,
        weights=weights,
        turnover=turnover,
        equity=equity,
        next_action_payload={
            "date": index[-1].date().isoformat(),
            "strategy": "xsmom_v1_shadow_impl",
            "action": "HOLD",
            "symbol": "SPY",
            "price": latest_price,
            "target_shares": target_shares,
            "resize_prev_shares": None,
            "resize_new_shares": None,
            "next_rebalance": index[-1].date().isoformat(),
            "event_id": "shadow-event-etf-rotation-clean",
        },
        metrics_summary={"gross_cagr": 0.12, "net_cagr": 0.10, "gross_sharpe": 1.0, "net_sharpe": 0.9},
        cost_assumptions={"slippage_bps": 5.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
    )

    assert outputs.signal["shadow_strategy_id"] == "primary_live_candidate_v1_etf_rotation"
    assert outputs.target_weights["current"]["SPY"] == 1.0
    assert outputs.target_weights["active_symbols"] == ["SPY"]
    assert outputs.diagnostics["risk_invariants"]["blocking_reasons"] == []
    assert outputs.diagnostics["risk_invariants"]["checks"]["regime_guardrails"]["status"] == "pass"
    assert outputs.diagnostics["risk_invariants"]["checks"]["drawdown_kill_switch"]["status"] == "pass"
    assert outputs.reports["shadow_review_bundle"]["strategy"] == "primary_live_candidate_v1_etf_rotation"
    assert outputs.reports["review_summary"]["automation_decision"] == "allow"
    assert "## Risk Invariants" in outputs.reports["shadow_review_markdown"]


def test_run_backtest_shadow_artifacts_include_standardized_shadow_template_fields(
    tmp_path: Path,
) -> None:
    repo_root, env = _repo_root_and_env()
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=520)

    store = LocalStore(base_dir=tmp_path / "data")
    alt = np.arange(len(index))
    _write_symbol_bars(store, "AAA", _price_series(index, np.full(len(index), 0.0012), 100.0))
    _write_symbol_bars(store, "BBB", _price_series(index, np.where(alt % 2 == 0, 0.025, -0.02), 110.0))
    _write_symbol_bars(store, "CCC", _price_series(index, np.where(alt % 3 == 0, 0.015, -0.008), 95.0))
    _write_symbol_bars(store, "SHY", _price_series(index, np.full(len(index), 0.0002), 100.0))

    shadow_dir = tmp_path / "shadow"
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        "--strategy",
        "valmom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "CCC",
        "--vm-defensive-symbol",
        "SHY",
        "--vm-mom-lookback",
        "63",
        "--vm-val-lookback",
        "126",
        "--vm-top-n",
        "2",
        "--vm-rebalance",
        "21",
        "--start",
        index[200].date().isoformat(),
        "--end",
        index[-1].date().isoformat(),
        "--no-plot",
        "--data-dir",
        str(tmp_path / "data"),
        "--next-action-json",
        "--shadow-artifacts-dir",
        str(shadow_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    artifact_dir = shadow_dir / "plans" / index[-1].date().isoformat()
    payload = json.loads(next(artifact_dir.glob("*_shadow_review.json")).read_text(encoding="utf-8"))

    assert payload["strategy"] == "valmom_v1"
    assert payload["shadow_strategy_id"] == "valmom_v1"
    assert "risk_invariants" in payload
    assert payload["risk_invariants"]["checks"]["position_caps"]["status"] == "pass"
    assert payload["risk_invariants"]["checks"]["turnover_caps"]["status"] == "pass"
    assert payload["risk_invariants"]["checks"]["liquidity_checks"]["status"] == "pass"
    assert payload["risk_invariants"]["summary"]["block_count"] == 0
