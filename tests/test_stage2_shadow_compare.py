from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_codex.backtest import metrics
from trading_codex.backtest.engine import BacktestResult
from trading_codex.data import LocalStore
from trading_codex.shadow.stage2_compare import (
    Stage2CompareCandidate,
    build_drawdown_cluster_review,
    build_stage2_shadow_compare_report,
    build_walk_forward_summary,
    derive_shadow_candidate_decision,
    summarize_benchmark_comparison,
    summarize_cost_sensitivity,
    summarize_parameter_stability,
)


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _price_series(index: pd.DatetimeIndex, returns: np.ndarray, base: float) -> pd.Series:
    return pd.Series(base * np.cumprod(1.0 + returns.astype(float)), index=index)


def _write_symbol_bars(
    store: LocalStore,
    symbol: str,
    close: pd.Series,
    *,
    volume: float = 1_000_000.0,
) -> None:
    store.write_bars(
        symbol,
        pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": float(volume),
            },
            index=close.index,
        ),
    )


def _candidate_from_returns(
    *,
    returns: pd.Series,
    benchmark_returns: pd.Series | None = None,
    strategy_id: str = "candidate",
    role: str = "shadow",
    weights: pd.DataFrame | None = None,
    review_bundle: dict[str, object] | None = None,
    parameter_stability_rows: list[dict[str, object]] | None = None,
    cost_sensitivity_rows: list[dict[str, object]] | None = None,
    turnover: pd.Series | None = None,
    estimated_costs: pd.Series | None = None,
) -> Stage2CompareCandidate:
    weight_frame = (
        weights.copy().astype(float)
        if weights is not None
        else pd.DataFrame({"SPY": 1.0}, index=returns.index, dtype=float)
    )
    turnover_series = (
        turnover.copy().astype(float)
        if turnover is not None
        else pd.Series(0.0, index=returns.index, dtype=float)
    )
    equity = (1.0 + returns).cumprod()
    result = BacktestResult(
        returns=returns,
        weights=weight_frame,
        turnover=turnover_series,
        equity=equity,
        estimated_costs=estimated_costs,
    )
    return Stage2CompareCandidate(
        strategy_id=strategy_id,
        role=role,
        runtime_strategy="test_strategy",
        implementation_label="test_impl",
        parameters={"risk_symbols": ["SPY"]},
        result=result,
        actions=pd.DataFrame(
            [
                {
                    "date": returns.index[0].date().isoformat(),
                    "action": "ENTER",
                    "from_symbol": "CASH",
                    "to_symbol": "SPY",
                    "price_assumed(close)": 100.0,
                    "weight_from": 0.0,
                    "weight_to": 1.0,
                }
            ]
        ),
        next_action={
            "date": returns.index[-1].date().isoformat(),
            "strategy": strategy_id,
            "action": "HOLD",
            "symbol": "SPY",
            "target_shares": 100,
            "next_rebalance": returns.index[-1].date().isoformat(),
        },
        metrics={
            "cagr": float(metrics.cagr(returns)),
            "sharpe": float(metrics.sharpe(returns)),
            "max_drawdown": float(metrics.max_drawdown(returns)),
            "annual_turnover": float(turnover_series.sum() / (len(returns) / 252.0)) if len(returns) else 0.0,
        },
        benchmark_returns=benchmark_returns,
        review_bundle=review_bundle or _review_bundle("clean"),
        parameter_stability_rows=parameter_stability_rows or [],
        cost_sensitivity_rows=cost_sensitivity_rows or [],
        artifacts={},
    )


def _review_bundle(
    review_state: str,
    *,
    automation_decision: str | None = None,
) -> dict[str, object]:
    derived_decision = automation_decision
    if derived_decision is None:
        derived_decision = {
            "clean": "allow",
            "warning": "review",
            "blocked": "block",
        }.get(review_state, "review")
    automation_status = {
        "allow": "automation_ready",
        "review": "review_required",
        "block": "blocked",
    }[derived_decision]
    return {
        "shadow_review_state": review_state,
        "review_summary": {
            "shadow_review_state": review_state,
            "automation_decision": derived_decision,
            "automation_status": automation_status,
            "warning_reasons": [] if review_state != "warning" else ["stale_data"],
            "blocking_reasons": [] if review_state != "blocked" else ["missing_price"],
        },
    }


def _shadow_decision(
    *,
    review_state: str = "clean",
    automation_decision: str | None = None,
    shadow_sharpe: float = 1.10,
    shadow_cagr: float = 0.12,
    shadow_max_drawdown: float = -0.16,
    walk_forward_label: str = "strong",
    parameter_label: str = "stable",
    drawdown_label: str = "contained",
    benchmark_label: str = "strong",
    cost_label: str = "resilient",
) -> str:
    return derive_shadow_candidate_decision(
        primary_row={"sharpe": 1.0, "cagr": 0.10, "max_drawdown": -0.15},
        shadow_row={
            "sharpe": shadow_sharpe,
            "cagr": shadow_cagr,
            "max_drawdown": shadow_max_drawdown,
        },
        shadow_review_bundle=_review_bundle(
            review_state,
            automation_decision=automation_decision,
        ),
        shadow_walk_forward_summary={"label": walk_forward_label},
        shadow_parameter_summary={"label": parameter_label},
        shadow_drawdown_summary={"label": drawdown_label},
        shadow_benchmark_summary={"label": benchmark_label},
        shadow_cost_sensitivity_summary={"label": cost_label},
    )


def test_summarize_parameter_stability_marks_consistent_neighborhood_stable() -> None:
    rows = [
        {
            "strategy_id": "shadow",
            "variant_label": "baseline",
            "parameter_name": "baseline",
            "momentum_lookback": 63,
            "rebalance": 21,
            "target_vol": 0.10,
            "vol_lookback": 20,
            "cagr": 0.12,
            "sharpe": 1.10,
            "max_drawdown": -0.12,
            "turnover": 1.8,
            "action_frequency": 10.0,
            "is_baseline": True,
        },
        {
            "strategy_id": "shadow",
            "variant_label": "momentum_lookback_m42_r21_tv0.10_vl20",
            "parameter_name": "momentum_lookback",
            "momentum_lookback": 42,
            "rebalance": 21,
            "target_vol": 0.10,
            "vol_lookback": 20,
            "cagr": 0.10,
            "sharpe": 0.95,
            "max_drawdown": -0.14,
            "turnover": 1.9,
            "action_frequency": 10.5,
            "is_baseline": False,
        },
        {
            "strategy_id": "shadow",
            "variant_label": "rebalance_m63_r28_tv0.10_vl20",
            "parameter_name": "rebalance",
            "momentum_lookback": 63,
            "rebalance": 28,
            "target_vol": 0.10,
            "vol_lookback": 20,
            "cagr": 0.11,
            "sharpe": 0.98,
            "max_drawdown": -0.13,
            "turnover": 1.6,
            "action_frequency": 8.0,
            "is_baseline": False,
        },
    ]

    summary = summarize_parameter_stability(rows)

    assert summary["label"] == "stable"
    assert summary["variant_count"] == 3
    assert summary["positive_cagr_fraction"] == 1.0
    assert summary["positive_sharpe_fraction"] == 1.0
    assert summary["baseline_variant"] == "baseline"
    assert "positive-Sharpe variants" in summary["summary_text"]


def test_summarize_benchmark_comparison_distinguishes_strong_mixed_and_weak() -> None:
    strong = summarize_benchmark_comparison(
        [
            {"period": "full", "excess_cagr": 0.01, "sharpe_delta": 0.02, "outperformed_benchmark": True},
            {"period": "first_half", "excess_cagr": 0.02, "sharpe_delta": 0.04, "outperformed_benchmark": True},
            {"period": "second_half", "excess_cagr": -0.01, "sharpe_delta": -0.02, "outperformed_benchmark": True},
            {"period": "recent_1y", "excess_cagr": 0.00, "sharpe_delta": -0.08, "outperformed_benchmark": False},
        ]
    )
    mixed = summarize_benchmark_comparison(
        [
            {"period": "full", "excess_cagr": -0.02, "sharpe_delta": -0.12, "outperformed_benchmark": True},
            {"period": "first_half", "excess_cagr": 0.01, "sharpe_delta": 0.01, "outperformed_benchmark": True},
            {"period": "second_half", "excess_cagr": -0.03, "sharpe_delta": -0.15, "outperformed_benchmark": False},
            {"period": "recent_1y", "excess_cagr": -0.01, "sharpe_delta": -0.05, "outperformed_benchmark": False},
        ]
    )
    weak = summarize_benchmark_comparison(
        [
            {"period": "full", "excess_cagr": -0.04, "sharpe_delta": -0.20, "outperformed_benchmark": False},
            {"period": "first_half", "excess_cagr": -0.02, "sharpe_delta": -0.10, "outperformed_benchmark": True},
            {"period": "second_half", "excess_cagr": -0.05, "sharpe_delta": -0.12, "outperformed_benchmark": False},
            {"period": "recent_1y", "excess_cagr": -0.01, "sharpe_delta": -0.04, "outperformed_benchmark": False},
        ]
    )

    assert strong["label"] == "strong"
    assert strong["benchmark_win_fraction"] == pytest.approx(0.75)
    assert mixed["label"] == "mixed"
    assert mixed["benchmark_win_fraction"] == pytest.approx(0.50)
    assert weak["label"] == "weak"
    assert weak["benchmark_win_fraction"] == pytest.approx(0.25)


def test_summarize_cost_sensitivity_distinguishes_resilient_mixed_and_fragile() -> None:
    resilient = summarize_cost_sensitivity(
        [
            {"scenario": "base", "cagr": 0.10, "sharpe": 1.00},
            {"scenario": "stress_mid", "cagr": 0.09, "sharpe": 0.88},
            {"scenario": "stress_high", "cagr": 0.07, "sharpe": 0.70},
        ]
    )
    mixed = summarize_cost_sensitivity(
        [
            {"scenario": "base", "cagr": 0.06, "sharpe": 0.30},
            {"scenario": "stress_mid", "cagr": 0.03, "sharpe": 0.05},
            {"scenario": "stress_high", "cagr": -0.02, "sharpe": -0.20},
        ]
    )
    fragile = summarize_cost_sensitivity(
        [
            {"scenario": "base", "cagr": 0.08, "sharpe": 0.90},
            {"scenario": "stress_mid", "cagr": 0.00, "sharpe": 0.10},
            {"scenario": "stress_high", "cagr": -0.05, "sharpe": -0.35},
        ]
    )

    assert resilient["label"] == "resilient"
    assert mixed["label"] == "mixed"
    assert fragile["label"] == "fragile"


def test_stage2_shadow_compare_report_uses_average_cash_allocation_for_partial_exposure() -> None:
    index = pd.bdate_range("2020-01-01", periods=4)
    primary_returns = pd.Series(0.0010, index=index)
    shadow_returns = pd.Series(0.0005, index=index)

    primary_candidate = _candidate_from_returns(
        returns=primary_returns,
        strategy_id="primary_live_candidate_v1",
        role="primary",
    )
    shadow_candidate = _candidate_from_returns(
        returns=shadow_returns,
        strategy_id="primary_live_candidate_v1_vol_managed",
        weights=pd.DataFrame({"SPY": 0.5}, index=index, dtype=float),
    )

    report = build_stage2_shadow_compare_report(
        pair_id="primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed",
        as_of_date=index[-1].date().isoformat(),
        generated_at="2026-04-08T12:00:00",
        command="pytest",
        data_dir="/tmp/data",
        primary=primary_candidate,
        shadow=shadow_candidate,
        primary_mapping={"strategy_id": "primary_live_candidate_v1"},
        shadow_runtime={"strategy_id": "primary_live_candidate_v1_vol_managed"},
    )

    rows = {
        row["strategy_id"]: row
        for row in report["scoreboard"]["rows"]
    }
    assert rows["primary_live_candidate_v1"]["percent_time_in_cash"] == pytest.approx(0.0)
    assert rows["primary_live_candidate_v1_vol_managed"]["percent_time_in_cash"] == pytest.approx(50.0)


def test_derive_shadow_candidate_decision_returns_not_advancing_when_review_is_blocked() -> None:
    assert _shadow_decision(review_state="blocked") == "not advancing"


def test_derive_shadow_candidate_decision_keeps_warning_state_shadow_only() -> None:
    assert _shadow_decision(review_state="warning") == "remain shadow-only"


@pytest.mark.parametrize(
    ("parameter_label", "walk_forward_label"),
    [
        ("fragile", "strong"),
        ("stable", "weak"),
    ],
)
def test_derive_shadow_candidate_decision_rejects_fragile_parameters_and_weak_walk_forward(
    parameter_label: str,
    walk_forward_label: str,
) -> None:
    assert (
        _shadow_decision(
            parameter_label=parameter_label,
            walk_forward_label=walk_forward_label,
        )
        == "not advancing"
    )


@pytest.mark.parametrize(
    ("shadow_sharpe", "shadow_cagr", "shadow_max_drawdown"),
    [
        (0.80, 0.12, -0.16),
        (1.10, 0.07, -0.16),
        (1.10, 0.12, -0.20),
    ],
)
def test_derive_shadow_candidate_decision_rejects_large_relative_regressions(
    shadow_sharpe: float,
    shadow_cagr: float,
    shadow_max_drawdown: float,
) -> None:
    assert (
        _shadow_decision(
            shadow_sharpe=shadow_sharpe,
            shadow_cagr=shadow_cagr,
            shadow_max_drawdown=shadow_max_drawdown,
        )
        == "not advancing"
    )


def test_derive_shadow_candidate_decision_requires_clean_strong_surfaces_for_candidate_branch() -> None:
    assert _shadow_decision() == "candidate for later paper promotion after Stage 2 exit"


@pytest.mark.parametrize(
    ("benchmark_label", "cost_label", "drawdown_label"),
    [
        ("mixed", "resilient", "contained"),
        ("strong", "mixed", "contained"),
        ("strong", "resilient", "clustered"),
    ],
)
def test_derive_shadow_candidate_decision_downgrades_to_shadow_only_when_supporting_surfaces_are_not_strong(
    benchmark_label: str,
    cost_label: str,
    drawdown_label: str,
) -> None:
    assert (
        _shadow_decision(
            benchmark_label=benchmark_label,
            cost_label=cost_label,
            drawdown_label=drawdown_label,
        )
        == "remain shadow-only"
    )


def test_walk_forward_and_drawdown_cluster_helpers_return_expected_shapes() -> None:
    index = pd.bdate_range("2020-01-01", periods=260)
    alternating = np.arange(len(index))
    positive_returns = pd.Series(
        np.where(alternating % 2 == 0, 0.0012, 0.0008),
        index=index,
    )
    benchmark_returns = pd.Series(
        np.where(alternating % 2 == 0, 0.0005, 0.0003),
        index=index,
    )
    positive_candidate = _candidate_from_returns(
        returns=positive_returns,
        benchmark_returns=benchmark_returns,
        strategy_id="positive_candidate",
    )

    walk_forward = build_walk_forward_summary(positive_candidate)

    assert walk_forward["summary"]["window_count"] >= 2
    assert walk_forward["summary"]["label"] == "strong"
    assert walk_forward["summary"]["benchmark_outperform_fraction"] == 1.0
    assert all(row["outperformed_benchmark"] is True for row in walk_forward["rows"])

    clustered_returns = pd.Series(0.0010, index=index)
    clustered_returns.iloc[40:45] = -0.0200
    clustered_returns.iloc[140:145] = -0.0250
    cluster_candidate = _candidate_from_returns(
        returns=clustered_returns,
        benchmark_returns=benchmark_returns,
        strategy_id="cluster_candidate",
    )

    drawdown_review = build_drawdown_cluster_review(cluster_candidate, threshold=-0.05, merge_gap=5)

    assert drawdown_review["summary"]["cluster_count"] == 2
    assert drawdown_review["summary"]["label"] == "clustered"
    assert len(drawdown_review["rows"]) == 2
    assert min(row["worst_drawdown"] for row in drawdown_review["rows"]) <= -0.05


def test_stage2_shadow_compare_cli_writes_expected_artifacts(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=520)
    alt = np.arange(len(index))

    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    store = LocalStore(base_dir=data_dir)
    close_map = {
        "SPY": _price_series(index, np.full(len(index), 0.0007), 100.0),
        "QQQ": _price_series(index, np.where(alt % 2 == 0, 0.0016, -0.0008), 105.0),
        "IWM": _price_series(index, np.full(len(index), -0.0001), 95.0),
        "EFA": _price_series(index, np.where(alt % 3 == 0, 0.0190, -0.0075), 98.0),
        "BIL": _price_series(index, np.full(len(index), 0.0001), 100.0),
    }
    for symbol, close in close_map.items():
        _write_symbol_bars(store, symbol, close)

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "stage2_shadow_compare.py"),
            "--data-dir",
            str(data_dir),
            "--artifacts-dir",
            str(artifacts_dir),
            "--start",
            index[0].date().isoformat(),
            "--end",
            index[-1].date().isoformat(),
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    summary = json.loads(lines[0])

    comparison_report_json = Path(summary["report_json"])
    comparison_report_markdown = Path(summary["report_markdown"])
    scoreboard_csv = Path(summary["scoreboard_csv"])
    primary_output_json = Path(summary["primary_output_json"])
    shadow_output_json = Path(summary["shadow_output_json"])

    assert comparison_report_json.exists()
    assert comparison_report_markdown.exists()
    assert scoreboard_csv.exists()
    assert primary_output_json.exists()
    assert shadow_output_json.exists()
    assert summary["current_decision"] in {
        "not advancing",
        "remain shadow-only",
        "candidate for later paper promotion after Stage 2 exit",
    }

    report = json.loads(comparison_report_json.read_text(encoding="utf-8"))
    assert report["artifact_type"] == "stage2_shadow_compare"
    assert report["pair_id"] == "primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed"
    assert report["current_decision"] == summary["current_decision"]
    assert report["scoreboard"]["columns"]
    assert len(report["scoreboard"]["rows"]) == 2
    assert {row["strategy_id"] for row in report["scoreboard"]["rows"]} == {
        "primary_live_candidate_v1",
        "primary_live_candidate_v1_vol_managed",
    }
    assert report["robustness_harness"]["parameter_stability"]["rows"]
    assert report["robustness_harness"]["subperiod_tests"]["rows"]
    assert report["robustness_harness"]["cost_sensitivity"]["rows"]
    assert report["robustness_harness"]["benchmark_comparison"]["rows"]
    assert report["robustness_harness"]["walk_forward"]["rows"]

    primary_output = json.loads(primary_output_json.read_text(encoding="utf-8"))
    shadow_output = json.loads(shadow_output_json.read_text(encoding="utf-8"))
    assert primary_output["template_output"]["signal"]["strategy"] == "primary_live_candidate_v1"
    assert shadow_output["template_output"]["signal"]["strategy"] == "primary_live_candidate_v1_vol_managed"
    assert "target_weights" in primary_output["template_output"]
    assert "diagnostics" in shadow_output["template_output"]
    assert "reports" in shadow_output["template_output"]

    markdown = comparison_report_markdown.read_text(encoding="utf-8")
    assert "## Scoreboard" in markdown
    assert "### Cost Sensitivity" in markdown
    assert "### Walk-Forward Windows" in markdown


def test_stage2_shadow_compare_cli_accepts_explicit_target_overrides(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    end = pd.Timestamp.now().normalize()
    index = pd.bdate_range(end=end, periods=520)
    alt = np.arange(len(index))

    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    store = LocalStore(base_dir=data_dir)
    close_map = {
        "SPY": _price_series(index, np.full(len(index), 0.0007), 100.0),
        "QQQ": _price_series(index, np.where(alt % 2 == 0, 0.0016, -0.0008), 105.0),
        "IWM": _price_series(index, np.full(len(index), -0.0001), 95.0),
        "EFA": _price_series(index, np.where(alt % 3 == 0, 0.0190, -0.0075), 98.0),
        "BIL": _price_series(index, np.full(len(index), 0.0001), 100.0),
    }
    for symbol, close in close_map.items():
        _write_symbol_bars(store, symbol, close)

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "stage2_shadow_compare.py"),
            "--pair-id",
            "primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed_alt",
            "--shadow-strategy-id",
            "primary_live_candidate_v1_vol_managed_alt",
            "--shadow-rebalance",
            "10",
            "--shadow-vol-target",
            "0.12",
            "--shadow-vol-lookback",
            "15",
            "--data-dir",
            str(data_dir),
            "--artifacts-dir",
            str(artifacts_dir),
            "--start",
            index[0].date().isoformat(),
            "--end",
            index[-1].date().isoformat(),
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    summary = json.loads(proc.stdout.strip())
    assert summary["pair_id"] == "primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed_alt"

    report = json.loads(Path(summary["report_json"]).read_text(encoding="utf-8"))
    assert report["pair_id"] == "primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed_alt"
    assert "primary_live_candidate_v1_vol_managed_alt" in report["candidates"]
    assert report["candidates"]["primary_live_candidate_v1_vol_managed_alt"]["parameters"]["rebalance"] == 10
    assert report["candidates"]["primary_live_candidate_v1_vol_managed_alt"]["parameters"]["vol_target"] == pytest.approx(0.12)
    assert report["candidates"]["primary_live_candidate_v1_vol_managed_alt"]["parameters"]["vol_lookback"] == 15
