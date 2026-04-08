from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.backtest import metrics
from trading_codex.backtest.engine import BacktestResult
from trading_codex.data import LocalStore
from trading_codex.shadow.stage2_compare import (
    Stage2CompareCandidate,
    build_drawdown_cluster_review,
    build_walk_forward_summary,
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
) -> Stage2CompareCandidate:
    weights = pd.DataFrame({"SPY": 1.0}, index=returns.index, dtype=float)
    turnover = pd.Series(0.0, index=returns.index, dtype=float)
    equity = (1.0 + returns).cumprod()
    result = BacktestResult(
        returns=returns,
        weights=weights,
        turnover=turnover,
        equity=equity,
    )
    return Stage2CompareCandidate(
        strategy_id=strategy_id,
        role="shadow",
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
            "annual_turnover": 0.0,
        },
        benchmark_returns=benchmark_returns,
        review_bundle={
            "shadow_review_state": "clean",
            "review_summary": {
                "automation_decision": "allow",
                "automation_status": "automation_ready",
            },
        },
        parameter_stability_rows=[],
        cost_sensitivity_rows=[],
        artifacts={},
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
