"""Stage 2 shadow-only primary-vs-shadow comparison helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_codex.backtest import metrics
from trading_codex.backtest.engine import BacktestResult

STAGE2_SHADOW_COMPARE_ARTIFACT_VERSION = 1
SCOREBOARD_COLUMNS = (
    "strategy_id",
    "role",
    "runtime_strategy",
    "cagr",
    "sharpe",
    "max_drawdown",
    "turnover",
    "percent_time_in_cash",
    "action_frequency",
    "walk_forward_quality",
    "current_decision",
    "shadow_review_state",
    "automation_decision",
    "automation_status",
    "latest_action",
    "latest_symbol",
    "next_rebalance",
)

_PAIRWISE_METRIC_DISPLAY_COLUMNS = (
    "metric",
    "primary",
    "shadow",
    "delta_shadow_minus_primary",
)

_SUBPERIOD_COLUMNS = (
    "strategy_id",
    "period",
    "start_date",
    "end_date",
    "observations",
    "cagr",
    "sharpe",
    "max_drawdown",
    "turnover",
    "percent_time_in_cash",
    "action_frequency",
)

_PARAMETER_STABILITY_COLUMNS = (
    "strategy_id",
    "variant_label",
    "parameter_name",
    "momentum_lookback",
    "rebalance",
    "target_vol",
    "vol_lookback",
    "cagr",
    "sharpe",
    "max_drawdown",
    "turnover",
    "action_frequency",
    "is_baseline",
)

_COST_SENSITIVITY_COLUMNS = (
    "strategy_id",
    "scenario",
    "slippage_bps",
    "commission_per_trade",
    "commission_bps",
    "cagr",
    "sharpe",
    "max_drawdown",
    "turnover",
    "total_estimated_cost",
)

_BENCHMARK_COLUMNS = (
    "strategy_id",
    "period",
    "cagr",
    "benchmark_cagr",
    "excess_cagr",
    "sharpe",
    "benchmark_sharpe",
    "sharpe_delta",
    "max_drawdown",
    "benchmark_max_drawdown",
    "drawdown_delta",
    "outperformed_benchmark",
)

_DRAWDOWN_CLUSTER_COLUMNS = (
    "strategy_id",
    "cluster_index",
    "start_date",
    "end_date",
    "trough_date",
    "duration_days",
    "worst_drawdown",
)

_WALK_FORWARD_COLUMNS = (
    "strategy_id",
    "window_index",
    "start_date",
    "end_date",
    "observations",
    "cagr",
    "sharpe",
    "max_drawdown",
    "benchmark_total_return",
    "outperformed_benchmark",
)


@dataclass(frozen=True)
class Stage2CompareCandidate:
    strategy_id: str
    role: str
    runtime_strategy: str
    implementation_label: str
    parameters: Mapping[str, Any]
    result: BacktestResult
    actions: pd.DataFrame
    next_action: Mapping[str, Any]
    metrics: Mapping[str, float]
    benchmark_returns: pd.Series | None
    review_bundle: Mapping[str, Any]
    parameter_stability_rows: Sequence[Mapping[str, Any]]
    cost_sensitivity_rows: Sequence[Mapping[str, Any]]
    artifacts: Mapping[str, str]


def _years_for_index(index: pd.DatetimeIndex) -> float:
    if len(index) == 0:
        return 0.0
    return float(len(index) / 252.0)


def _invested_mask(weights: pd.Series | pd.DataFrame) -> pd.Series:
    if isinstance(weights, pd.DataFrame):
        return weights.abs().sum(axis=1) > 0.0
    return weights.abs() > 0.0


def _action_dates(actions: pd.DataFrame) -> pd.Series:
    if actions.empty or "date" not in actions.columns:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(actions["date"], errors="coerce")


def _action_count(actions: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> int:
    dates = _action_dates(actions)
    if dates.empty:
        return 0
    mask = dates.ge(start) & dates.le(end)
    return int(mask.sum())


def _slice_result(
    result: BacktestResult,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> BacktestResult:
    returns = result.returns.loc[start:end]
    if returns.empty:
        empty = pd.Series(dtype=float, index=returns.index)
        return BacktestResult(
            returns=empty,
            weights=result.weights.iloc[0:0],  # type: ignore[index]
            turnover=result.turnover.iloc[0:0],
            equity=empty,
            gross_returns=empty if result.gross_returns is not None else None,
            gross_equity=empty if result.gross_equity is not None else None,
            cost_returns=empty if result.cost_returns is not None else None,
            estimated_costs=empty if result.estimated_costs is not None else None,
            trade_count=empty if result.trade_count is not None else None,
            leverage=empty if result.leverage is not None else None,
            realized_vol=empty if result.realized_vol is not None else None,
        )

    if isinstance(result.weights, pd.DataFrame):
        weights = result.weights.loc[returns.index]
    else:
        weights = result.weights.loc[returns.index]
    turnover = result.turnover.loc[returns.index]
    gross_returns = (
        result.gross_returns.loc[returns.index]
        if result.gross_returns is not None
        else returns
    )
    cost_returns = (
        result.cost_returns.loc[returns.index]
        if result.cost_returns is not None
        else gross_returns - returns
    )
    estimated_costs = (
        result.estimated_costs.loc[returns.index]
        if result.estimated_costs is not None
        else None
    )
    trade_count = (
        result.trade_count.loc[returns.index]
        if result.trade_count is not None
        else None
    )
    leverage = (
        result.leverage.loc[returns.index]
        if result.leverage is not None
        else None
    )
    realized_vol = (
        result.realized_vol.loc[returns.index]
        if result.realized_vol is not None
        else None
    )
    return BacktestResult(
        returns=returns,
        weights=weights,
        turnover=turnover,
        equity=(1.0 + returns).cumprod(),
        gross_returns=gross_returns,
        gross_equity=(1.0 + gross_returns).cumprod(),
        cost_returns=cost_returns,
        estimated_costs=estimated_costs,
        trade_count=trade_count,
        leverage=leverage,
        realized_vol=realized_vol,
    )


def _benchmark_summary(
    benchmark_returns: pd.Series | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any] | None:
    if benchmark_returns is None:
        return None
    benchmark_slice = benchmark_returns.loc[start:end]
    if benchmark_slice.empty:
        return None
    return {
        "cagr": float(metrics.cagr(benchmark_slice)),
        "sharpe": float(metrics.sharpe(benchmark_slice)),
        "max_drawdown": float(metrics.max_drawdown(benchmark_slice)),
        "total_return": float((1.0 + benchmark_slice).prod() - 1.0),
    }


def _result_summary(
    result: BacktestResult,
    *,
    actions: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any]:
    years = _years_for_index(result.returns.index)
    invested = _invested_mask(result.weights)
    exposure_pct = float(invested.mean() * 100.0) if len(invested) else 0.0
    action_count = _action_count(actions, start, end)
    action_frequency = float(action_count / years) if years > 0 else 0.0
    total_turnover = float(result.turnover.sum()) if len(result.turnover) else 0.0
    annual_turnover = float(total_turnover / years) if years > 0 else 0.0
    total_estimated_cost = (
        float(result.estimated_costs.sum())
        if result.estimated_costs is not None and len(result.estimated_costs)
        else 0.0
    )
    return {
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "observations": int(len(result.returns)),
        "cagr": float(metrics.cagr(result.returns)),
        "sharpe": float(metrics.sharpe(result.returns)),
        "max_drawdown": float(metrics.max_drawdown(result.returns)),
        "turnover": annual_turnover,
        "percent_time_in_cash": float(100.0 - exposure_pct),
        "action_count": action_count,
        "action_frequency": action_frequency,
        "total_return": float((1.0 + result.returns).prod() - 1.0) if len(result.returns) else 0.0,
        "total_estimated_cost": total_estimated_cost,
    }


def _subperiod_windows(index: pd.DatetimeIndex) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    if len(index) == 0:
        return []

    windows: list[tuple[str, pd.Timestamp, pd.Timestamp]] = [
        ("full", pd.Timestamp(index[0]), pd.Timestamp(index[-1]))
    ]
    if len(index) >= 2:
        mid = len(index) // 2
        windows.extend(
            [
                ("first_half", pd.Timestamp(index[0]), pd.Timestamp(index[max(mid - 1, 0)])),
                ("second_half", pd.Timestamp(index[mid]), pd.Timestamp(index[-1])),
            ]
        )
    if len(index) >= 756:
        windows.append(("recent_3y", pd.Timestamp(index[-756]), pd.Timestamp(index[-1])))
    elif len(index) >= 252:
        windows.append(("recent_1y", pd.Timestamp(index[-252]), pd.Timestamp(index[-1])))

    deduped: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
    seen: set[tuple[str, str]] = set()
    for label, start, end in windows:
        key = (start.date().isoformat(), end.date().isoformat())
        if key in seen:
            continue
        seen.add(key)
        deduped.append((label, start, end))
    return deduped


def build_subperiod_rows(candidate: Stage2CompareCandidate) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label, start, end in _subperiod_windows(candidate.result.returns.index):
        sliced = _slice_result(candidate.result, start, end)
        summary = _result_summary(sliced, actions=candidate.actions, start=start, end=end)
        rows.append(
            {
                "strategy_id": candidate.strategy_id,
                "period": label,
                **summary,
            }
        )
    return rows


def build_benchmark_rows(candidate: Stage2CompareCandidate) -> list[dict[str, Any]]:
    if candidate.benchmark_returns is None:
        return []

    rows: list[dict[str, Any]] = []
    for label, start, end in _subperiod_windows(candidate.result.returns.index):
        sliced = _slice_result(candidate.result, start, end)
        summary = _result_summary(sliced, actions=candidate.actions, start=start, end=end)
        benchmark = _benchmark_summary(candidate.benchmark_returns, start, end)
        if benchmark is None:
            continue
        rows.append(
            {
                "strategy_id": candidate.strategy_id,
                "period": label,
                "cagr": summary["cagr"],
                "benchmark_cagr": benchmark["cagr"],
                "excess_cagr": float(summary["cagr"] - benchmark["cagr"]),
                "sharpe": summary["sharpe"],
                "benchmark_sharpe": benchmark["sharpe"],
                "sharpe_delta": float(summary["sharpe"] - benchmark["sharpe"]),
                "max_drawdown": summary["max_drawdown"],
                "benchmark_max_drawdown": benchmark["max_drawdown"],
                "drawdown_delta": float(summary["max_drawdown"] - benchmark["max_drawdown"]),
                "outperformed_benchmark": bool(summary["total_return"] > benchmark["total_return"]),
            }
        )
    return rows


def build_walk_forward_summary(candidate: Stage2CompareCandidate) -> dict[str, Any]:
    index = candidate.result.returns.index
    if len(index) == 0:
        return {
            "summary": {
                "window_count": 0,
                "positive_cagr_fraction": 0.0,
                "positive_sharpe_fraction": 0.0,
                "benchmark_outperform_fraction": None,
                "quality_score": 0.0,
                "label": "weak",
                "summary_text": "weak; 0 windows",
            },
            "rows": [],
        }

    if len(index) < 63:
        split_count = 1
    elif len(index) < 126:
        split_count = 2
    else:
        split_count = min(4, max(2, len(index) // 126))

    positions = np.array_split(np.arange(len(index)), split_count)
    rows: list[dict[str, Any]] = []
    for window_index, position_slice in enumerate(positions, start=1):
        if len(position_slice) == 0:
            continue
        start = pd.Timestamp(index[int(position_slice[0])])
        end = pd.Timestamp(index[int(position_slice[-1])])
        sliced = _slice_result(candidate.result, start, end)
        summary = _result_summary(sliced, actions=candidate.actions, start=start, end=end)
        benchmark = _benchmark_summary(candidate.benchmark_returns, start, end)
        rows.append(
            {
                "strategy_id": candidate.strategy_id,
                "window_index": window_index,
                "start_date": summary["start_date"],
                "end_date": summary["end_date"],
                "observations": summary["observations"],
                "cagr": summary["cagr"],
                "sharpe": summary["sharpe"],
                "max_drawdown": summary["max_drawdown"],
                "benchmark_total_return": None if benchmark is None else benchmark["total_return"],
                "outperformed_benchmark": (
                    None
                    if benchmark is None
                    else bool(summary["total_return"] > benchmark["total_return"])
                ),
            }
        )

    window_count = len(rows)
    positive_cagr_fraction = (
        float(sum(float(row["cagr"]) > 0.0 for row in rows) / window_count)
        if window_count
        else 0.0
    )
    positive_sharpe_fraction = (
        float(sum(float(row["sharpe"]) > 0.0 for row in rows) / window_count)
        if window_count
        else 0.0
    )
    benchmark_flags = [
        row["outperformed_benchmark"]
        for row in rows
        if row["outperformed_benchmark"] is not None
    ]
    benchmark_outperform_fraction = (
        float(sum(bool(item) for item in benchmark_flags) / len(benchmark_flags))
        if benchmark_flags
        else None
    )
    components = [positive_cagr_fraction, positive_sharpe_fraction]
    if benchmark_outperform_fraction is not None:
        components.append(float(benchmark_outperform_fraction))
    quality_score = float(sum(components) / len(components)) if components else 0.0
    if quality_score >= 0.67 and positive_sharpe_fraction >= 0.50:
        label = "strong"
    elif quality_score >= 0.45:
        label = "mixed"
    else:
        label = "weak"

    summary_text = (
        f"{label}; {sum(float(row['sharpe']) > 0.0 for row in rows)}/{window_count} "
        "positive-Sharpe windows"
    )
    if benchmark_outperform_fraction is not None:
        summary_text += (
            f"; {sum(bool(item) for item in benchmark_flags)}/{len(benchmark_flags)} benchmark wins"
        )

    return {
        "summary": {
            "window_count": window_count,
            "positive_cagr_fraction": positive_cagr_fraction,
            "positive_sharpe_fraction": positive_sharpe_fraction,
            "benchmark_outperform_fraction": benchmark_outperform_fraction,
            "quality_score": quality_score,
            "label": label,
            "summary_text": summary_text,
        },
        "rows": rows,
    }


def build_drawdown_cluster_review(
    candidate: Stage2CompareCandidate,
    *,
    threshold: float = -0.05,
    merge_gap: int = 21,
) -> dict[str, Any]:
    equity = candidate.result.equity
    if equity.empty:
        return {
            "summary": {
                "label": "none",
                "cluster_count": 0,
                "worst_cluster_drawdown": 0.0,
                "longest_cluster_days": 0,
                "summary_text": "none; no drawdown clusters",
            },
            "rows": [],
        }

    drawdown = (equity / equity.cummax()) - 1.0
    severe_positions = np.flatnonzero((drawdown <= float(threshold)).to_numpy())
    if len(severe_positions) == 0:
        return {
            "summary": {
                "label": "none",
                "cluster_count": 0,
                "worst_cluster_drawdown": float(drawdown.min()),
                "longest_cluster_days": 0,
                "summary_text": "none; no severe drawdown clusters",
            },
            "rows": [],
        }

    clusters: list[tuple[int, int]] = []
    cluster_start = int(severe_positions[0])
    cluster_end = int(severe_positions[0])
    for raw_pos in severe_positions[1:]:
        pos = int(raw_pos)
        if pos - cluster_end <= int(merge_gap):
            cluster_end = pos
            continue
        clusters.append((cluster_start, cluster_end))
        cluster_start = pos
        cluster_end = pos
    clusters.append((cluster_start, cluster_end))

    rows: list[dict[str, Any]] = []
    for cluster_index, (start_pos, end_pos) in enumerate(clusters, start=1):
        segment = drawdown.iloc[start_pos : end_pos + 1]
        trough_date = pd.Timestamp(segment.idxmin())
        rows.append(
            {
                "strategy_id": candidate.strategy_id,
                "cluster_index": cluster_index,
                "start_date": pd.Timestamp(drawdown.index[start_pos]).date().isoformat(),
                "end_date": pd.Timestamp(drawdown.index[end_pos]).date().isoformat(),
                "trough_date": trough_date.date().isoformat(),
                "duration_days": int(end_pos - start_pos + 1),
                "worst_drawdown": float(segment.min()),
            }
        )

    worst_cluster_drawdown = float(min(float(row["worst_drawdown"]) for row in rows))
    longest_cluster_days = int(max(int(row["duration_days"]) for row in rows))
    label = "clustered" if len(rows) >= 2 else "contained"
    summary_text = (
        f"{label}; {len(rows)} severe clusters, worst {worst_cluster_drawdown:.4f}, "
        f"longest {longest_cluster_days} days"
    )
    return {
        "summary": {
            "label": label,
            "cluster_count": len(rows),
            "worst_cluster_drawdown": worst_cluster_drawdown,
            "longest_cluster_days": longest_cluster_days,
            "summary_text": summary_text,
        },
        "rows": rows,
    }


def summarize_parameter_stability(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    materialized = [dict(row) for row in rows]
    baseline = next((row for row in materialized if bool(row.get("is_baseline"))), None)
    if baseline is None:
        return {
            "variant_count": len(materialized),
            "positive_cagr_fraction": 0.0,
            "positive_sharpe_fraction": 0.0,
            "median_sharpe": 0.0,
            "median_cagr": 0.0,
            "baseline_variant": None,
            "baseline_sharpe_rank": None,
            "label": "fragile",
            "summary_text": "fragile; baseline missing",
        }

    sharpe_values = [float(row.get("sharpe", 0.0)) for row in materialized]
    cagr_values = [float(row.get("cagr", 0.0)) for row in materialized]
    positive_cagr_fraction = float(sum(value > 0.0 for value in cagr_values) / len(cagr_values))
    positive_sharpe_fraction = float(sum(value > 0.0 for value in sharpe_values) / len(sharpe_values))
    median_sharpe = float(pd.Series(sharpe_values).median()) if sharpe_values else 0.0
    median_cagr = float(pd.Series(cagr_values).median()) if cagr_values else 0.0
    baseline_sharpe = float(baseline.get("sharpe", 0.0))
    baseline_rank = 1 + sum(value > baseline_sharpe for value in sharpe_values)

    if (
        positive_cagr_fraction >= 0.60
        and positive_sharpe_fraction >= 0.60
        and baseline_sharpe >= (median_sharpe - 0.15)
    ):
        label = "stable"
    elif positive_cagr_fraction >= 0.40 and positive_sharpe_fraction >= 0.40:
        label = "mixed"
    else:
        label = "fragile"

    summary_text = (
        f"{label}; {sum(value > 0.0 for value in sharpe_values)}/{len(sharpe_values)} "
        "positive-Sharpe variants"
    )
    return {
        "variant_count": len(materialized),
        "positive_cagr_fraction": positive_cagr_fraction,
        "positive_sharpe_fraction": positive_sharpe_fraction,
        "median_sharpe": median_sharpe,
        "median_cagr": median_cagr,
        "baseline_variant": baseline.get("variant_label"),
        "baseline_sharpe_rank": baseline_rank,
        "label": label,
        "summary_text": summary_text,
    }


def _scoreboard_row(
    candidate: Stage2CompareCandidate,
    *,
    walk_forward_summary: Mapping[str, Any],
    current_decision: str,
) -> dict[str, Any]:
    review_summary = candidate.review_bundle.get("review_summary")
    review_summary = review_summary if isinstance(review_summary, Mapping) else {}
    full_summary = _result_summary(
        candidate.result,
        actions=candidate.actions,
        start=pd.Timestamp(candidate.result.returns.index[0]),
        end=pd.Timestamp(candidate.result.returns.index[-1]),
    )
    return {
        "strategy_id": candidate.strategy_id,
        "role": candidate.role,
        "runtime_strategy": candidate.runtime_strategy,
        "cagr": float(candidate.metrics.get("cagr", full_summary["cagr"])),
        "sharpe": float(candidate.metrics.get("sharpe", full_summary["sharpe"])),
        "max_drawdown": float(candidate.metrics.get("max_drawdown", full_summary["max_drawdown"])),
        "turnover": float(candidate.metrics.get("annual_turnover", full_summary["turnover"])),
        "percent_time_in_cash": float(full_summary["percent_time_in_cash"]),
        "action_frequency": float(full_summary["action_frequency"]),
        "walk_forward_quality": str(walk_forward_summary.get("summary_text", "-")),
        "current_decision": current_decision,
        "shadow_review_state": str(candidate.review_bundle.get("shadow_review_state", "-")),
        "automation_decision": str(review_summary.get("automation_decision", "-")),
        "automation_status": str(review_summary.get("automation_status", "-")),
        "latest_action": str(candidate.next_action.get("action", "-")),
        "latest_symbol": str(candidate.next_action.get("symbol", "-")),
        "next_rebalance": (
            None if candidate.next_action.get("next_rebalance") is None else str(candidate.next_action.get("next_rebalance"))
        ),
    }


def derive_shadow_candidate_decision(
    *,
    primary_row: Mapping[str, Any],
    shadow_row: Mapping[str, Any],
    shadow_review_bundle: Mapping[str, Any],
    shadow_walk_forward_summary: Mapping[str, Any],
    shadow_parameter_summary: Mapping[str, Any],
) -> str:
    review_state = str(shadow_review_bundle.get("shadow_review_state", "-"))
    walk_forward_label = str(shadow_walk_forward_summary.get("label", "weak"))
    parameter_label = str(shadow_parameter_summary.get("label", "fragile"))
    sharpe_delta = float(shadow_row["sharpe"]) - float(primary_row["sharpe"])
    cagr_delta = float(shadow_row["cagr"]) - float(primary_row["cagr"])
    drawdown_delta = float(shadow_row["max_drawdown"]) - float(primary_row["max_drawdown"])

    if review_state == "blocked":
        return "not advancing"
    if parameter_label == "fragile" or walk_forward_label == "weak":
        return "not advancing"
    if sharpe_delta <= -0.20 or cagr_delta <= -0.03 or drawdown_delta <= -0.05:
        return "not advancing"
    if (
        sharpe_delta >= 0.10
        and cagr_delta >= 0.0
        and drawdown_delta >= -0.02
        and parameter_label == "stable"
        and walk_forward_label in {"strong", "mixed"}
    ):
        return "candidate for later paper promotion after Stage 2 exit"
    return "remain shadow-only"


def _pairwise_metric_rows(
    primary_row: Mapping[str, Any],
    shadow_row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    metric_names = (
        "cagr",
        "sharpe",
        "max_drawdown",
        "turnover",
        "percent_time_in_cash",
        "action_frequency",
    )
    rows: list[dict[str, Any]] = []
    for name in metric_names:
        primary_value = float(primary_row[name])
        shadow_value = float(shadow_row[name])
        rows.append(
            {
                "metric": name,
                "primary": primary_value,
                "shadow": shadow_value,
                "delta_shadow_minus_primary": float(shadow_value - primary_value),
            }
        )
    return rows


def build_stage2_shadow_compare_report(
    *,
    pair_id: str,
    as_of_date: str,
    generated_at: str,
    command: str,
    data_dir: str,
    primary: Stage2CompareCandidate,
    shadow: Stage2CompareCandidate,
    primary_mapping: Mapping[str, Any],
    shadow_runtime: Mapping[str, Any],
) -> dict[str, Any]:
    primary_parameter_summary = summarize_parameter_stability(primary.parameter_stability_rows)
    shadow_parameter_summary = summarize_parameter_stability(shadow.parameter_stability_rows)
    primary_walk_forward = build_walk_forward_summary(primary)
    shadow_walk_forward = build_walk_forward_summary(shadow)
    primary_drawdown = build_drawdown_cluster_review(primary)
    shadow_drawdown = build_drawdown_cluster_review(shadow)
    primary_subperiod_rows = build_subperiod_rows(primary)
    shadow_subperiod_rows = build_subperiod_rows(shadow)
    primary_benchmark_rows = build_benchmark_rows(primary)
    shadow_benchmark_rows = build_benchmark_rows(shadow)

    primary_row = _scoreboard_row(
        primary,
        walk_forward_summary=primary_walk_forward["summary"],
        current_decision="approved primary baseline",
    )
    provisional_shadow_row = _scoreboard_row(
        shadow,
        walk_forward_summary=shadow_walk_forward["summary"],
        current_decision="remain shadow-only",
    )
    shadow_decision = derive_shadow_candidate_decision(
        primary_row=primary_row,
        shadow_row=provisional_shadow_row,
        shadow_review_bundle=shadow.review_bundle,
        shadow_walk_forward_summary=shadow_walk_forward["summary"],
        shadow_parameter_summary=shadow_parameter_summary,
    )
    shadow_row = dict(provisional_shadow_row)
    shadow_row["current_decision"] = shadow_decision

    pairwise_rows = _pairwise_metric_rows(primary_row, shadow_row)
    action_comparison = {
        "primary_action": str(primary.next_action.get("action", "-")),
        "shadow_action": str(shadow.next_action.get("action", "-")),
        "primary_symbol": str(primary.next_action.get("symbol", "-")),
        "shadow_symbol": str(shadow.next_action.get("symbol", "-")),
        "same_action": str(primary.next_action.get("action")) == str(shadow.next_action.get("action")),
        "same_symbol": str(primary.next_action.get("symbol")) == str(shadow.next_action.get("symbol")),
        "primary_target_shares": primary.next_action.get("target_shares"),
        "shadow_target_shares": shadow.next_action.get("target_shares"),
        "primary_next_rebalance": primary.next_action.get("next_rebalance"),
        "shadow_next_rebalance": shadow.next_action.get("next_rebalance"),
    }

    return {
        "artifact_type": "stage2_shadow_compare",
        "artifact_version": STAGE2_SHADOW_COMPARE_ARTIFACT_VERSION,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "as_of_date": as_of_date,
        "command": command,
        "data_dir": data_dir,
        "current_decision": shadow_decision,
        "control_plane": {
            "primary_mapping": dict(primary_mapping),
            "shadow_runtime": dict(shadow_runtime),
        },
        "candidates": {
            primary.strategy_id: {
                "role": primary.role,
                "runtime_strategy": primary.runtime_strategy,
                "implementation_label": primary.implementation_label,
                "parameters": dict(primary.parameters),
                "artifacts": dict(primary.artifacts),
                "next_action": dict(primary.next_action),
                "review_summary": dict(primary.review_bundle.get("review_summary", {})),
                "parameter_stability_summary": primary_parameter_summary,
                "walk_forward_summary": primary_walk_forward["summary"],
                "drawdown_cluster_summary": primary_drawdown["summary"],
            },
            shadow.strategy_id: {
                "role": shadow.role,
                "runtime_strategy": shadow.runtime_strategy,
                "implementation_label": shadow.implementation_label,
                "parameters": dict(shadow.parameters),
                "artifacts": dict(shadow.artifacts),
                "next_action": dict(shadow.next_action),
                "review_summary": dict(shadow.review_bundle.get("review_summary", {})),
                "parameter_stability_summary": shadow_parameter_summary,
                "walk_forward_summary": shadow_walk_forward["summary"],
                "drawdown_cluster_summary": shadow_drawdown["summary"],
            },
        },
        "comparison": {
            "action_comparison": action_comparison,
            "metric_rows": pairwise_rows,
        },
        "scoreboard": {
            "columns": SCOREBOARD_COLUMNS,
            "rows": [primary_row, shadow_row],
        },
        "robustness_harness": {
            "parameter_stability": {
                "columns": _PARAMETER_STABILITY_COLUMNS,
                "rows": [
                    *[dict(row) for row in primary.parameter_stability_rows],
                    *[dict(row) for row in shadow.parameter_stability_rows],
                ],
                "summaries": {
                    primary.strategy_id: primary_parameter_summary,
                    shadow.strategy_id: shadow_parameter_summary,
                },
            },
            "subperiod_tests": {
                "columns": _SUBPERIOD_COLUMNS,
                "rows": [*primary_subperiod_rows, *shadow_subperiod_rows],
            },
            "cost_sensitivity": {
                "columns": _COST_SENSITIVITY_COLUMNS,
                "rows": [
                    *[dict(row) for row in primary.cost_sensitivity_rows],
                    *[dict(row) for row in shadow.cost_sensitivity_rows],
                ],
            },
            "benchmark_comparison": {
                "columns": _BENCHMARK_COLUMNS,
                "rows": [*primary_benchmark_rows, *shadow_benchmark_rows],
            },
            "drawdown_clustering": {
                "columns": _DRAWDOWN_CLUSTER_COLUMNS,
                "rows": [*primary_drawdown["rows"], *shadow_drawdown["rows"]],
                "summaries": {
                    primary.strategy_id: primary_drawdown["summary"],
                    shadow.strategy_id: shadow_drawdown["summary"],
                },
            },
            "walk_forward": {
                "columns": _WALK_FORWARD_COLUMNS,
                "rows": [*primary_walk_forward["rows"], *shadow_walk_forward["rows"]],
                "summaries": {
                    primary.strategy_id: primary_walk_forward["summary"],
                    shadow.strategy_id: shadow_walk_forward["summary"],
                },
            },
        },
    }


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _markdown_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    if not rows:
        headers = [col.replace("_", " ") for col in columns]
        return "\n".join(
            [
                "| " + " | ".join(headers) + " |",
                "| " + " | ".join(["---"] * len(columns)) + " |",
            ]
        )

    headers = [col.replace("_", " ") for col in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_fmt(row.get(column)) for column in columns) + " |")
    return "\n".join(lines)


def render_stage2_shadow_compare_markdown(report: Mapping[str, Any]) -> str:
    scoreboard = report["scoreboard"]["rows"]
    pairwise = report["comparison"]["metric_rows"]
    robustness = report["robustness_harness"]
    candidate_map = report["candidates"]
    primary_id, shadow_id = tuple(candidate_map.keys())
    primary_candidate = candidate_map[primary_id]
    shadow_candidate = candidate_map[shadow_id]
    action_comparison = report["comparison"]["action_comparison"]
    parameter_rows = robustness["parameter_stability"]["rows"]
    subperiod_rows = robustness["subperiod_tests"]["rows"]
    cost_rows = robustness["cost_sensitivity"]["rows"]
    benchmark_rows = robustness["benchmark_comparison"]["rows"]
    drawdown_rows = robustness["drawdown_clustering"]["rows"]
    walk_forward_rows = robustness["walk_forward"]["rows"]

    lines = [
        f"# Stage 2 Shadow Compare {report['pair_id']}",
        "",
        f"- Artifact version: `{report['artifact_version']}`",
        f"- Generated at: `{report['generated_at']}`",
        f"- As-of date: `{report['as_of_date']}`",
        f"- Data dir: `{report['data_dir']}`",
        f"- Current decision: `{report['current_decision']}`",
        f"- Command: `{report['command']}`",
        "",
        "## Scoreboard",
        "",
        _markdown_table(scoreboard, SCOREBOARD_COLUMNS),
        "",
        "## Candidate Artifacts",
        "",
        f"- `{primary_id}` output JSON: `{primary_candidate['artifacts'].get('template_output_json', '-')}`",
        f"- `{primary_id}` review JSON: `{primary_candidate['artifacts'].get('review_json', '-')}`",
        f"- `{primary_id}` review Markdown: `{primary_candidate['artifacts'].get('review_markdown', '-')}`",
        f"- `{shadow_id}` output JSON: `{shadow_candidate['artifacts'].get('template_output_json', '-')}`",
        f"- `{shadow_id}` review JSON: `{shadow_candidate['artifacts'].get('review_json', '-')}`",
        f"- `{shadow_id}` review Markdown: `{shadow_candidate['artifacts'].get('review_markdown', '-')}`",
        "",
        "## Latest Action Comparison",
        "",
        f"- Primary action: `{action_comparison['primary_action']}` on `{action_comparison['primary_symbol']}`",
        f"- Shadow action: `{action_comparison['shadow_action']}` on `{action_comparison['shadow_symbol']}`",
        f"- Same action: `{str(bool(action_comparison['same_action'])).lower()}`",
        f"- Same symbol: `{str(bool(action_comparison['same_symbol'])).lower()}`",
        "",
        "## Pairwise Metrics",
        "",
        _markdown_table(pairwise, _PAIRWISE_METRIC_DISPLAY_COLUMNS),
        "",
        "## Robustness Harness",
        "",
        f"- `{primary_id}` parameter stability: `{primary_candidate['parameter_stability_summary']['summary_text']}`",
        f"- `{shadow_id}` parameter stability: `{shadow_candidate['parameter_stability_summary']['summary_text']}`",
        f"- `{primary_id}` walk-forward: `{primary_candidate['walk_forward_summary']['summary_text']}`",
        f"- `{shadow_id}` walk-forward: `{shadow_candidate['walk_forward_summary']['summary_text']}`",
        f"- `{primary_id}` drawdown clustering: `{primary_candidate['drawdown_cluster_summary']['summary_text']}`",
        f"- `{shadow_id}` drawdown clustering: `{shadow_candidate['drawdown_cluster_summary']['summary_text']}`",
        "",
        "### Parameter Stability",
        "",
        _markdown_table(parameter_rows, _PARAMETER_STABILITY_COLUMNS),
        "",
        "### Subperiod Tests",
        "",
        _markdown_table(subperiod_rows, _SUBPERIOD_COLUMNS),
        "",
        "### Cost Sensitivity",
        "",
        _markdown_table(cost_rows, _COST_SENSITIVITY_COLUMNS),
        "",
        "### Benchmark Comparison",
        "",
        _markdown_table(benchmark_rows, _BENCHMARK_COLUMNS),
        "",
        "### Drawdown Clusters",
        "",
        _markdown_table(drawdown_rows, _DRAWDOWN_CLUSTER_COLUMNS),
        "",
        "### Walk-Forward Windows",
        "",
        _markdown_table(walk_forward_rows, _WALK_FORWARD_COLUMNS),
        "",
    ]
    return "\n".join(lines)


def _write_csv(
    path: Path,
    *,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        pd.DataFrame(columns=list(columns)).to_csv(path, index=False)
        return
    frame = pd.DataFrame([dict(row) for row in rows])
    frame = frame.reindex(columns=list(columns))
    frame.to_csv(path, index=False)


def write_stage2_shadow_compare_artifacts(
    *,
    report_dir: Path,
    report: Mapping[str, Any],
) -> dict[str, str]:
    report_dir.mkdir(parents=True, exist_ok=True)
    robustness_dir = report_dir / "robustness"
    report_json = report_dir / "comparison_report.json"
    report_markdown = report_dir / "comparison_report.md"
    scoreboard_csv = report_dir / "scoreboard.csv"
    comparison_metrics_csv = report_dir / "comparison_metrics.csv"
    parameter_stability_csv = robustness_dir / "parameter_stability.csv"
    subperiod_csv = robustness_dir / "subperiod_tests.csv"
    cost_csv = robustness_dir / "cost_sensitivity.csv"
    benchmark_csv = robustness_dir / "benchmark_comparison.csv"
    drawdown_csv = robustness_dir / "drawdown_clusters.csv"
    walk_forward_csv = robustness_dir / "walk_forward.csv"

    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_markdown.write_text(render_stage2_shadow_compare_markdown(report), encoding="utf-8")
    _write_csv(
        scoreboard_csv,
        columns=report["scoreboard"]["columns"],
        rows=report["scoreboard"]["rows"],
    )
    _write_csv(
        comparison_metrics_csv,
        columns=_PAIRWISE_METRIC_DISPLAY_COLUMNS,
        rows=report["comparison"]["metric_rows"],
    )
    _write_csv(
        parameter_stability_csv,
        columns=report["robustness_harness"]["parameter_stability"]["columns"],
        rows=report["robustness_harness"]["parameter_stability"]["rows"],
    )
    _write_csv(
        subperiod_csv,
        columns=report["robustness_harness"]["subperiod_tests"]["columns"],
        rows=report["robustness_harness"]["subperiod_tests"]["rows"],
    )
    _write_csv(
        cost_csv,
        columns=report["robustness_harness"]["cost_sensitivity"]["columns"],
        rows=report["robustness_harness"]["cost_sensitivity"]["rows"],
    )
    _write_csv(
        benchmark_csv,
        columns=report["robustness_harness"]["benchmark_comparison"]["columns"],
        rows=report["robustness_harness"]["benchmark_comparison"]["rows"],
    )
    _write_csv(
        drawdown_csv,
        columns=report["robustness_harness"]["drawdown_clustering"]["columns"],
        rows=report["robustness_harness"]["drawdown_clustering"]["rows"],
    )
    _write_csv(
        walk_forward_csv,
        columns=report["robustness_harness"]["walk_forward"]["columns"],
        rows=report["robustness_harness"]["walk_forward"]["rows"],
    )
    return {
        "report_json": str(report_json),
        "report_markdown": str(report_markdown),
        "scoreboard_csv": str(scoreboard_csv),
        "comparison_metrics_csv": str(comparison_metrics_csv),
        "parameter_stability_csv": str(parameter_stability_csv),
        "subperiod_tests_csv": str(subperiod_csv),
        "cost_sensitivity_csv": str(cost_csv),
        "benchmark_comparison_csv": str(benchmark_csv),
        "drawdown_clusters_csv": str(drawdown_csv),
        "walk_forward_csv": str(walk_forward_csv),
    }
