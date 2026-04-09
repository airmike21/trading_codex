#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import run_backtest as run_backtest_script
except ImportError:  # pragma: no cover - direct script execution path
    import run_backtest as run_backtest_script  # type: ignore[no-redef]

from trading_codex.backtest.engine import run_backtest
from trading_codex.backtest.shadow_artifacts import write_shadow_review_artifacts
from trading_codex.data import LocalStore
from trading_codex.run_archive import resolve_archive_root
from trading_codex.shadow import (
    PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL,
    PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK,
    PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS,
    PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_REBALANCE,
    PRIMARY_LIVE_CANDIDATE_V1_ID,
    PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_TARGET_VOL,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_LOOKBACK,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
    build_shadow_template_for_strategy,
    primary_live_candidate_v1_runtime_mapping,
    resolve_shadow_runtime_config,
)
from trading_codex.shadow.stage2_compare import (
    Stage2CompareCandidate,
    build_stage2_shadow_compare_report,
    write_stage2_shadow_compare_artifacts,
)
from trading_codex.strategies.dual_mom_v1 import DualMomentumV1Strategy
from trading_codex.strategies.dual_mom_vol10_cash import DualMomentumVol10CashStrategy

DEFAULT_COST_ASSUMPTIONS = {
    "slippage_bps": 5.0,
    "commission_per_trade": 0.0,
    "commission_bps": 0.0,
}
DEFAULT_PAIR_ID = f"{PRIMARY_LIVE_CANDIDATE_V1_ID}_vs_{PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID}"


@dataclass(frozen=True)
class CandidateSpec:
    strategy_id: str
    role: str
    runtime_strategy: str
    implementation_label: str
    template_family_id: str | None
    risk_symbols: tuple[str, ...]
    defensive_symbol: str
    momentum_lookback: int
    rebalance: int
    vol_target: float
    vol_lookback: int
    top_n: int = 1
    vol_min: float = 0.0
    vol_max: float = 1.0
    vol_update: str = "rebalance"
    uses_internal_vol_sizing: bool = False

    def parameter_payload(self) -> dict[str, Any]:
        return {
            "risk_symbols": list(self.risk_symbols),
            "defensive_symbol": self.defensive_symbol,
            "momentum_lookback": self.momentum_lookback,
            "rebalance": self.rebalance,
            "top_n": self.top_n,
            "vol_target": self.vol_target,
            "vol_lookback": self.vol_lookback,
            "vol_min": self.vol_min,
            "vol_max": self.vol_max,
            "vol_update": self.vol_update,
            "uses_internal_vol_sizing": self.uses_internal_vol_sizing,
        }


def _default_artifacts_dir(pair_id: str) -> Path:
    return resolve_archive_root(create=True) / "stage2_shadow_compare" / str(pair_id).strip()


def _default_data_dir() -> Path:
    return REPO_ROOT / "data"


def _primary_spec() -> CandidateSpec:
    return CandidateSpec(
        strategy_id=PRIMARY_LIVE_CANDIDATE_V1_ID,
        role="primary",
        runtime_strategy=PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY,
        implementation_label=PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY,
        template_family_id=None,
        risk_symbols=tuple(PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS),
        defensive_symbol=PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL,
        momentum_lookback=PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK,
        rebalance=PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_REBALANCE,
        vol_target=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_TARGET_VOL,
        vol_lookback=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_LOOKBACK,
        uses_internal_vol_sizing=True,
    )


def _shadow_spec(args: argparse.Namespace) -> CandidateSpec:
    shadow_config = resolve_shadow_runtime_config(
        args.shadow_strategy_family,
        strategy_id=args.shadow_strategy_id,
        symbols=_parse_symbol_list(args.shadow_risk_symbols),
        defensive_symbol=args.shadow_defensive_symbol,
        momentum_lookback=args.shadow_momentum_lookback,
        top_n=args.shadow_top_n,
        rebalance=args.shadow_rebalance,
        vol_target=args.shadow_vol_target,
        vol_lookback=args.shadow_vol_lookback,
        vol_min=args.shadow_vol_min,
        vol_max=args.shadow_vol_max,
        vol_update=args.shadow_vol_update,
    )
    return CandidateSpec(
        strategy_id=shadow_config.strategy_id,
        role="shadow",
        runtime_strategy=shadow_config.implementation_strategy,
        implementation_label=shadow_config.implementation_label,
        template_family_id=shadow_config.template_family_id,
        risk_symbols=tuple(shadow_config.risk_symbols),
        defensive_symbol=shadow_config.defensive_symbol,
        momentum_lookback=shadow_config.momentum_lookback,
        rebalance=shadow_config.rebalance,
        vol_target=shadow_config.vol_target,
        vol_lookback=shadow_config.vol_lookback,
        top_n=shadow_config.top_n,
        vol_min=shadow_config.vol_min,
        vol_max=shadow_config.vol_max,
        vol_update=shadow_config.vol_update,
        uses_internal_vol_sizing=False,
    )


def _parse_symbol_list(value: str | None) -> tuple[str, ...] | None:
    if value is None:
        return None
    symbols: list[str] = []
    seen: set[str] = set()
    for item in value.split(","):
        rendered = str(item).strip().upper()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        symbols.append(rendered)
    if not symbols:
        raise ValueError("shadow risk symbol override must contain at least one symbol.")
    return tuple(symbols)


def _latest_series_value(series: pd.Series | None) -> float | None:
    if series is None or series.empty:
        return None
    value = series.iloc[-1]
    if pd.isna(value):
        return None
    return float(value)


def _build_runtime(
    spec: CandidateSpec,
    *,
    momentum_lookback: int | None = None,
    rebalance: int | None = None,
    target_vol: float | None = None,
    vol_lookback: int | None = None,
) -> tuple[object, dict[str, Any]]:
    effective_momentum = int(momentum_lookback if momentum_lookback is not None else spec.momentum_lookback)
    effective_rebalance = int(rebalance if rebalance is not None else spec.rebalance)
    effective_target_vol = float(target_vol if target_vol is not None else spec.vol_target)
    effective_vol_lookback = int(vol_lookback if vol_lookback is not None else spec.vol_lookback)

    if spec.uses_internal_vol_sizing:
        strategy = DualMomentumVol10CashStrategy(
            symbols=spec.risk_symbols,
            defensive_symbol=spec.defensive_symbol,
            momentum_lookback=effective_momentum,
            rebalance=effective_rebalance,
            vol_lookback=effective_vol_lookback,
            target_vol=effective_target_vol,
        )
        return strategy, {
            "rebalance": effective_rebalance,
            "engine_vol_target": None,
            "engine_vol_lookback": effective_vol_lookback,
            "reported_vol_target": effective_target_vol,
            "vol_update": "rebalance",
            "allow_resize_without_vol_target": True,
            "momentum_lookback": effective_momentum,
            "vol_lookback": effective_vol_lookback,
            "target_vol": effective_target_vol,
        }

    strategy = DualMomentumV1Strategy(
        symbols=spec.risk_symbols,
        lookback=effective_momentum,
        top_n=spec.top_n,
        rebalance=effective_rebalance,
        defensive_symbol=spec.defensive_symbol,
    )
    return strategy, {
        "rebalance": effective_rebalance,
        "engine_vol_target": effective_target_vol,
        "engine_vol_lookback": effective_vol_lookback,
        "reported_vol_target": effective_target_vol,
        "vol_update": spec.vol_update,
        "allow_resize_without_vol_target": False,
        "momentum_lookback": effective_momentum,
        "vol_lookback": effective_vol_lookback,
        "target_vol": effective_target_vol,
    }


def _benchmark_returns(
    *,
    store: LocalStore,
    bars: pd.DataFrame,
    benchmark_symbol: str = "SPY",
) -> pd.Series:
    if isinstance(bars.columns, pd.MultiIndex):
        close_panel = bars.xs("close", axis=1, level=1)
        if benchmark_symbol in close_panel.columns:
            return close_panel[benchmark_symbol].astype(float).pct_change().fillna(0.0)

    benchmark_bars = store.read_bars(str(benchmark_symbol), start=bars.index[0], end=bars.index[-1])
    benchmark_returns = benchmark_bars["close"].astype(float).pct_change().fillna(0.0)
    return benchmark_returns.reindex(bars.index).fillna(0.0)


def _expected_actual_symbol_count(
    bars: pd.DataFrame,
    *,
    as_of_date: str,
) -> tuple[int | None, int | None]:
    if not isinstance(bars.columns, pd.MultiIndex):
        return None, None

    expected_symbol_count = len(bars.columns.get_level_values(0).unique().tolist())
    try:
        close_row = bars.xs("close", axis=1, level=1).loc[as_of_date]
    except KeyError:
        return expected_symbol_count, 0
    return expected_symbol_count, int(close_row.notna().sum())


def _write_candidate_output_json(
    *,
    candidate_outputs_dir: Path,
    spec: CandidateSpec,
    template_output: Mapping[str, Any],
) -> Path:
    candidate_outputs_dir.mkdir(parents=True, exist_ok=True)
    output_path = candidate_outputs_dir / f"{spec.strategy_id}.json"
    payload = {
        "artifact_type": "stage2_shadow_candidate_output",
        "artifact_version": 1,
        "strategy_id": spec.strategy_id,
        "role": spec.role,
        "runtime_strategy": spec.runtime_strategy,
        "implementation_label": spec.implementation_label,
        "parameters": spec.parameter_payload(),
        "template_output": dict(template_output),
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _int_neighborhood(base: int, *, step: int, minimum: int) -> tuple[int, ...]:
    values = [max(minimum, int(base - step)), int(base), int(base + step)]
    deduped: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return tuple(deduped)


def _float_neighborhood(base: float, *, step: float, minimum: float) -> tuple[float, ...]:
    values = [max(minimum, float(base - step)), float(base), float(base + step)]
    deduped: list[float] = []
    seen: set[str] = set()
    for value in values:
        key = f"{value:.6f}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(float(value))
    return tuple(deduped)


def _parameter_variants(spec: CandidateSpec) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = [
        {
            "variant_label": "baseline",
            "parameter_name": "baseline",
            "momentum_lookback": spec.momentum_lookback,
            "rebalance": spec.rebalance,
            "target_vol": spec.vol_target,
            "vol_lookback": spec.vol_lookback,
            "is_baseline": True,
        }
    ]
    seen_labels = {"baseline"}

    def add_variant(parameter_name: str, *, momentum_lookback: int, rebalance: int, target_vol: float, vol_lookback: int) -> None:
        label = (
            f"{parameter_name}_m{momentum_lookback}_r{rebalance}_tv{target_vol:.2f}_vl{vol_lookback}"
        )
        if label in seen_labels:
            return
        seen_labels.add(label)
        variants.append(
            {
                "variant_label": label,
                "parameter_name": parameter_name,
                "momentum_lookback": momentum_lookback,
                "rebalance": rebalance,
                "target_vol": target_vol,
                "vol_lookback": vol_lookback,
                "is_baseline": False,
            }
        )

    for value in _int_neighborhood(spec.momentum_lookback, step=21, minimum=21):
        if value != spec.momentum_lookback:
            add_variant(
                "momentum_lookback",
                momentum_lookback=value,
                rebalance=spec.rebalance,
                target_vol=spec.vol_target,
                vol_lookback=spec.vol_lookback,
            )
    for value in _int_neighborhood(spec.rebalance, step=7, minimum=7):
        if value != spec.rebalance:
            add_variant(
                "rebalance",
                momentum_lookback=spec.momentum_lookback,
                rebalance=value,
                target_vol=spec.vol_target,
                vol_lookback=spec.vol_lookback,
            )
    for value in _float_neighborhood(spec.vol_target, step=0.02, minimum=0.02):
        if abs(value - spec.vol_target) > 1e-12:
            add_variant(
                "target_vol",
                momentum_lookback=spec.momentum_lookback,
                rebalance=spec.rebalance,
                target_vol=value,
                vol_lookback=spec.vol_lookback,
            )
    for value in _int_neighborhood(spec.vol_lookback, step=max(5, spec.vol_lookback // 2), minimum=5):
        if value != spec.vol_lookback:
            add_variant(
                "vol_lookback",
                momentum_lookback=spec.momentum_lookback,
                rebalance=spec.rebalance,
                target_vol=spec.vol_target,
                vol_lookback=value,
            )

    return variants


def _cost_scenarios(base_costs: Mapping[str, float]) -> list[dict[str, Any]]:
    slippage = float(base_costs["slippage_bps"])
    commission_per_trade = float(base_costs["commission_per_trade"])
    commission_bps = float(base_costs["commission_bps"])
    return [
        {
            "scenario": "base",
            "slippage_bps": slippage,
            "commission_per_trade": commission_per_trade,
            "commission_bps": commission_bps,
        },
        {
            "scenario": "stress_mid",
            "slippage_bps": max(slippage * 2.0, slippage + 5.0),
            "commission_per_trade": max(commission_per_trade, 0.25),
            "commission_bps": max(commission_bps, 1.0),
        },
        {
            "scenario": "stress_high",
            "slippage_bps": max(slippage * 4.0, slippage + 15.0),
            "commission_per_trade": max(commission_per_trade, 1.0),
            "commission_bps": max(commission_bps, 2.0),
        },
    ]


def _run_candidate_core(
    *,
    spec: CandidateSpec,
    bars: pd.DataFrame,
    cost_assumptions: Mapping[str, float],
    rebalance_anchor_date: str | None,
    momentum_lookback: int | None = None,
    rebalance: int | None = None,
    target_vol: float | None = None,
    vol_lookback: int | None = None,
    include_template_outputs: bool = False,
) -> dict[str, Any]:
    strategy, runtime = _build_runtime(
        spec,
        momentum_lookback=momentum_lookback,
        rebalance=rebalance,
        target_vol=target_vol,
        vol_lookback=vol_lookback,
    )
    result = run_backtest(
        bars,
        strategy,  # type: ignore[arg-type]
        slippage_bps=float(cost_assumptions["slippage_bps"]),
        commission_per_trade=float(cost_assumptions["commission_per_trade"]),
        commission_bps=float(cost_assumptions["commission_bps"]),
        vol_target=runtime["engine_vol_target"],
        vol_lookback=int(runtime["engine_vol_lookback"]),
        vol_min=spec.vol_min,
        vol_max=spec.vol_max,
        vol_update=str(runtime["vol_update"]),
        rebalance_cadence=int(runtime["rebalance"]),
    )
    actions = run_backtest_script.build_dual_actions(
        bars,
        result.weights,
        vol_target=runtime["engine_vol_target"],
        vol_update=str(runtime["vol_update"]),
        rebalance=int(runtime["rebalance"]),
        allow_resize_without_vol_target=bool(runtime["allow_resize_without_vol_target"]),
    )
    metrics_summary = run_backtest_script.compute_extended_metrics(result)
    latest_realized_vol = _latest_series_value(result.realized_vol)
    latest_leverage = _latest_series_value(result.leverage)

    output: dict[str, Any] = {
        "result": result,
        "actions": actions,
        "metrics": metrics_summary,
        "runtime": runtime,
        "latest_realized_vol": latest_realized_vol,
        "latest_leverage": latest_leverage,
    }

    if not include_template_outputs:
        return output

    next_action = run_backtest_script.build_next_action_payload(
        strategy_label=spec.strategy_id,
        bars=bars,
        weights=result.weights,
        actions=actions,
        resize_rebalance=int(runtime["rebalance"]),
        next_rebalance=int(runtime["rebalance"]),
        rebalance_anchor_date=rebalance_anchor_date,
        vol_target=runtime["engine_vol_target"],
        vol_lookback=int(runtime["engine_vol_lookback"]),
        vol_update=str(runtime["vol_update"]),
        latest_realized_vol=latest_realized_vol,
        latest_leverage=latest_leverage,
        leverage_last_update_date=None,
        allow_resize_without_vol_target=bool(runtime["allow_resize_without_vol_target"]),
    )
    expected_symbol_count, actual_symbol_count = _expected_actual_symbol_count(
        bars,
        as_of_date=str(next_action.get("date")),
    )
    shadow_outputs = build_shadow_template_for_strategy(
        spec.strategy_id,
        template_family_id=spec.template_family_id,
        defensive_symbol=spec.defensive_symbol,
    ).build_outputs(
        bars=bars,
        weights=result.weights,
        turnover=result.turnover,
        equity=result.equity,
        next_action_payload=next_action,
        metrics_summary=metrics_summary,
        cost_assumptions=dict(cost_assumptions),
        actions=[dict(next_action)],
        expected_symbol_count=expected_symbol_count,
        actual_symbol_count=actual_symbol_count,
        leverage=latest_leverage,
        vol_target=float(runtime["reported_vol_target"]),
        realized_vol=latest_realized_vol,
    )
    output["next_action"] = next_action
    output["shadow_outputs"] = shadow_outputs
    return output


def _parameter_stability_rows(
    *,
    spec: CandidateSpec,
    bars: pd.DataFrame,
    cost_assumptions: Mapping[str, float],
    rebalance_anchor_date: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in _parameter_variants(spec):
        run = _run_candidate_core(
            spec=spec,
            bars=bars,
            cost_assumptions=cost_assumptions,
            rebalance_anchor_date=rebalance_anchor_date,
            momentum_lookback=int(variant["momentum_lookback"]),
            rebalance=int(variant["rebalance"]),
            target_vol=float(variant["target_vol"]),
            vol_lookback=int(variant["vol_lookback"]),
        )
        result = run["result"]
        actions = run["actions"]
        years = float(len(result.returns) / 252.0) if len(result.returns) else 0.0
        action_frequency = float(len(actions) / years) if years > 0 else 0.0
        rows.append(
            {
                "strategy_id": spec.strategy_id,
                "variant_label": variant["variant_label"],
                "parameter_name": variant["parameter_name"],
                "momentum_lookback": int(variant["momentum_lookback"]),
                "rebalance": int(variant["rebalance"]),
                "target_vol": float(variant["target_vol"]),
                "vol_lookback": int(variant["vol_lookback"]),
                "cagr": float(run["metrics"]["cagr"]),
                "sharpe": float(run["metrics"]["sharpe"]),
                "max_drawdown": float(run["metrics"]["max_drawdown"]),
                "turnover": float(run["metrics"]["annual_turnover"]),
                "action_frequency": action_frequency,
                "is_baseline": bool(variant["is_baseline"]),
            }
        )
    return rows


def _cost_sensitivity_rows(
    *,
    spec: CandidateSpec,
    bars: pd.DataFrame,
    base_cost_assumptions: Mapping[str, float],
    rebalance_anchor_date: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _cost_scenarios(base_cost_assumptions):
        run = _run_candidate_core(
            spec=spec,
            bars=bars,
            cost_assumptions=scenario,
            rebalance_anchor_date=rebalance_anchor_date,
        )
        rows.append(
            {
                "strategy_id": spec.strategy_id,
                "scenario": scenario["scenario"],
                "slippage_bps": float(scenario["slippage_bps"]),
                "commission_per_trade": float(scenario["commission_per_trade"]),
                "commission_bps": float(scenario["commission_bps"]),
                "cagr": float(run["metrics"]["cagr"]),
                "sharpe": float(run["metrics"]["sharpe"]),
                "max_drawdown": float(run["metrics"]["max_drawdown"]),
                "turnover": float(run["metrics"]["annual_turnover"]),
                "total_estimated_cost": float(run["metrics"]["total_estimated_cost"]),
            }
        )
    return rows


def _build_candidate(
    *,
    spec: CandidateSpec,
    bars: pd.DataFrame,
    benchmark_returns: pd.Series,
    cost_assumptions: Mapping[str, float],
    rebalance_anchor_date: str | None,
    report_dir: Path,
) -> Stage2CompareCandidate:
    baseline_run = _run_candidate_core(
        spec=spec,
        bars=bars,
        cost_assumptions=cost_assumptions,
        rebalance_anchor_date=rebalance_anchor_date,
        include_template_outputs=True,
    )
    template_output = baseline_run["shadow_outputs"].as_dict()
    candidate_output_json = _write_candidate_output_json(
        candidate_outputs_dir=report_dir / "candidate_outputs",
        spec=spec,
        template_output=template_output,
    )
    review_paths = write_shadow_review_artifacts(
        base_dir=report_dir / "candidate_reviews",
        bundle=baseline_run["shadow_outputs"].reports["shadow_review_bundle"],
    )
    return Stage2CompareCandidate(
        strategy_id=spec.strategy_id,
        role=spec.role,
        runtime_strategy=spec.runtime_strategy,
        implementation_label=spec.implementation_label,
        parameters=spec.parameter_payload(),
        result=baseline_run["result"],
        actions=baseline_run["actions"],
        next_action=baseline_run["next_action"],
        metrics=baseline_run["metrics"],
        benchmark_returns=benchmark_returns,
        review_bundle=baseline_run["shadow_outputs"].reports["shadow_review_bundle"],
        parameter_stability_rows=_parameter_stability_rows(
            spec=spec,
            bars=bars,
            cost_assumptions=cost_assumptions,
            rebalance_anchor_date=rebalance_anchor_date,
        ),
        cost_sensitivity_rows=_cost_sensitivity_rows(
            spec=spec,
            bars=bars,
            base_cost_assumptions=cost_assumptions,
            rebalance_anchor_date=rebalance_anchor_date,
        ),
        artifacts={
            "template_output_json": str(candidate_output_json),
            "review_json": str(review_paths.json_path),
            "review_markdown": str(review_paths.markdown_path),
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stage 2 shadow-only comparison/reporting flow for the approved primary "
            "candidate versus one explicitly configured local-only shadow target."
        ),
        epilog=(
            "Example:\n"
            "  ./.venv/bin/python scripts/stage2_shadow_compare.py "
            "--data-dir ./data "
            "--artifacts-dir ./artifacts/stage2_shadow_compare "
            "--pair-id primary_live_candidate_v1_vs_primary_live_candidate_v1_vol_managed"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--pair-id",
        default=DEFAULT_PAIR_ID,
        help=f"Explicit control-plane pair/target id for retained artifacts (default: {DEFAULT_PAIR_ID}).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=_default_data_dir(),
        help="Directory containing cached parquet bars (default: repo data/).",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=None,
        help="Optional base directory for deterministic comparison artifacts. Defaults to archive_root/stage2_shadow_compare/<pair_id>.",
    )
    parser.add_argument("--start", default=None, help="Optional inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=None, help="Optional inclusive end date (YYYY-MM-DD).")
    parser.add_argument(
        "--config",
        type=Path,
        default=REPO_ROOT / "config.toml",
        help="Optional TOML config path for rebalance anchor lookup (default: repo config.toml).",
    )
    parser.add_argument(
        "--rebalance-anchor-date",
        default=None,
        help="Optional YYYY-MM-DD anchor for trading-day next_rebalance schedules.",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=DEFAULT_COST_ASSUMPTIONS["slippage_bps"],
        help="Slippage in basis points per unit turnover (default: 5.0).",
    )
    parser.add_argument(
        "--commission-per-trade",
        type=float,
        default=DEFAULT_COST_ASSUMPTIONS["commission_per_trade"],
        help="Fixed commission per changed sleeve/order (default: 0.0).",
    )
    parser.add_argument(
        "--commission-bps",
        type=float,
        default=DEFAULT_COST_ASSUMPTIONS["commission_bps"],
        help="Legacy commission in basis points per unit turnover (default: 0.0).",
    )
    parser.add_argument(
        "--shadow-strategy-family",
        default=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID,
        help=(
            "Supported shadow runtime family id. "
            f"Default: {PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID}"
        ),
    )
    parser.add_argument(
        "--shadow-strategy-id",
        default=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
        help=f"Explicit shadow strategy id for the configured target. Default: {PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID}",
    )
    parser.add_argument(
        "--shadow-risk-symbols",
        default=None,
        help="Optional comma-separated shadow risk symbol override.",
    )
    parser.add_argument(
        "--shadow-defensive-symbol",
        default=None,
        help="Optional shadow defensive symbol override.",
    )
    parser.add_argument(
        "--shadow-momentum-lookback",
        type=int,
        default=None,
        help="Optional shadow momentum lookback override.",
    )
    parser.add_argument(
        "--shadow-top-n",
        type=int,
        default=None,
        help="Optional shadow top_n override.",
    )
    parser.add_argument(
        "--shadow-rebalance",
        type=int,
        default=None,
        help="Optional shadow rebalance override.",
    )
    parser.add_argument(
        "--shadow-vol-target",
        type=float,
        default=None,
        help="Optional shadow vol target override.",
    )
    parser.add_argument(
        "--shadow-vol-lookback",
        type=int,
        default=None,
        help="Optional shadow vol lookback override.",
    )
    parser.add_argument(
        "--shadow-vol-min",
        type=float,
        default=None,
        help="Optional shadow vol floor override.",
    )
    parser.add_argument(
        "--shadow-vol-max",
        type=float,
        default=None,
        help="Optional shadow vol cap override.",
    )
    parser.add_argument(
        "--shadow-vol-update",
        default=None,
        help="Optional shadow vol update mode override.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.pair_id = str(args.pair_id).strip()
    if not args.pair_id:
        print("[stage2_shadow_compare] ERROR: --pair-id must not be empty.", file=sys.stderr)
        return 2
    if args.artifacts_dir is None:
        args.artifacts_dir = _default_artifacts_dir(args.pair_id)
    if args.config is not None:
        cfg = run_backtest_script.load_run_backtest_config(args.config)
        if args.rebalance_anchor_date is None and cfg.rebalance_anchor_date is not None:
            args.rebalance_anchor_date = cfg.rebalance_anchor_date

    primary_spec = _primary_spec()
    try:
        shadow_spec = _shadow_spec(args)
    except ValueError as exc:
        print(f"[stage2_shadow_compare] ERROR: {exc}", file=sys.stderr)
        return 2
    symbols_to_load = list(
        dict.fromkeys(
            list(primary_spec.risk_symbols)
            + [primary_spec.defensive_symbol]
            + list(shadow_spec.risk_symbols)
            + [shadow_spec.defensive_symbol]
        )
    )
    cost_assumptions = {
        "slippage_bps": float(args.slippage_bps),
        "commission_per_trade": float(args.commission_per_trade),
        "commission_bps": float(args.commission_bps),
    }

    store = LocalStore(base_dir=args.data_dir.expanduser())
    bars = run_backtest_script.load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
    benchmark_returns = _benchmark_returns(store=store, bars=bars)
    as_of_date = pd.Timestamp(bars.index[-1]).date().isoformat()
    report_dir = args.artifacts_dir.expanduser() / as_of_date

    primary_candidate = _build_candidate(
        spec=primary_spec,
        bars=bars,
        benchmark_returns=benchmark_returns,
        cost_assumptions=cost_assumptions,
        rebalance_anchor_date=args.rebalance_anchor_date,
        report_dir=report_dir,
    )
    shadow_candidate = _build_candidate(
        spec=shadow_spec,
        bars=bars,
        benchmark_returns=benchmark_returns,
        cost_assumptions=cost_assumptions,
        rebalance_anchor_date=args.rebalance_anchor_date,
        report_dir=report_dir,
    )

    primary_mapping = primary_live_candidate_v1_runtime_mapping()
    shadow_runtime = resolve_shadow_runtime_config(
        args.shadow_strategy_family,
        strategy_id=args.shadow_strategy_id,
        symbols=_parse_symbol_list(args.shadow_risk_symbols),
        defensive_symbol=args.shadow_defensive_symbol,
        momentum_lookback=args.shadow_momentum_lookback,
        top_n=args.shadow_top_n,
        rebalance=args.shadow_rebalance,
        vol_target=args.shadow_vol_target,
        vol_lookback=args.shadow_vol_lookback,
        vol_min=args.shadow_vol_min,
        vol_max=args.shadow_vol_max,
        vol_update=args.shadow_vol_update,
    )
    report = build_stage2_shadow_compare_report(
        pair_id=args.pair_id,
        as_of_date=as_of_date,
        generated_at=pd.Timestamp.now().isoformat(),
        command=shlex.join([sys.executable, *sys.argv]),
        data_dir=str(args.data_dir.expanduser()),
        primary=primary_candidate,
        shadow=shadow_candidate,
        primary_mapping={
            "strategy_id": primary_mapping.strategy_id,
            "runtime_strategy": primary_mapping.runtime_strategy,
            "default_preset": primary_mapping.default_preset,
            "default_state_key": primary_mapping.default_state_key,
        },
        shadow_runtime={
            "strategy_id": shadow_runtime.strategy_id,
            "primary_candidate_strategy_id": shadow_runtime.primary_candidate_mapping.strategy_id,
            "implementation_strategy": shadow_runtime.implementation_strategy,
            "implementation_label": shadow_runtime.implementation_label,
            "risk_symbols": list(shadow_runtime.risk_symbols),
            "defensive_symbol": shadow_runtime.defensive_symbol,
            "momentum_lookback": shadow_runtime.momentum_lookback,
            "rebalance": shadow_runtime.rebalance,
            "vol_target": shadow_runtime.vol_target,
            "vol_lookback": shadow_runtime.vol_lookback,
            "vol_min": shadow_runtime.vol_min,
            "vol_max": shadow_runtime.vol_max,
            "vol_update": shadow_runtime.vol_update,
        },
    )
    report_paths = write_stage2_shadow_compare_artifacts(report_dir=report_dir, report=report)
    summary = {
        "pair_id": args.pair_id,
        "as_of_date": as_of_date,
        "current_decision": report["current_decision"],
        **report_paths,
        "primary_output_json": primary_candidate.artifacts["template_output_json"],
        "primary_review_markdown": primary_candidate.artifacts["review_markdown"],
        "shadow_output_json": shadow_candidate.artifacts["template_output_json"],
        "shadow_review_markdown": shadow_candidate.artifacts["review_markdown"],
    }
    print(json.dumps(summary, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
