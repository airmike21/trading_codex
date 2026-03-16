#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd

try:
    from scripts import daily_signal
    from scripts import run_backtest as rb_cli
except ImportError:  # pragma: no cover - direct script execution path.
    import daily_signal  # type: ignore[no-redef]
    import run_backtest as rb_cli  # type: ignore[no-redef]

from trading_codex.backtest.engine import BacktestResult, run_backtest
from trading_codex.data import LocalStore

DEFAULT_TARGET_VOLS = (0.08, 0.10, 0.12)
DEFAULT_VOL_LOOKBACKS = (21, 63, 126)
DEFAULT_MIN_LEVERAGE = 0.0
DEFAULT_MAX_LEVERAGE = 1.0
DEFAULT_RECENT_YEARS = 5
DEFAULT_PRESETS = ("vm_core", "dual_mom_core")


@dataclass(frozen=True)
class EvalContext:
    preset_name: str
    preset_description: str
    strategy_name: str
    args: argparse.Namespace
    bars: pd.DataFrame
    strategy: object
    rebalance_cadence: str | int


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_presets_path(repo_root: Path) -> Path:
    local = repo_root / "configs" / "presets.json"
    if local.exists():
        return local
    return repo_root / "configs" / "presets.example.json"


def _parse_run_backtest_args(args: list[str]) -> argparse.Namespace:
    expanded = daily_signal._expand_known_path_args(args)
    with patch.object(sys, "argv", ["run_backtest.py", *expanded]):
        return rb_cli.parse_args()


def _select_presets(
    presets: dict[str, daily_signal.Preset],
    requested: list[str] | None,
) -> list[str]:
    if requested:
        missing = [name for name in requested if name not in presets]
        if missing:
            known = ", ".join(sorted(presets))
            raise ValueError(f"Unknown preset(s): {', '.join(missing)}. Known: {known}")
        return requested

    chosen = [name for name in DEFAULT_PRESETS if name in presets]
    if chosen:
        return chosen

    fallback: list[str] = []
    seen_strategies: set[str] = set()
    for name, preset in presets.items():
        args = _parse_run_backtest_args(preset.run_backtest_args)
        if args.strategy in {"dual_mom", "valmom_v1"} and args.strategy not in seen_strategies:
            fallback.append(name)
            seen_strategies.add(str(args.strategy))
    return fallback


def _build_eval_context(name: str, preset: daily_signal.Preset) -> EvalContext:
    args = _parse_run_backtest_args(preset.run_backtest_args)
    store = LocalStore(base_dir=Path(str(args.data_dir)))

    if args.strategy == "dual_mom":
        defensive_symbol = rb_cli._normalize_defensive_symbol(args.defensive)
        gate_symbol = args.gate_symbol.strip()
        gate_symbols_to_load = [gate_symbol] if args.regime_gate == "sma200" and gate_symbol else []
        symbols_to_load = list(
            dict.fromkeys(args.symbols + ([defensive_symbol] if defensive_symbol else []) + gate_symbols_to_load)
        )
        bars = rb_cli.load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = rb_cli.DualMomentumStrategy(
            risk_universe=args.symbols,
            defensive=defensive_symbol,
            lookback=args.mom_lookback,
            rebalance=args.rebalance,
            regime_gate=args.regime_gate,
            gate_symbol=gate_symbol,
            gate_sma_window=args.gate_sma_window,
        )
        rebalance_cadence: str | int = args.rebalance
    elif args.strategy == "valmom_v1":
        risk_symbols = list(dict.fromkeys(args.symbols))
        defensive_symbol = args.vm_defensive_symbol.strip()
        if not defensive_symbol:
            raise ValueError(f"Preset {name!r} has empty --vm-defensive-symbol.")
        symbols_to_load = list(dict.fromkeys(risk_symbols + [defensive_symbol]))
        bars = rb_cli.load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = rb_cli.ValueMomentumV1Strategy(
            symbols=risk_symbols,
            mom_lookback=args.vm_mom_lookback,
            val_lookback=args.vm_val_lookback,
            top_n=args.vm_top_n,
            rebalance=args.vm_rebalance,
            defensive_symbol=defensive_symbol,
            mom_weight=args.vm_mom_weight,
            val_weight=args.vm_val_weight,
        )
        rebalance_cadence = args.vm_rebalance
    else:
        raise ValueError(
            f"Preset {name!r} uses unsupported strategy {args.strategy!r}. "
            "This evaluation pack currently supports dual_mom and valmom_v1."
        )

    return EvalContext(
        preset_name=name,
        preset_description=preset.description,
        strategy_name=str(args.strategy),
        args=args,
        bars=bars,
        strategy=strategy,
        rebalance_cadence=rebalance_cadence,
    )


def _recent_period_start(index: pd.DatetimeIndex, recent_years: int) -> pd.Timestamp:
    cutoff = index[-1] - pd.DateOffset(years=recent_years)
    pos = int(index.searchsorted(cutoff, side="left"))
    if pos >= len(index):
        pos = len(index) - 1
    return pd.Timestamp(index[pos])


def _slice_result(
    result: BacktestResult,
    start: pd.Timestamp,
) -> tuple[BacktestResult, pd.Series]:
    returns = result.returns.loc[start:]
    if isinstance(result.weights, pd.DataFrame):
        weights = result.weights.loc[returns.index]
    else:
        weights = result.weights.loc[returns.index]
    turnover = result.turnover.loc[returns.index]
    gross_returns = result.gross_returns.loc[returns.index] if result.gross_returns is not None else returns
    gross_equity = result.gross_equity.loc[returns.index] if result.gross_equity is not None else (1.0 + gross_returns).cumprod()
    cost_returns = result.cost_returns.loc[returns.index] if result.cost_returns is not None else (gross_returns - returns)
    estimated_costs = result.estimated_costs.loc[returns.index] if result.estimated_costs is not None else None
    trade_count = result.trade_count.loc[returns.index] if result.trade_count is not None else None
    leverage = (
        result.leverage.loc[returns.index]
        if result.leverage is not None
        else pd.Series(1.0, index=returns.index, dtype=float)
    )
    realized_vol = result.realized_vol.loc[returns.index] if result.realized_vol is not None else None
    sliced = BacktestResult(
        returns=returns,
        weights=weights,
        turnover=turnover,
        equity=(1.0 + returns).cumprod(),
        gross_returns=gross_returns,
        gross_equity=gross_equity,
        cost_returns=cost_returns,
        estimated_costs=estimated_costs,
        trade_count=trade_count,
        leverage=leverage,
        realized_vol=realized_vol,
    )
    return sliced, leverage


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return float(value)


def _evaluate_context(
    ctx: EvalContext,
    *,
    target_vol: float | None,
    vol_lookback: int | None,
    min_leverage: float,
    max_leverage: float,
    recent_years: int,
) -> list[dict[str, object]]:
    result = run_backtest(
        ctx.bars,
        ctx.strategy,  # type: ignore[arg-type]
        slippage_bps=ctx.args.slippage_bps,
        commission_bps=ctx.args.commission_bps,
        commission_per_trade=ctx.args.commission_per_trade,
        vol_target=target_vol,
        vol_lookback=int(vol_lookback) if vol_lookback is not None else ctx.args.vol_lookback,
        vol_min=min_leverage,
        vol_max=max_leverage,
        vol_update=ctx.args.vol_update,
        rebalance_cadence=ctx.rebalance_cadence,
        ivol=ctx.args.ivol,
        ivol_lookback=ctx.args.ivol_lookback,
        ivol_eps=ctx.args.ivol_eps,
    )

    periods = {
        "full": result.returns.index[0],
        f"recent_{recent_years}y": _recent_period_start(result.returns.index, recent_years),
    }
    config_label = (
        "baseline"
        if target_vol is None
        else f"tv_{target_vol:.2f}_lb_{int(vol_lookback or ctx.args.vol_lookback)}"
    )

    rows: list[dict[str, object]] = []
    for period_name, start in periods.items():
        sliced, leverage = _slice_result(result, start)
        extended = rb_cli.compute_extended_metrics(sliced)
        total_return = float((1.0 + sliced.returns).prod() - 1.0) if len(sliced.returns) else 0.0
        rows.append(
            {
                "preset": ctx.preset_name,
                "preset_description": ctx.preset_description,
                "strategy": ctx.strategy_name,
                "config_label": config_label,
                "overlay_enabled": target_vol is not None,
                "target_vol": _float_or_none(target_vol),
                "vol_lookback": int(vol_lookback) if target_vol is not None and vol_lookback is not None else None,
                "min_leverage": float(min_leverage) if target_vol is not None else None,
                "max_leverage": float(max_leverage) if target_vol is not None else None,
                "vol_update": ctx.args.vol_update if target_vol is not None else "baseline",
                "period": period_name,
                "period_start": sliced.returns.index[0].date().isoformat(),
                "period_end": sliced.returns.index[-1].date().isoformat(),
                "observations": int(len(sliced.returns)),
                "cagr": float(extended["cagr"]),
                "annualized_vol": float(extended["vol"]),
                "sharpe": float(extended["sharpe"]),
                "max_drawdown": float(extended["max_drawdown"]),
                "calmar": float(extended["calmar"]),
                "total_return": total_return,
                "average_leverage": float(leverage.mean()) if len(leverage) else 1.0,
                "min_leverage_observed": float(leverage.min()) if len(leverage) else 1.0,
                "max_leverage_observed": float(leverage.max()) if len(leverage) else 1.0,
                # Legacy alias retained for compatibility; matches turnover/rebalance days.
                "trade_count": int((sliced.turnover > 0).sum()),
                "rebalance_event_count": int(extended["rebalance_event_count"]),
                "commission_trade_count": int(extended["commission_trade_count"]),
                "trades_per_year": float(extended["trades_per_year"]),
                "rebalance_events_per_year": float(extended["rebalance_events_per_year"]),
                "commission_trade_count_per_year": float(extended["commission_trade_count_per_year"]),
            }
        )
    return rows


def _rank_strategy_configs(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby(
            ["preset", "strategy", "config_label", "overlay_enabled", "target_vol", "vol_lookback"],
            dropna=False,
        )[
            ["cagr", "annualized_vol", "sharpe", "max_drawdown", "calmar", "total_return", "average_leverage"]
        ]
        .mean()
        .reset_index()
    )
    agg["rank_sharpe"] = agg["sharpe"].rank(ascending=False, method="min")
    agg["rank_calmar"] = agg["calmar"].rank(ascending=False, method="min")
    agg["rank_cagr"] = agg["cagr"].rank(ascending=False, method="min")
    agg["rank_total_return"] = agg["total_return"].rank(ascending=False, method="min")
    agg["rank_drawdown"] = agg["max_drawdown"].rank(ascending=False, method="min")
    agg["mean_rank"] = agg[
        ["rank_sharpe", "rank_calmar", "rank_cagr", "rank_total_return", "rank_drawdown"]
    ].mean(axis=1)
    return agg.sort_values(
        by=["mean_rank", "sharpe", "calmar", "cagr", "max_drawdown"],
        ascending=[True, False, False, False, False],
    ).reset_index(drop=True)


def _compare_config_rows(df: pd.DataFrame, config_label: str) -> pd.DataFrame:
    columns = [
        "period",
        "cagr",
        "annualized_vol",
        "sharpe",
        "max_drawdown",
        "calmar",
        "total_return",
        "average_leverage",
        "min_leverage_observed",
        "max_leverage_observed",
        "trade_count",
        "rebalance_event_count",
        "commission_trade_count",
    ]
    subset = df[df["config_label"] == config_label].copy()
    return subset[columns].sort_values("period").reset_index(drop=True)


def _recommend_strategy(
    strategy_rows: pd.DataFrame,
    ranked: pd.DataFrame,
) -> tuple[str, pd.Series | None, pd.Series]:
    baseline = ranked[ranked["config_label"] == "baseline"].iloc[0]
    overlay_rows = ranked[ranked["overlay_enabled"]]
    if overlay_rows.empty:
        return "no", None, baseline

    best_overlay = overlay_rows.iloc[0]
    baseline_period = strategy_rows[strategy_rows["config_label"] == "baseline"].set_index("period")
    overlay_period = strategy_rows[strategy_rows["config_label"] == best_overlay["config_label"]].set_index("period")

    overlay_better = (
        float(best_overlay["mean_rank"]) < float(baseline["mean_rank"])
        and float(overlay_period.loc["full", "sharpe"]) > float(baseline_period.loc["full", "sharpe"])
        and float(overlay_period.iloc[-1]["sharpe"]) >= float(baseline_period.iloc[-1]["sharpe"])
        and float(overlay_period.loc["full", "max_drawdown"]) >= float(baseline_period.loc["full", "max_drawdown"])
        and float(overlay_period.loc["full", "calmar"]) >= float(baseline_period.loc["full", "calmar"])
    )
    return ("yes" if overlay_better else "no"), best_overlay, baseline


def _overall_overlay_candidate(ranked_by_strategy: list[pd.DataFrame]) -> pd.Series | None:
    rows: list[pd.DataFrame] = []
    for ranked in ranked_by_strategy:
        overlay_rows = ranked[ranked["overlay_enabled"]].copy()
        if overlay_rows.empty:
            continue
        rows.append(overlay_rows)
    if not rows:
        return None

    combined = pd.concat(rows, ignore_index=True)
    grouped = (
        combined.groupby(["target_vol", "vol_lookback"], dropna=False)["mean_rank"]
        .mean()
        .reset_index()
        .sort_values(["mean_rank", "target_vol", "vol_lookback"])
        .reset_index(drop=True)
    )
    return grouped.iloc[0]


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return f"{value:.{digits}f}"
    return str(value)


def _markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    headers = [col.replace("_", " ") for col in columns]
    rows = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for _, row in df.iterrows():
        rows.append("| " + " | ".join(_fmt(row[col]) for col in columns) + " |")
    return "\n".join(rows)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(_repo_root()))
    except ValueError:
        return str(path)


def _write_summary(
    summary_path: Path,
    csv_path: Path,
    results: pd.DataFrame,
    ranked_by_strategy: dict[str, pd.DataFrame],
    recommendations: dict[str, tuple[str, pd.Series | None, pd.Series]],
    overall_candidate: pd.Series | None,
    recent_years: int,
) -> None:
    lines = [
        "# Volatility Overlay Evaluation Pack",
        "",
        f"- CSV: `{_display_path(csv_path)}`",
        f"- Periods: full history and trailing {recent_years} years from the full-history run.",
        f"- Grid: target_vol in {list(DEFAULT_TARGET_VOLS)}, vol_lookback in {list(DEFAULT_VOL_LOOKBACKS)}, min_leverage=0.0, max_leverage=1.0.",
        "- Baseline rows use a scalar leverage of 1.0 because the overlay is disabled.",
        "",
        "## Best Configuration By Strategy",
        "",
    ]

    for strategy, ranked in ranked_by_strategy.items():
        rec, best_overlay, baseline = recommendations[strategy]
        strategy_rows = results[results["strategy"] == strategy].copy()
        lines.append(f"### {strategy}")
        lines.append("")
        top_cols = ["config_label", "target_vol", "vol_lookback", "mean_rank", "sharpe", "calmar", "cagr", "max_drawdown"]
        lines.append(_markdown_table(ranked.head(5), top_cols))
        lines.append("")
        lines.append("Baseline vs best overlay by period:")
        lines.append("")
        compare_frames = [_compare_config_rows(strategy_rows, "baseline")]
        if best_overlay is not None:
            overlay_label = str(best_overlay["config_label"])
            compare = _compare_config_rows(strategy_rows, overlay_label).copy()
            compare.insert(0, "config_label", overlay_label)
            base = compare_frames[0].copy()
            base.insert(0, "config_label", "baseline")
            lines.append(_markdown_table(pd.concat([base, compare], ignore_index=True), ["config_label", "period", "cagr", "annualized_vol", "sharpe", "max_drawdown", "calmar", "total_return", "average_leverage", "rebalance_event_count", "commission_trade_count"]))
        else:
            base = compare_frames[0].copy()
            base.insert(0, "config_label", "baseline")
            lines.append(_markdown_table(base, ["config_label", "period", "cagr", "annualized_vol", "sharpe", "max_drawdown", "calmar", "total_return", "average_leverage", "rebalance_event_count", "commission_trade_count"]))
        lines.append("")
        if best_overlay is not None:
            lines.append(
                f"Recommendation for default overlay: **{rec.upper()}**. "
                f"Best overlay candidate is `{best_overlay['config_label']}` against baseline `baseline`."
            )
        else:
            lines.append("Recommendation for default overlay: **NO**. No overlay configurations were evaluated.")
        lines.append("")

    lines.append("## Overall Recommendation")
    lines.append("")
    dual_rec = recommendations.get("dual_mom", ("no", None, pd.Series(dtype=object)))[0]
    vm_rec = recommendations.get("valmom_v1", ("no", None, pd.Series(dtype=object)))[0]
    lines.append(f"- overlay default for dual_mom: **{dual_rec}**")
    lines.append(f"- overlay default for valmom_v1: **{vm_rec}**")
    if overall_candidate is not None:
        lines.append(
            f"- single recommended default parameter set: `target_vol={_fmt(overall_candidate['target_vol'], 2)}`, "
            f"`vol_lookback={int(overall_candidate['vol_lookback'])}`"
        )
    else:
        lines.append("- single recommended default parameter set: none")
    lines.append("")
    lines.append("## Preset Naming Proposal")
    lines.append("")
    if dual_rec == "yes" or vm_rec == "yes":
        lines.append("- Keep the existing preset name where the overlay becomes the default.")
        lines.append("- Add an explicit raw/no-overlay opt-out preset if needed, for example `vm_core_raw` or `dual_mom_core_raw`.")
    else:
        lines.append("- Keep current default preset names unchanged.")
        lines.append("- If you want an opt-in pilot, use trial names like `vm_core_vt` and `dual_mom_core_vt`.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `vm_core_due` was not evaluated separately because it shares the same run_backtest args as `vm_core`; due mode affects alerting, not backtest returns.")
    lines.append("- Recommendations are conservative: the overlay is only a default candidate when the best overlay beats baseline on average rank and does not weaken the full-history drawdown/Calmar profile.")

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate volatility-target overlay settings for production presets.")
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Defaults to configs/presets.json when present.",
    )
    parser.add_argument(
        "--preset",
        action="append",
        default=None,
        help="Preset name to evaluate. Repeat to evaluate multiple presets.",
    )
    parser.add_argument(
        "--target-vols",
        type=float,
        nargs="+",
        default=list(DEFAULT_TARGET_VOLS),
        help="Overlay target vols to evaluate (default: 0.08 0.10 0.12).",
    )
    parser.add_argument(
        "--vol-lookbacks",
        type=int,
        nargs="+",
        default=list(DEFAULT_VOL_LOOKBACKS),
        help="Vol lookbacks to evaluate (default: 21 63 126).",
    )
    parser.add_argument(
        "--recent-years",
        type=int,
        default=DEFAULT_RECENT_YEARS,
        help="Trailing years slice to include alongside full history (default: 5).",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=Path("docs/analysis/vol_overlay_evaluation.csv"),
        help="CSV output path (default: docs/analysis/vol_overlay_evaluation.csv).",
    )
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=Path("docs/analysis/vol_overlay_evaluation.md"),
        help="Markdown summary output path (default: docs/analysis/vol_overlay_evaluation.md).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = _repo_root()
    presets_path = args.presets_file or _default_presets_path(repo_root)
    if not presets_path.exists():
        print(f"[evaluate_vol_overlay] ERROR: presets file not found: {presets_path}", file=sys.stderr)
        return 2

    presets = daily_signal._load_presets_json(presets_path)
    selected = _select_presets(presets, args.preset)
    if not selected:
        print("[evaluate_vol_overlay] ERROR: no supported presets selected.", file=sys.stderr)
        return 2

    contexts = [_build_eval_context(name, presets[name]) for name in selected]
    rows: list[dict[str, object]] = []
    for ctx in contexts:
        rows.extend(
            _evaluate_context(
                ctx,
                target_vol=None,
                vol_lookback=None,
                min_leverage=DEFAULT_MIN_LEVERAGE,
                max_leverage=DEFAULT_MAX_LEVERAGE,
                recent_years=args.recent_years,
            )
        )
        for target_vol in args.target_vols:
            for vol_lookback in args.vol_lookbacks:
                rows.extend(
                    _evaluate_context(
                        ctx,
                        target_vol=float(target_vol),
                        vol_lookback=int(vol_lookback),
                        min_leverage=DEFAULT_MIN_LEVERAGE,
                        max_leverage=DEFAULT_MAX_LEVERAGE,
                        recent_years=args.recent_years,
                    )
                )

    results = pd.DataFrame(rows).sort_values(
        by=["strategy", "overlay_enabled", "target_vol", "vol_lookback", "period"],
        na_position="first",
    ).reset_index(drop=True)

    args.csv_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(args.csv_out, index=False)

    ranked_by_strategy: dict[str, pd.DataFrame] = {}
    recommendations: dict[str, tuple[str, pd.Series | None, pd.Series]] = {}
    for strategy, group in results.groupby("strategy", sort=True):
        ranked = _rank_strategy_configs(group)
        ranked_by_strategy[strategy] = ranked
        recommendations[strategy] = _recommend_strategy(group, ranked)

    overall_candidate = _overall_overlay_candidate(list(ranked_by_strategy.values()))
    _write_summary(
        args.summary_out,
        args.csv_out,
        results,
        ranked_by_strategy,
        recommendations,
        overall_candidate,
        recent_years=args.recent_years,
    )

    print(f"CSV={args.csv_out}")
    print(f"SUMMARY={args.summary_out}")
    for strategy, (decision, best_overlay, _) in recommendations.items():
        if best_overlay is None:
            print(f"{strategy}: decision={decision} best=baseline")
            continue
        print(
            f"{strategy}: decision={decision} "
            f"best={best_overlay['config_label']} "
            f"mean_rank={float(best_overlay['mean_rank']):.2f}"
        )
    if overall_candidate is not None:
        print(
            "OVERALL="
            f"target_vol={_fmt(overall_candidate['target_vol'], 2)} "
            f"vol_lookback={int(overall_candidate['vol_lookback'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
