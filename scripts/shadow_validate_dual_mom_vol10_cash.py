#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
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

from trading_codex.backtest.next_rebalance import compute_next_rebalance_date
from trading_codex.backtest.shadow_artifacts import (
    build_shadow_artifact_paths,
    build_shadow_review_bundle,
    write_shadow_review_artifacts,
)
from trading_codex.data import LocalStore
from trading_codex.run_archive import resolve_archive_root

DEFAULT_RISK_SYMBOLS = ("SPY", "QQQ", "IWM", "EFA")
DEFAULT_DEFENSIVE_SYMBOL = "BIL"
DEFAULT_COST_ASSUMPTIONS = {
    "slippage_bps": 5.0,
    "commission_per_trade": 0.0,
    "commission_bps": 0.0,
}


def _default_data_dir() -> Path:
    return Path.home() / "trading_codex" / "data"


def _default_shadow_artifacts_dir() -> Path:
    return resolve_archive_root(create=True) / "shadow_validations" / "dual_mom_vol10_cash"


def _today_iso() -> str:
    return pd.Timestamp.now().normalize().date().isoformat()


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    src_text = str(SRC_PATH)
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = f"{src_text}:{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = src_text
    return env


def _stale_calendar_days(as_of_date: str) -> int:
    return int(
        (pd.Timestamp.now().normalize() - pd.Timestamp(as_of_date).normalize()).days
    )


def _minimum_history_rows(
    *,
    momentum_lookback: int,
    vol_lookback: int,
    rebalance: int,
) -> int:
    if rebalance <= 0:
        raise ValueError("rebalance must be > 0.")

    warmup = max(int(momentum_lookback), int(vol_lookback))
    first_decision_index = int(rebalance) - 1
    if warmup > first_decision_index:
        increments = (warmup - first_decision_index + int(rebalance) - 1) // int(rebalance)
        first_decision_index += increments * int(rebalance)
    return first_decision_index + 2


def _load_symbol_frames(
    store: LocalStore,
    *,
    required_symbols: list[str],
    start: str | None,
    end: str | None,
) -> tuple[dict[str, pd.DataFrame], dict[str, int], dict[str, str]]:
    frames: dict[str, pd.DataFrame] = {}
    row_counts: dict[str, int] = {}
    latest_dates: dict[str, str] = {}

    for symbol in required_symbols:
        try:
            df = store.read_bars(symbol, start=start, end=end)
        except FileNotFoundError:
            continue
        if df.empty:
            continue
        frame = df.sort_index()
        frames[symbol] = frame
        row_counts[symbol] = int(len(frame))
        latest_dates[symbol] = pd.Timestamp(frame.index[-1]).date().isoformat()

    return frames, row_counts, latest_dates


def _build_overlap_bars(frames: dict[str, pd.DataFrame], ordered_symbols: list[str]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat({symbol: frames[symbol] for symbol in ordered_symbols}, axis=1, join="inner").sort_index()


def _actual_symbol_count_from_bars(bars: pd.DataFrame) -> int:
    if bars.empty:
        return 0
    if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
        return 0
    if "close" not in bars.columns.get_level_values(1):
        return 0
    close_panel = bars.xs("close", axis=1, level=1)
    return int(close_panel.iloc[-1].notna().sum())


def _as_of_date_for_validation(bars: pd.DataFrame, latest_dates: dict[str, str]) -> str:
    if not bars.empty:
        return pd.Timestamp(bars.index[-1]).date().isoformat()
    if latest_dates:
        return min(pd.Timestamp(value) for value in latest_dates.values()).date().isoformat()
    return _today_iso()


def _blocked_next_action_payload(
    *,
    as_of_date: str,
    rebalance: int,
    rebalance_anchor_date: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "schema_minor": 0,
        "schema_name": "next_action",
        "date": as_of_date,
        "strategy": "dual_mom_vol10_cash",
        "action": "HOLD",
        "symbol": "CASH",
        "price": None,
        "target_shares": 0,
        "resize_prev_shares": None,
        "resize_new_shares": None,
        "next_rebalance": compute_next_rebalance_date(
            pd.DatetimeIndex([]),
            pd.Timestamp(as_of_date),
            trading_days=rebalance,
            anchor_date=rebalance_anchor_date,
        ),
        "vol_target": None,
        "vol_lookback": None,
        "lookback": None,
        "vol_update": None,
        "realized_vol": None,
        "leverage": None,
        "leverage_update": None,
    }
    payload["event_id"] = run_backtest_script._next_action_event_id(payload)
    return payload


def _next_action_summary(payload: dict[str, Any]) -> str:
    action = str(payload.get("action", "-"))
    symbol = str(payload.get("symbol", "-"))
    target_shares = payload.get("target_shares")
    resize_prev_shares = payload.get("resize_prev_shares")
    resize_new_shares = payload.get("resize_new_shares")
    next_rebalance = payload.get("next_rebalance")

    parts = [f"{action} {symbol}"]
    if target_shares is not None:
        parts.append(f"target_shares={target_shares}")
    if resize_prev_shares is not None or resize_new_shares is not None:
        parts.append(
            "resize="
            f"{resize_prev_shares if resize_prev_shares is not None else '-'}->"
            f"{resize_new_shares if resize_new_shares is not None else '-'}"
        )
    if next_rebalance is not None:
        parts.append(f"next_rebalance={next_rebalance}")
    return "; ".join(parts)


def _shadow_summary(paths: Any, bundle: dict[str, Any]) -> dict[str, Any]:
    review_summary = bundle.get("review_summary") or {}
    return {
        "strategy": bundle.get("strategy"),
        "as_of_date": bundle.get("as_of_date"),
        "shadow_review_state": bundle.get("shadow_review_state"),
        "automation_decision": review_summary.get("automation_decision"),
        "automation_status": review_summary.get("automation_status"),
        "json_artifact": str(paths.json_path),
        "markdown_artifact": str(paths.markdown_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Shadow-only readiness validation for dual_mom_vol10_cash on cached ETF data."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=_default_data_dir(),
        help="Directory containing cached parquet bars (default: ~/trading_codex/data).",
    )
    parser.add_argument(
        "--shadow-artifacts-dir",
        type=Path,
        default=None,
        help="Optional base directory for deterministic shadow validation artifacts.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_RISK_SYMBOLS),
        help="Risk symbols for dual_mom_vol10_cash validation (default: SPY QQQ IWM EFA).",
    )
    parser.add_argument(
        "--dmv-defensive-symbol",
        default=DEFAULT_DEFENSIVE_SYMBOL,
        help="Defensive symbol for dual_mom_vol10_cash validation (default: BIL).",
    )
    parser.add_argument(
        "--dmv-mom-lookback",
        type=int,
        default=63,
        help="Momentum lookback in trading days (default: 63).",
    )
    parser.add_argument(
        "--dmv-rebalance",
        type=int,
        default=21,
        help="Fixed trading-day rebalance interval (default: 21).",
    )
    parser.add_argument(
        "--dmv-vol-lookback",
        type=int,
        default=20,
        help="Realized-vol lookback in trading days (default: 20).",
    )
    parser.add_argument(
        "--dmv-target-vol",
        type=float,
        default=0.10,
        help="Internal annualized target vol for the strategy (default: 0.10).",
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
    return parser


def _build_run_backtest_command(
    args: argparse.Namespace,
    *,
    shadow_artifacts_dir: Path,
    checklist_out: Path,
) -> list[str]:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "run_backtest.py"),
        "--strategy",
        "dual_mom_vol10_cash",
        "--symbols",
        *args.symbols,
        "--dmv-defensive-symbol",
        args.dmv_defensive_symbol,
        "--dmv-mom-lookback",
        str(args.dmv_mom_lookback),
        "--dmv-rebalance",
        str(args.dmv_rebalance),
        "--dmv-vol-lookback",
        str(args.dmv_vol_lookback),
        "--dmv-target-vol",
        str(args.dmv_target_vol),
        "--data-dir",
        str(args.data_dir),
        "--no-plot",
        "--next-action-json",
        "--shadow-artifacts-dir",
        str(shadow_artifacts_dir),
        "--checklist-out",
        str(checklist_out),
    ]
    if args.start:
        cmd.extend(["--start", args.start])
    if args.end:
        cmd.extend(["--end", args.end])
    if args.config:
        cmd.extend(["--config", str(args.config)])
    if args.rebalance_anchor_date:
        cmd.extend(["--rebalance-anchor-date", args.rebalance_anchor_date])
    return cmd


def _load_existing_shadow_bundle(
    *,
    shadow_artifacts_dir: Path,
    next_action_payload: dict[str, Any],
) -> dict[str, Any]:
    paths = build_shadow_artifact_paths(
        shadow_artifacts_dir,
        as_of_date=str(next_action_payload.get("date")),
        strategy="dual_mom_vol10_cash",
        action=str(next_action_payload.get("action") or ""),
        symbol=str(next_action_payload.get("symbol") or ""),
    )
    return json.loads(paths.json_path.read_text(encoding="utf-8"))


def _run_underlying_backtest(
    run_backtest_command: list[str],
    *,
    shadow_artifacts_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    proc = subprocess.run(
        run_backtest_command,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env=_subprocess_env(),
    )
    if proc.returncode != 0:
        detail = (proc.stderr or "") + (proc.stdout or "")
        raise RuntimeError(f"run_backtest failed ({proc.returncode}): {detail.strip()}")

    lines = proc.stdout.splitlines()
    if len(lines) != 1:
        raise RuntimeError(
            "run_backtest --next-action-json must emit exactly one line "
            f"(got {len(lines)} lines)."
        )

    payload = json.loads(lines[0])
    if not isinstance(payload, dict):
        raise RuntimeError("run_backtest --next-action-json returned a non-object payload.")

    bundle = _load_existing_shadow_bundle(
        shadow_artifacts_dir=shadow_artifacts_dir,
        next_action_payload=payload,
    )
    return payload, bundle


def validate_shadow_run(args: argparse.Namespace) -> tuple[dict[str, Any], Any]:
    shadow_artifacts_dir = (
        args.shadow_artifacts_dir.expanduser()
        if args.shadow_artifacts_dir is not None
        else _default_shadow_artifacts_dir()
    )
    data_dir = args.data_dir.expanduser()
    args.data_dir = data_dir
    args.symbols = _dedupe_symbols(list(args.symbols))
    args.dmv_defensive_symbol = _normalize_symbol(args.dmv_defensive_symbol)
    args.config = args.config.expanduser() if args.config is not None else None

    required_symbols = _dedupe_symbols(args.symbols + [args.dmv_defensive_symbol])
    expected_symbol_count = len(required_symbols)
    minimum_history_rows = _minimum_history_rows(
        momentum_lookback=args.dmv_mom_lookback,
        vol_lookback=args.dmv_vol_lookback,
        rebalance=args.dmv_rebalance,
    )

    frames: dict[str, pd.DataFrame] = {}
    row_counts: dict[str, int] = {}
    latest_dates: dict[str, str] = {}
    loaded_symbols: list[str] = []
    missing_symbols: list[str] = list(required_symbols)
    overlap_bars = pd.DataFrame()
    history_rows = 0
    actual_symbol_count = 0
    validated_with_cached_data = False

    warnings: list[str] = []
    blockers: list[str] = []
    extra_warning_reasons: list[str] = []
    extra_blocking_reasons: list[str] = []

    cost_assumptions = dict(DEFAULT_COST_ASSUMPTIONS)
    metrics_summary: dict[str, float] = {}

    shadow_artifacts_dir.mkdir(parents=True, exist_ok=True)
    checklist_out = shadow_artifacts_dir / "latest_dual_mom_vol10_cash_checklist.md"
    run_backtest_command = _build_run_backtest_command(
        args,
        shadow_artifacts_dir=shadow_artifacts_dir,
        checklist_out=checklist_out,
    )

    if not data_dir.exists() or not data_dir.is_dir():
        extra_blocking_reasons.extend(["no_cached_data", "missing_required_symbols"])
        blockers.append(f"Data directory not found: {data_dir}")
    else:
        store = LocalStore(base_dir=data_dir)
        frames, row_counts, latest_dates = _load_symbol_frames(
            store,
            required_symbols=required_symbols,
            start=args.start,
            end=args.end,
        )
        loaded_symbols = [symbol for symbol in required_symbols if symbol in frames]
        missing_symbols = [symbol for symbol in required_symbols if symbol not in frames]

        if not loaded_symbols:
            extra_blocking_reasons.extend(["no_cached_data", "missing_required_symbols"])
            blockers.append(
                "No cached bars available for required symbols in the requested date range."
            )
        else:
            overlap_bars = _build_overlap_bars(frames, loaded_symbols)
            history_rows = int(len(overlap_bars))
            actual_symbol_count = _actual_symbol_count_from_bars(overlap_bars)

            if missing_symbols:
                extra_blocking_reasons.append("missing_required_symbols")
                blockers.append(f"Missing required symbols: {', '.join(missing_symbols)}")

            if history_rows == 0:
                extra_blocking_reasons.append("no_overlapping_history")
                blockers.append("Loaded symbols do not share an overlapping history window.")
            elif history_rows < minimum_history_rows:
                extra_blocking_reasons.append("insufficient_history")
                blockers.append(
                    "Insufficient overlapping history: "
                    f"need >= {minimum_history_rows} rows, found {history_rows}."
                )

            if actual_symbol_count != expected_symbol_count:
                blockers.append(
                    "Symbol count mismatch on the validation as-of date: "
                    f"expected {expected_symbol_count}, found {actual_symbol_count}."
                )

    as_of_date = _as_of_date_for_validation(overlap_bars, latest_dates)
    stale_days = _stale_calendar_days(as_of_date)
    if stale_days > 5:
        warnings.append(
            f"Cached data is stale by {stale_days} calendar days (as_of_date={as_of_date})."
        )

    next_action_payload = _blocked_next_action_payload(
        as_of_date=as_of_date,
        rebalance=args.dmv_rebalance,
        rebalance_anchor_date=args.rebalance_anchor_date,
    )
    existing_bundle: dict[str, Any] = {}

    if not extra_blocking_reasons:
        try:
            next_action_payload, existing_bundle = _run_underlying_backtest(
                run_backtest_command,
                shadow_artifacts_dir=shadow_artifacts_dir,
            )
            validated_with_cached_data = True
            raw_metrics = existing_bundle.get("metrics")
            raw_costs = existing_bundle.get("cost_assumptions")
            if isinstance(raw_metrics, dict):
                metrics_summary = {
                    str(key): float(value)
                    for key, value in raw_metrics.items()
                    if isinstance(value, (int, float))
                }
            if isinstance(raw_costs, dict):
                cost_assumptions = {
                    str(key): float(value)
                    for key, value in raw_costs.items()
                    if isinstance(value, (int, float))
                }
        except Exception as exc:
            extra_blocking_reasons.append("validation_error")
            blockers.append(str(exc))

    bundle = build_shadow_review_bundle(
        strategy="dual_mom_vol10_cash",
        as_of_date=str(next_action_payload.get("date") or as_of_date),
        next_rebalance=(
            None
            if next_action_payload.get("next_rebalance") is None
            else str(next_action_payload.get("next_rebalance"))
        ),
        actions=[dict(next_action_payload)],
        cost_assumptions=cost_assumptions,
        metrics=metrics_summary,
        leverage=(
            float(existing_bundle["leverage"])
            if existing_bundle.get("leverage") is not None
            else None
        ),
        vol_target=(
            float(existing_bundle["vol_target"])
            if existing_bundle.get("vol_target") is not None
            else None
        ),
        realized_vol=(
            float(existing_bundle["realized_vol"])
            if existing_bundle.get("realized_vol") is not None
            else None
        ),
        warnings=warnings,
        blockers=blockers,
        expected_symbol_count=expected_symbol_count,
        actual_symbol_count=actual_symbol_count,
        extra_warning_reasons=extra_warning_reasons,
        extra_blocking_reasons=extra_blocking_reasons,
    )
    bundle.update(
        {
            "data_dir": str(data_dir),
            "required_symbols": required_symbols,
            "loaded_symbols": loaded_symbols,
            "missing_symbols": missing_symbols,
            "loaded_symbol_row_counts": row_counts,
            "loaded_symbol_latest_dates": latest_dates,
            "history_rows": history_rows,
            "minimum_history_rows": minimum_history_rows,
            "expected_symbol_count": expected_symbol_count,
            "actual_symbol_count": actual_symbol_count,
            "loaded_symbol_count": len(loaded_symbols),
            "validated_with_cached_data": validated_with_cached_data,
            "strategy_momentum_lookback": int(args.dmv_mom_lookback),
            "strategy_rebalance": int(args.dmv_rebalance),
            "strategy_vol_lookback": int(args.dmv_vol_lookback),
            "strategy_target_vol": float(args.dmv_target_vol),
            "command": shlex.join([sys.executable, *sys.argv]),
            "run_backtest_command": shlex.join(run_backtest_command),
            "next_action_summary": _next_action_summary(next_action_payload),
            "target_shares": next_action_payload.get("target_shares"),
            "resize_prev_shares": next_action_payload.get("resize_prev_shares"),
            "resize_new_shares": next_action_payload.get("resize_new_shares"),
            "automation_decision": bundle["review_summary"]["automation_decision"],
            "automation_status": bundle["review_summary"]["automation_status"],
        }
    )

    paths = write_shadow_review_artifacts(base_dir=shadow_artifacts_dir, bundle=bundle)
    return bundle, paths


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = run_backtest_script.load_run_backtest_config(args.config)
    if args.rebalance_anchor_date is None and cfg.rebalance_anchor_date is not None:
        args.rebalance_anchor_date = cfg.rebalance_anchor_date

    bundle, paths = validate_shadow_run(args)
    print(json.dumps(_shadow_summary(paths, bundle), separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
