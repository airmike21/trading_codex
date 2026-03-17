"""Helpers for local-only shadow review artifacts."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

# Data is considered stale when the as_of_date is more than this many calendar
# days behind today.  5 days covers a long weekend plus one trading day of lag.
_STALE_CALENDAR_DAYS = 5


def _compute_stale_data_warning(as_of_date: str) -> bool:
    """Return True when as_of_date is more than _STALE_CALENDAR_DAYS old."""
    # NOTE: staleness is measured against local wall-clock time (pd.Timestamp.now()).
    delta = pd.Timestamp.now().normalize() - pd.Timestamp(as_of_date).normalize()
    return delta.days > _STALE_CALENDAR_DAYS


def _compute_missing_price_warning(actions: list[dict[str, Any]]) -> bool:
    """Return True when any non-CASH action is missing a usable price."""
    for item in actions:
        symbol = item.get("symbol")
        action = item.get("action")
        price = item.get("price")
        if symbol is None or str(symbol).upper() == "CASH":
            continue
        if action is not None and str(action).upper() == "HOLD" and price is None:
            # HOLD with no price is acceptable (held position priced separately)
            continue
        if price is None:
            return True
        try:
            if math.isnan(float(price)):
                return True
        except (TypeError, ValueError):
            return True
    return False


def _compute_symbol_count_mismatch_warning(
    expected_symbol_count: int | None,
    actual_symbol_count: int | None,
) -> bool:
    """Return True when the loaded symbol count differs from the expected count.

    expected_symbol_count: number of distinct symbols configured for the strategy
        (derived from the bars panel column headers at the call site).
    actual_symbol_count: number of those symbols that have valid close-price data
        on the as_of_date row.
    """
    if expected_symbol_count is None or actual_symbol_count is None:
        return False
    return actual_symbol_count != expected_symbol_count


@dataclass(frozen=True)
class ShadowArtifactPaths:
    base_dir: Path
    plans_dir: Path
    reviews_dir: Path
    json_path: Path
    markdown_path: Path


def _safe_label(value: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return collapsed.strip("_") or "shadow_review"


def build_shadow_artifact_paths(
    base_dir: Path,
    *,
    as_of_date: str,
    strategy: str,
    action: str | None,
    symbol: str | None,
) -> ShadowArtifactPaths:
    day_slug = pd.Timestamp(as_of_date).date().isoformat()
    plans_dir = base_dir / "plans" / day_slug
    reviews_dir = base_dir / "reviews" / day_slug
    for path in (plans_dir, reviews_dir):
        path.mkdir(parents=True, exist_ok=True)

    label_parts = [strategy, action or "hold", symbol or "cash", "shadow_review"]
    filename = _safe_label("__".join(label_parts))
    return ShadowArtifactPaths(
        base_dir=base_dir,
        plans_dir=plans_dir,
        reviews_dir=reviews_dir,
        json_path=plans_dir / f"{filename}.json",
        markdown_path=reviews_dir / f"{filename}.md",
    )


def build_shadow_review_bundle(
    *,
    strategy: str,
    as_of_date: str,
    next_rebalance: str | None,
    actions: list[dict[str, Any]],
    cost_assumptions: dict[str, float],
    metrics: dict[str, float],
    leverage: float | None = None,
    vol_target: float | None = None,
    realized_vol: float | None = None,
    warnings: list[str] | None = None,
    blockers: list[str] | None = None,
    expected_symbol_count: int | None = None,
    actual_symbol_count: int | None = None,
) -> dict[str, Any]:
    warnings_list = list(warnings or [])
    blockers_list = list(blockers or [])
    action_types = [str(item.get("action", "")) for item in actions if item.get("action") is not None]
    symbols = [str(item.get("symbol", "")) for item in actions if item.get("symbol") is not None]

    stale_data_warning = _compute_stale_data_warning(as_of_date)
    missing_price_warning = _compute_missing_price_warning(actions)
    symbol_count_mismatch_warning = _compute_symbol_count_mismatch_warning(
        expected_symbol_count, actual_symbol_count
    )
    ready_for_shadow_review = not (
        stale_data_warning or missing_price_warning or symbol_count_mismatch_warning
    )

    # Derive reason lists from the computed booleans (never re-run independent logic).
    warning_reasons: list[str] = []
    blocking_reasons: list[str] = []
    if stale_data_warning:
        warning_reasons.append("stale_data")
    if missing_price_warning:
        blocking_reasons.append("missing_price")
    if symbol_count_mismatch_warning:
        blocking_reasons.append("symbol_count_mismatch")

    return {
        "artifact_type": "shadow_review",
        "artifact_version": 1,
        "strategy": strategy,
        # Keep bundle content deterministic from the signal date for easy local diffs/review.
        "generated_at": pd.Timestamp(as_of_date).isoformat(),
        "as_of_date": as_of_date,
        "next_rebalance": next_rebalance,
        "shadow_status": "review",
        "actions": actions,
        "action_types": action_types,
        "symbols": symbols,
        "cost_assumptions": dict(cost_assumptions),
        "metrics": dict(metrics),
        "rebalance_event_count": int(metrics.get("rebalance_event_count", 0.0)),
        "commission_trade_count": int(metrics.get("commission_trade_count", 0.0)),
        "leverage": leverage,
        "vol_target": vol_target,
        "realized_vol": realized_vol,
        "warnings": warnings_list,
        "blockers": blockers_list,
        "stale_data_warning": stale_data_warning,
        "missing_price_warning": missing_price_warning,
        "symbol_count_mismatch_warning": symbol_count_mismatch_warning,
        "ready_for_shadow_review": ready_for_shadow_review,
        "warning_reasons": warning_reasons,
        "blocking_reasons": blocking_reasons,
    }


def render_shadow_review_markdown(bundle: dict[str, Any]) -> str:
    actions = bundle.get("actions", [])
    action_types = ", ".join(dict.fromkeys(str(item) for item in bundle.get("action_types", []) if item)) or "-"
    symbols = ", ".join(dict.fromkeys(str(item) for item in bundle.get("symbols", []) if item)) or "-"
    metrics = bundle.get("metrics", {})
    cost_assumptions = bundle.get("cost_assumptions", {})
    warnings = bundle.get("warnings", [])
    blockers = bundle.get("blockers", [])

    warning_reasons = bundle.get("warning_reasons") or []
    blocking_reasons = bundle.get("blocking_reasons") or []
    ready_for_shadow_review = str(bool(bundle.get("ready_for_shadow_review"))).lower()
    stale_data_warning = str(bool(bundle.get("stale_data_warning"))).lower()
    missing_price_warning = str(bool(bundle.get("missing_price_warning"))).lower()
    symbol_count_mismatch_warning = str(bool(bundle.get("symbol_count_mismatch_warning"))).lower()

    lines = [
        f"# Shadow Review {bundle.get('strategy', '-')}",
        "",
        f"- Artifact version: `{bundle.get('artifact_version', 1)}`",
        f"- Shadow status: `{bundle.get('shadow_status', '-')}`",
        f"- Strategy: `{bundle.get('strategy', '-')}`",
        f"- As-of date: `{bundle.get('as_of_date', '-')}`",
        f"- Next rebalance: `{bundle.get('next_rebalance') or '-'}`",
        f"- Number of actions: `{len(actions)}`",
        f"- Symbols: `{symbols}`",
        f"- Action types: `{action_types}`",
        (
            "- Cost assumptions: "
            f"`slippage_bps={float(cost_assumptions.get('slippage_bps', 0.0)):.1f}, "
            f"commission_per_trade={float(cost_assumptions.get('commission_per_trade', 0.0)):.2f}, "
            f"commission_bps={float(cost_assumptions.get('commission_bps', 0.0)):.1f}`"
        ),
        f"- Gross CAGR: `{metrics.get('gross_cagr', '-')}`",
        f"- Net CAGR: `{metrics.get('net_cagr', '-')}`",
        f"- Gross Sharpe: `{metrics.get('gross_sharpe', '-')}`",
        f"- Net Sharpe: `{metrics.get('net_sharpe', '-')}`",
        f"- Rebalance-event count: `{bundle.get('rebalance_event_count', 0)}`",
        f"- Commission-counted sleeve/order count: `{bundle.get('commission_trade_count', 0)}`",
        f"- Warnings: `{', '.join(str(item) for item in warnings) if warnings else '-'}`",
        f"- Blockers: `{', '.join(str(item) for item in blockers) if blockers else '-'}`",
        f"- Warning reasons: `{', '.join(warning_reasons) if warning_reasons else '-'}`",
        f"- Blocking reasons: `{', '.join(blocking_reasons) if blocking_reasons else '-'}`",
        f"- Ready for shadow review: `{ready_for_shadow_review}`",
        f"- Stale data warning: `{stale_data_warning}`",
        f"- Missing price warning: `{missing_price_warning}`",
        f"- Symbol count mismatch warning: `{symbol_count_mismatch_warning}`",
        "",
    ]

    if warning_reasons:
        lines += ["## Warnings", ""]
        for reason in warning_reasons:
            lines.append(f"- {reason}")
        lines.append("")

    if blocking_reasons:
        lines += ["## Blockers", ""]
        for reason in blocking_reasons:
            lines.append(f"- {reason}")
        lines.append("")

    lines += [
        "## Actions",
        "",
    ]

    if not actions:
        lines.append("- None")
        return "\n".join(lines) + "\n"

    for action in actions:
        action_type = str(action.get("action", "-"))
        symbol = str(action.get("symbol", "-"))
        target_shares = action.get("target_shares", "-")
        event_id = action.get("event_id", "-")
        lines.append(
            f"- `{action_type} {symbol} target_shares={target_shares} event_id={event_id}`"
        )
    return "\n".join(lines) + "\n"


def write_shadow_review_artifacts(
    *,
    base_dir: Path,
    bundle: dict[str, Any],
) -> ShadowArtifactPaths:
    actions = bundle.get("actions", [])
    first_action = actions[0] if actions else {}
    paths = build_shadow_artifact_paths(
        base_dir,
        as_of_date=str(bundle.get("as_of_date")),
        strategy=str(bundle.get("strategy")),
        action=None if not first_action else str(first_action.get("action") or ""),
        symbol=None if not first_action else str(first_action.get("symbol") or ""),
    )
    paths.json_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths.markdown_path.write_text(render_shadow_review_markdown(bundle), encoding="utf-8")
    return paths
