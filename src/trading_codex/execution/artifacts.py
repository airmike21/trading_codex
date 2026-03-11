from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.execution.models import ExecutionPlan, LiveSubmissionExport, OrderIntentExport, SimulatedSubmissionExport
from trading_codex.execution.planner import (
    build_live_submission_preview,
    execution_plan_to_dict,
    order_intent_export_to_dict,
    plan_sha256_for_preview,
    simulated_submission_export_to_dict,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


@dataclass(frozen=True)
class ArtifactPaths:
    base_dir: Path
    logs_dir: Path
    plans_dir: Path
    reviews_dir: Path
    csv_log_path: Path
    json_path: Path
    markdown_path: Path


def resolve_timestamp(value: str | None) -> datetime:
    chicago = ZoneInfo("America/Chicago") if ZoneInfo is not None else None
    if value:
        dt = datetime.fromisoformat(value)
        if chicago is not None:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=chicago)
            return dt.astimezone(chicago)
        return dt
    if chicago is not None:
        return datetime.now(chicago).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _timestamp_slug(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S%z")


def _safe_label(value: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return collapsed.strip("_") or "execution_plan"


def build_artifact_paths(base_dir: Path, *, timestamp: datetime, source_label: str) -> ArtifactPaths:
    day_slug = timestamp.date().isoformat()
    stamp = _timestamp_slug(timestamp)
    logs_dir = base_dir / "logs"
    plans_dir = base_dir / "plans" / day_slug
    reviews_dir = base_dir / "reviews" / day_slug
    for path in (logs_dir, plans_dir, reviews_dir):
        path.mkdir(parents=True, exist_ok=True)
    safe_label = _safe_label(source_label)
    return ArtifactPaths(
        base_dir=base_dir,
        logs_dir=logs_dir,
        plans_dir=plans_dir,
        reviews_dir=reviews_dir,
        csv_log_path=logs_dir / "execution_plans.csv",
        json_path=plans_dir / f"{stamp}_{safe_label}_execution_plan.json",
        markdown_path=reviews_dir / f"{stamp}_{safe_label}_execution_plan.md",
    )


def build_order_intent_artifact_path(artifacts: ArtifactPaths) -> Path:
    return artifacts.plans_dir / artifacts.json_path.name.replace("_execution_plan.json", "_order_intents.json")


def build_manual_order_checklist_path(artifacts: ArtifactPaths) -> Path:
    return artifacts.reviews_dir / artifacts.markdown_path.name.replace("_execution_plan.md", "_manual_order_checklist.md")


def build_manual_ticket_csv_path(artifacts: ArtifactPaths) -> Path:
    return artifacts.plans_dir / artifacts.json_path.name.replace("_execution_plan.json", "_manual_ticket_export.csv")


def build_simulated_submission_artifact_path(artifacts: ArtifactPaths) -> Path:
    return artifacts.plans_dir / artifacts.json_path.name.replace(
        "_execution_plan.json",
        "_simulated_order_requests.json",
    )


def build_live_submission_artifact_path(artifacts: ArtifactPaths) -> Path:
    return artifacts.plans_dir / artifacts.json_path.name.replace(
        "_execution_plan.json",
        "_live_submission.json",
    )


def build_live_submission_ledger_path(artifacts: ArtifactPaths) -> Path:
    return artifacts.logs_dir / "live_submission_fingerprints.jsonl"


def render_manual_order_checklist(export: OrderIntentExport) -> str:
    lines = [
        f"# Manual Order Checklist {export.source_label}",
        "",
        "- This is a dry-run review artifact only. No orders were placed.",
        f"- Generated: `{export.generated_at_chicago}`",
        f"- Dry run only: `{str(export.dry_run).lower()}`",
        f"- Source label: `{export.source_label}`",
        f"- Source ref: `{export.source_ref or '-'}`",
        f"- Broker source: `{export.broker_source_ref or '-'}`",
        f"- Broker/account: `{export.broker_name} / {export.account_id or '-'}`",
        f"- Account scope: `{export.account_scope}`",
        f"- Plan math scope: `{export.plan_math_scope}`",
        f"- Plan SHA256: `{export.plan_sha256}`",
        f"- Sizing mode: `{export.sizing.mode}`",
        f"- Capital input: `{export.sizing.capital_input if export.sizing.capital_input is not None else '-'}`",
        f"- Effective capital used: `{export.sizing.effective_capital_used if export.sizing.effective_capital_used is not None else '-'}`",
        f"- Buying power cap applied: `{str(export.sizing.buying_power_cap_applied).lower()}`",
        f"- Usable capital: `{export.sizing.usable_capital if export.sizing.usable_capital is not None else '-'}`",
        f"- Managed symbols universe: `{', '.join(export.managed_symbols_universe) if export.managed_symbols_universe else '-'}`",
        f"- Unmanaged positions count: `{export.unmanaged_positions_count}`",
        f"- Warnings: `{', '.join(export.warnings) if export.warnings else '-'}`",
        "",
    ]

    if not export.intents:
        lines.extend(["## Orders", "", "- None", ""])
        return "\n".join(lines)

    lines.extend(["## Orders", ""])
    for index, intent in enumerate(export.intents, start=1):
        reference_price = "-" if intent.reference_price is None else f"{intent.reference_price:.2f}"
        estimated_notional = "-" if intent.estimated_notional is None else f"{intent.estimated_notional:.2f}"
        lines.extend(
            [
                f"### {index}. {intent.side} {intent.quantity} {intent.symbol}",
                "",
                f"- Event ID: `{intent.event_id}`",
                f"- Strategy: `{intent.strategy}`",
                f"- Account scope: `{export.account_scope}`",
                f"- Symbol: `{intent.symbol}`",
                f"- Side: `{intent.side}`",
                f"- Quantity: `{intent.quantity}`",
                f"- Reference price: `{reference_price}`",
                f"- Estimated notional: `{estimated_notional}`",
                f"- Classification: `{intent.classification}`",
                "",
            ]
        )
    return "\n".join(lines)


def render_markdown(plan: ExecutionPlan, *, artifacts: ArtifactPaths) -> str:
    plan_status = "BLOCKED" if plan.blockers else "READY"
    plan_preview = build_live_submission_preview(plan)
    plan_sha256 = plan_sha256_for_preview(plan_preview)
    lines = [
        f"# Dry-Run Execution Plan {plan.source_label}",
        "",
        f"- Plan status: `{plan_status}`",
        f"- Generated: `{plan.generated_at_chicago}`",
        f"- Dry run only: `{str(plan.dry_run).lower()}`",
        f"- Account scope: `{plan.account_scope}`",
        f"- Plan math scope: `{plan.plan_math_scope}`",
        f"- Plan SHA256: `{plan_sha256}`",
        f"- Unmanaged holdings acknowledged: `{str(plan.unmanaged_holdings_acknowledged).lower()}`",
        f"- Managed symbols universe: `{', '.join(plan.managed_symbols_universe) if plan.managed_symbols_universe else '-'}`",
        f"- Signal source: `{plan.source_kind}`",
        f"- Signal ref: `{plan.source_ref or '-'}`",
        f"- Broker source: `{plan.broker_source_ref or '-'}`",
        f"- Broker account: `{plan.broker_snapshot.account_id or '-'}`",
        f"- Strategy signal: `{plan.signal.action} {plan.signal.symbol} target={plan.signal.desired_target_shares}`",
        f"- Event ID: `{plan.signal.event_id}`",
        f"- Next rebalance: `{plan.signal.next_rebalance or '-'}`",
        f"- Sizing mode: `{plan.sizing.mode}`",
        f"- Capital input: `{plan.sizing.capital_input if plan.sizing.capital_input is not None else '-'}`",
        f"- Effective capital used: `{plan.sizing.effective_capital_used if plan.sizing.effective_capital_used is not None else '-'}`",
        f"- Buying power cap applied: `{str(plan.sizing.buying_power_cap_applied).lower()}`",
        f"- Reserve cash pct: `{plan.sizing.reserve_cash_pct}`",
        f"- Max allocation pct: `{plan.sizing.max_allocation_pct}`",
        f"- Usable capital: `{plan.sizing.usable_capital if plan.sizing.usable_capital is not None else '-'}`",
        f"- Inferred signal allocation pct: `{plan.sizing.inferred_signal_allocation_pct if plan.sizing.inferred_signal_allocation_pct is not None else '-'}`",
        f"- Applied allocation pct: `{plan.sizing.applied_allocation_pct if plan.sizing.applied_allocation_pct is not None else '-'}`",
        f"- JSON artifact: `{artifacts.json_path}`",
        f"- CSV log: `{artifacts.csv_log_path}`",
        "",
        "## Totals",
        "",
        f"- Buy notional: `{plan.total_buy_notional:.2f}`",
        f"- Sell notional: `{plan.total_sell_notional:.2f}`",
        f"- Net notional: `{plan.net_notional:.2f}`",
        f"- Cash: `{plan.broker_snapshot.cash if plan.broker_snapshot.cash is not None else '-'}`",
        f"- Buying power: `{plan.broker_snapshot.buying_power if plan.broker_snapshot.buying_power is not None else '-'}`",
        "",
    ]

    if plan.warnings:
        lines.extend(["## Warnings", ""])
        for warning in plan.warnings:
            lines.append(f"- `{warning}`")
        lines.append("")

    if plan.blockers:
        lines.extend(["## Blockers", ""])
        for blocker in plan.blockers:
            lines.append(f"- `{blocker}`")
        lines.append("")

    def _append_scope_section(title: str, positions: list[object]) -> None:
        lines.extend([f"## {title}", ""])
        if not positions:
            lines.append("- None")
            lines.append("")
            return
        lines.extend(
            [
                "| Symbol | Scope Symbol | Shares | Instrument Type | Price | Reason |",
                "| --- | --- | ---: | --- | ---: | --- |",
            ]
        )
        for position in positions:
            price = "-" if position.price is None else f"{position.price:.2f}"
            instrument_type = position.instrument_type or "-"
            lines.append(
                f"| {position.symbol} | {position.scope_symbol} | {position.shares} | {instrument_type} | "
                f"{price} | {position.classification_reason} |"
            )
        lines.append("")

    _append_scope_section("Managed Supported Positions", plan.managed_supported_positions)
    _append_scope_section("Managed Unsupported Positions", plan.managed_unsupported_positions)
    _append_scope_section("Unmanaged Positions", plan.unmanaged_positions)

    lines.extend(
        [
            "## Symbols",
            "",
            "| Symbol | Desired | Current | Delta | Classification | Ref Price | Est Notional | Warnings | Blockers |",
            "| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | --- |",
        ]
    )
    for item in plan.items:
        ref_price = "-" if item.reference_price is None else f"{item.reference_price:.2f}"
        est_notional = "-" if item.estimated_notional is None else f"{item.estimated_notional:.2f}"
        warnings = ", ".join(item.warnings) if item.warnings else "-"
        blockers = ", ".join(item.blockers) if item.blockers else "-"
        lines.append(
            f"| {item.symbol} | {item.desired_target_shares} | {item.current_broker_shares} | {item.delta_shares} | "
            f"{item.classification} | {ref_price} | {est_notional} | {warnings} | {blockers} |"
        )

    return "\n".join(lines) + "\n"


def _append_csv_log(plan: ExecutionPlan, *, artifacts: ArtifactPaths) -> None:
    fieldnames = [
        "generated_at_chicago",
        "account_scope",
        "source_kind",
        "source_label",
        "signal_event_id",
        "broker_name",
        "account_id",
        "buy_notional",
        "sell_notional",
        "warnings_count",
        "blockers_count",
        "json_path",
        "markdown_path",
    ]
    row = {
        "generated_at_chicago": plan.generated_at_chicago,
        "account_scope": plan.account_scope,
        "source_kind": plan.source_kind,
        "source_label": plan.source_label,
        "signal_event_id": plan.signal.event_id,
        "broker_name": plan.broker_snapshot.broker_name,
        "account_id": plan.broker_snapshot.account_id or "",
        "buy_notional": f"{plan.total_buy_notional:.2f}",
        "sell_notional": f"{plan.total_sell_notional:.2f}",
        "warnings_count": str(len(plan.warnings)),
        "blockers_count": str(len(plan.blockers)),
        "json_path": str(artifacts.json_path),
        "markdown_path": str(artifacts.markdown_path),
    }

    write_header = not artifacts.csv_log_path.exists()
    with artifacts.csv_log_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def write_order_intent_artifact(
    export: OrderIntentExport,
    *,
    path: Path,
    artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = order_intent_export_to_dict(export, artifacts=artifacts)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def write_simulated_submission_artifact(
    export: SimulatedSubmissionExport,
    *,
    path: Path,
    artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = simulated_submission_export_to_dict(export, artifacts=artifacts)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _live_submission_export_to_dict(
    export: LiveSubmissionExport,
    *,
    artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "account_id": export.account_id,
        "account_scope": export.account_scope,
        "artifacts": artifacts or {},
        "blockers": list(export.blockers),
        "broker_name": export.broker_name,
        "broker_source_ref": export.broker_source_ref,
        "dry_run": export.dry_run,
        "duplicate_submit_refusal": export.duplicate_submit_refusal,
        "generated_at_chicago": export.generated_at_chicago,
        "live_allowed_account": export.live_allowed_account,
        "live_max_order_notional": export.live_max_order_notional,
        "live_max_order_qty": export.live_max_order_qty,
        "live_submission_fingerprint": export.live_submission_fingerprint,
        "live_submit_attempted": export.live_submit_attempted,
        "live_submission_preview": export.plan_preview,
        "live_submit_requested": export.live_submit_requested,
        "managed_symbols_universe": list(export.managed_symbols_universe),
        "orders": [
            {
                "account_id": order.account_id,
                "attempted": order.attempted,
                "broker_name": order.broker_name,
                "broker_order_id": order.broker_order_id,
                "broker_response": order.broker_response,
                "broker_status": order.broker_status,
                "classification": order.classification,
                "dry_run": order.dry_run,
                "error": order.error,
                "estimated_notional": order.estimated_notional,
                "event_id": order.event_id,
                "instrument_type": order.instrument_type,
                "order_type": order.order_type,
                "quantity": order.quantity,
                "reference_price": order.reference_price,
                "side": order.side,
                "strategy": order.strategy,
                "submitted_at_chicago": order.submitted_at_chicago,
                "succeeded": order.succeeded,
                "symbol": order.symbol,
                "time_in_force": order.time_in_force,
            }
            for order in export.orders
        ],
        "plan_sha256": export.plan_sha256,
        "plan_math_scope": export.plan_math_scope,
        "refusal_reasons": list(export.refusal_reasons),
        "schema_name": "live_submission_export",
        "schema_version": 1,
        "sizing": {
            "applied_allocation_pct": export.sizing.applied_allocation_pct,
            "baseline_signal_capital": export.sizing.baseline_signal_capital,
            "buying_power_cap_applied": export.sizing.buying_power_cap_applied,
            "capital_input": export.sizing.capital_input,
            "effective_capital_used": export.sizing.effective_capital_used,
            "inferred_signal_allocation_pct": export.sizing.inferred_signal_allocation_pct,
            "max_allocation_pct": export.sizing.max_allocation_pct,
            "mode": export.sizing.mode,
            "reserve_cash_pct": export.sizing.reserve_cash_pct,
            "usable_capital": export.sizing.usable_capital,
        },
        "source": {
            "kind": export.source_kind,
            "label": export.source_label,
            "ref": export.source_ref,
        },
        "submission_succeeded": export.submission_succeeded,
        "unmanaged_holdings_acknowledged": export.unmanaged_holdings_acknowledged,
        "unmanaged_positions_count": export.unmanaged_positions_count,
        "unmanaged_positions_summary": [
            {
                "classification_reason": position.classification_reason,
                "instrument_type": position.instrument_type,
                "price": position.price,
                "scope_symbol": position.scope_symbol,
                "shares": position.shares,
                "symbol": position.symbol,
                "underlying_symbol": position.underlying_symbol,
            }
            for position in export.unmanaged_positions_summary
        ],
        "warnings": list(export.warnings),
    }


def write_live_submission_artifact(
    export: LiveSubmissionExport,
    *,
    path: Path,
    artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = _live_submission_export_to_dict(export, artifacts=artifacts)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def write_manual_order_checklist(export: OrderIntentExport, *, path: Path) -> None:
    path.write_text(render_manual_order_checklist(export) + "\n", encoding="utf-8")


def write_manual_ticket_csv(export: OrderIntentExport, *, path: Path) -> None:
    fieldnames = [
        "generated_at_chicago",
        "source_label",
        "event_id",
        "strategy",
        "account_scope",
        "plan_math_scope",
        "symbol",
        "side",
        "quantity",
        "reference_price",
        "estimated_notional",
        "classification",
        "current_broker_shares",
        "desired_target_shares",
        "warnings",
        "unmanaged_positions_count",
        "broker_source_ref",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for intent in export.intents:
            writer.writerow(
                {
                    "generated_at_chicago": export.generated_at_chicago,
                    "source_label": export.source_label,
                    "event_id": intent.event_id,
                    "strategy": intent.strategy,
                    "account_scope": export.account_scope,
                    "plan_math_scope": export.plan_math_scope,
                    "symbol": intent.symbol,
                    "side": intent.side,
                    "quantity": str(intent.quantity),
                    "reference_price": "" if intent.reference_price is None else f"{intent.reference_price:.2f}",
                    "estimated_notional": "" if intent.estimated_notional is None else f"{intent.estimated_notional:.2f}",
                    "classification": intent.classification,
                    "current_broker_shares": str(intent.current_broker_shares),
                    "desired_target_shares": str(intent.desired_target_shares),
                    "warnings": ", ".join(intent.warnings),
                    "unmanaged_positions_count": str(export.unmanaged_positions_count),
                    "broker_source_ref": export.broker_source_ref or "",
                }
            )


def write_artifacts(
    plan: ExecutionPlan,
    *,
    artifacts: ArtifactPaths,
    extra_artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    artifact_dict = {
        "csv_log_path": str(artifacts.csv_log_path),
        "json_path": str(artifacts.json_path),
        "markdown_path": str(artifacts.markdown_path),
    }
    if extra_artifacts:
        artifact_dict.update(extra_artifacts)
    payload = execution_plan_to_dict(plan, artifacts=artifact_dict)
    artifacts.json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts.markdown_path.write_text(render_markdown(plan, artifacts=artifacts), encoding="utf-8")
    _append_csv_log(plan, artifacts=artifacts)
    return payload
