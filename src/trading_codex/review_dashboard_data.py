from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from trading_codex.run_archive import recent_runs, resolve_archive_root, resolve_manifest_path


SUMMARY_FIELDS: tuple[tuple[str, str], ...] = (
    ("run_kind", "run_kind"),
    ("mode", "mode"),
    ("source_label", "source_label"),
    ("strategy", "strategy"),
    ("action", "action"),
    ("symbol", "symbol"),
    ("target_shares", "target_shares"),
    ("resize_new_shares", "resize_new_shares"),
    ("next_rebalance", "next_rebalance"),
    ("buying_power_available", "buying_power_available"),
    ("effective_capital", "effective_capital"),
    ("leverage", "leverage"),
    ("vol_target", "vol_target"),
    ("plan_sha256", "plan_sha256"),
    ("event_id", "event_id"),
)

REVIEW_ARTIFACT_PRIORITY: tuple[str, ...] = (
    "execution_plan_markdown",
    "review_markdown",
    "manual_order_checklist_path",
)

PRIMARY_ACTIVITY_ARTIFACT_PRIORITY: tuple[str, ...] = (
    *REVIEW_ARTIFACT_PRIORITY,
    "manual_ticket_csv_path",
    "simulated_order_requests_path",
    "order_intents_json_path",
    "execution_plan_json",
    "signal_payload",
    "next_action_payload",
)

NEEDS_REVIEW_PRIORITIES: dict[str, int] = {
    "blockers": 500,
    "warnings": 400,
    "missing_review": 300,
    "trade_change": 250,
    "capital_change": 200,
}


@dataclass(frozen=True)
class ReviewRun:
    archive_root: Path
    manifest_path: Path | None
    manifest: dict[str, Any]
    payloads: dict[str, dict[str, Any]]

    @property
    def run_id(self) -> str:
        value = self.manifest.get("run_id")
        if isinstance(value, str) and value:
            return value
        if self.manifest_path is not None:
            return self.manifest_path.parent.name
        return "-"

    @property
    def run_kind(self) -> str:
        value = self.manifest.get("run_kind")
        return value if isinstance(value, str) and value else "-"

    @property
    def timestamp(self) -> str:
        value = self.manifest.get("timestamp")
        return value if isinstance(value, str) and value else "-"

    def resolved_artifact_paths(self) -> dict[str, Path]:
        resolved: dict[str, Path] = {}
        artifact_paths = _as_dict(self.manifest.get("artifact_paths"))
        for key, raw_path in artifact_paths.items():
            if not isinstance(raw_path, str) or not raw_path:
                continue
            path = Path(raw_path)
            if not path.is_absolute() and self.manifest_path is not None:
                path = self.manifest_path.parent / path
            resolved[key] = path
        return resolved

    def signal_payload(self) -> dict[str, Any]:
        payload = self.payloads.get("signal_payload")
        if payload:
            return payload
        payload = self.payloads.get("next_action_payload")
        if payload:
            return payload
        signal = _as_dict(self.execution_plan().get("signal"))
        if signal:
            return signal
        return {}

    def execution_plan(self) -> dict[str, Any]:
        return _as_dict(self.payloads.get("execution_plan_json"))

    def order_intents(self) -> dict[str, Any]:
        return _as_dict(self.payloads.get("order_intents_json_path"))

    def warnings(self) -> list[str]:
        values: list[str] = []
        for source in (
            self.manifest.get("warnings"),
            self.execution_plan().get("warnings"),
            self.order_intents().get("warnings"),
        ):
            values.extend(_as_string_list(source))
        for item in _iter_dict_list(self.execution_plan().get("items")):
            values.extend(_as_string_list(item.get("warnings")))
        for intent in _iter_dict_list(self.order_intents().get("intents")):
            values.extend(_as_string_list(intent.get("warnings")))
        return _dedupe_preserve_order(values)

    def blockers(self) -> list[str]:
        values: list[str] = []
        for source in (
            self.manifest.get("blockers"),
            self.execution_plan().get("blockers"),
            self.order_intents().get("blockers"),
        ):
            values.extend(_as_string_list(source))
        for item in _iter_dict_list(self.execution_plan().get("items")):
            values.extend(_as_string_list(item.get("blockers")))
        for intent in _iter_dict_list(self.order_intents().get("intents")):
            values.extend(_as_string_list(intent.get("blockers")))
        return _dedupe_preserve_order(values)

    def proposed_trades(self) -> list[dict[str, Any]]:
        trades = _extract_execution_plan_trades(self.execution_plan())
        if trades:
            return trades
        return _extract_order_intent_trades(self.order_intents())


def load_review_runs(
    *,
    limit: int = 25,
    root_dir: Path | None = None,
    home_dir: Path | None = None,
    tmp_root: Path | None = None,
) -> tuple[Path, list[ReviewRun]]:
    archive_root = resolve_archive_root(
        preferred_root=root_dir,
        home_dir=home_dir,
        tmp_root=tmp_root,
        create=False,
    )
    entries = recent_runs(limit=max(int(limit), 0), root_dir=archive_root)
    runs: list[ReviewRun] = []
    if entries:
        for entry in entries:
            run = _load_review_run_from_entry(archive_root, entry)
            if run is not None:
                runs.append(run)
        return archive_root, runs
    return archive_root, _scan_review_runs_from_manifests(archive_root, limit=max(int(limit), 0))


def summarize_run(run: ReviewRun) -> dict[str, Any]:
    signal = run.signal_payload()
    execution_plan = run.execution_plan()
    order_intents = run.order_intents()
    sizing = _as_dict(execution_plan.get("sizing")) or _as_dict(order_intents.get("sizing"))
    broker_snapshot = _as_dict(execution_plan.get("broker_snapshot"))
    live_preview = _as_dict(execution_plan.get("live_submission_preview")) or _as_dict(
        order_intents.get("live_submission_preview")
    )
    manifest_source = _as_dict(run.manifest.get("source"))
    execution_source = _as_dict(execution_plan.get("source"))
    order_source = _as_dict(order_intents.get("source"))

    return {
        "run_id": run.run_id,
        "timestamp": _first_present(run.manifest.get("timestamp"), execution_plan.get("generated_at_chicago")),
        "run_kind": _first_present(run.manifest.get("run_kind")),
        "mode": _first_present(run.manifest.get("mode")),
        "source_label": _first_present(
            manifest_source.get("label"),
            execution_source.get("label"),
            order_source.get("label"),
        ),
        "strategy": _first_present(run.manifest.get("strategy"), signal.get("strategy"), live_preview.get("strategy")),
        "action": _first_present(run.manifest.get("action"), signal.get("action")),
        "symbol": _first_present(run.manifest.get("symbol"), signal.get("symbol")),
        "target_shares": _first_present(run.manifest.get("target_shares"), signal.get("target_shares")),
        "resize_new_shares": _first_present(
            run.manifest.get("resize_new_shares"),
            signal.get("resize_new_shares"),
        ),
        "next_rebalance": _first_present(
            run.manifest.get("next_rebalance"),
            signal.get("next_rebalance"),
            live_preview.get("rebalance_date"),
        ),
        "buying_power_available": _first_present(
            run.manifest.get("buying_power_available"),
            broker_snapshot.get("buying_power"),
        ),
        "effective_capital": _first_present(
            run.manifest.get("effective_capital"),
            sizing.get("effective_capital_used"),
            live_preview.get("effective_capital_used"),
        ),
        "leverage": _first_present(run.manifest.get("leverage"), signal.get("leverage")),
        "vol_target": _first_present(run.manifest.get("vol_target"), signal.get("vol_target")),
        "plan_sha256": _first_present(
            run.manifest.get("plan_sha256"),
            execution_plan.get("plan_sha256"),
            order_intents.get("plan_sha256"),
        ),
        "event_id": _first_present(
            run.manifest.get("event_id"),
            signal.get("event_id"),
            live_preview.get("event_id"),
        ),
        "broker_account_id": _first_present(
            broker_snapshot.get("account_id"),
            order_intents.get("account_id"),
            live_preview.get("broker_account_id"),
        ),
        "buying_power_cap_applied": sizing.get("buying_power_cap_applied"),
        "live_submit_state_touched": run.manifest.get("live_submit_state_touched"),
    }


def build_run_history_rows(runs: list[ReviewRun]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in runs:
        summary = summarize_run(run)
        rows.append(
            {
                "timestamp": summary.get("timestamp"),
                "run_kind": summary.get("run_kind"),
                "mode": summary.get("mode"),
                "source_label": summary.get("source_label"),
                "strategy": summary.get("strategy"),
                "action": summary.get("action"),
                "symbol": summary.get("symbol"),
                "warning_count": len(run.warnings()),
                "blocker_count": len(run.blockers()),
                "trade_count": len(run.proposed_trades()),
                "run_id": run.run_id,
                "manifest_path": str(run.manifest_path) if run.manifest_path is not None else "-",
            }
        )
    return rows


def build_run_comparison_rows(current: ReviewRun, previous: ReviewRun) -> list[dict[str, str]]:
    current_summary = summarize_run(current)
    previous_summary = summarize_run(previous)
    rows: list[dict[str, str]] = []
    for key, label in SUMMARY_FIELDS:
        current_value = current_summary.get(key)
        previous_value = previous_summary.get(key)
        if _normalize_for_compare(current_value) == _normalize_for_compare(previous_value):
            continue
        rows.append(
            {
                "field": label,
                "latest": _format_value(current_value),
                "previous": _format_value(previous_value),
            }
        )

    current_trades = _trade_signatures(current.proposed_trades())
    previous_trades = _trade_signatures(previous.proposed_trades())
    if current_trades != previous_trades:
        rows.append(
            {
                "field": "proposed_trades",
                "latest": "; ".join(current_trades) or "-",
                "previous": "; ".join(previous_trades) or "-",
            }
        )
    return rows


def build_artifact_rows(run: ReviewRun) -> list[dict[str, str]]:
    rows = [
        {
            "artifact": "manifest",
            "path": str(run.manifest_path) if run.manifest_path is not None else "-",
        }
    ]
    for key, path in sorted(run.resolved_artifact_paths().items()):
        rows.append({"artifact": key, "path": str(path)})
    return rows


def build_needs_review_rows(runs: list[ReviewRun]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, run in enumerate(runs):
        summary = summarize_run(run)
        prior = _find_prior_comparable_run(run, runs[index + 1 :])
        warnings = run.warnings()
        blockers = run.blockers()

        if blockers:
            rows.append(
                _build_needs_review_row(
                    priority_key="blockers",
                    run=run,
                    summary=summary,
                    headline="Archived run contains blockers",
                    detail=f"{len(blockers)} blocker(s): " + "; ".join(blockers),
                    path=_preferred_review_path(run),
                )
            )

        if warnings:
            rows.append(
                _build_needs_review_row(
                    priority_key="warnings",
                    run=run,
                    summary=summary,
                    headline="Archived run contains warnings",
                    detail=f"{len(warnings)} warning(s): " + "; ".join(warnings),
                    path=_preferred_review_path(run),
                )
            )

        if _has_plan_artifact(run) and not _review_artifact_paths(run):
            rows.append(
                _build_needs_review_row(
                    priority_key="missing_review",
                    run=run,
                    summary=summary,
                    headline="New plan found; no review artifact detected",
                    detail="Plan/order artifacts exist in the archive, but no markdown/checklist review artifact was found.",
                    path=_preferred_plan_path(run),
                )
            )

        if prior is None:
            continue

        current_trade_signatures = _trade_signatures(run.proposed_trades())
        previous_trade_signatures = _trade_signatures(prior.proposed_trades())
        if current_trade_signatures != previous_trade_signatures:
            rows.append(
                _build_needs_review_row(
                    priority_key="trade_change",
                    run=run,
                    summary=summary,
                    headline="New execution plan with trade changes vs prior comparable run",
                    detail=_build_trade_change_detail(
                        current_trade_signatures=current_trade_signatures,
                        previous_trade_signatures=previous_trade_signatures,
                    ),
                    path=_preferred_plan_path(run),
                    compare_to_run=prior,
                    compare_to_path=_preferred_plan_path(prior),
                )
            )

        capital_detail = _build_capital_change_detail(run=run, previous=prior)
        if capital_detail is not None:
            rows.append(
                _build_needs_review_row(
                    priority_key="capital_change",
                    run=run,
                    summary=summary,
                    headline="Capital allocation changed from prior comparable run",
                    detail=capital_detail,
                    path=_preferred_plan_path(run),
                    compare_to_run=prior,
                    compare_to_path=_preferred_plan_path(prior),
                )
            )

    rows.sort(
        key=lambda row: (
            int(row.get("_priority", 0)),
            str(row.get("timestamp") or ""),
            str(row.get("run_id") or ""),
        ),
        reverse=True,
    )
    for row in rows:
        row.pop("_priority", None)
    return rows


def build_recent_activity_rows(runs: list[ReviewRun], *, limit: int = 25) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, run in enumerate(runs):
        summary = summarize_run(run)
        prior = _find_prior_comparable_run(run, runs[index + 1 :])
        artifact_type, path = _primary_activity_artifact(run)
        related_paths = _related_activity_paths(run=run, primary_path=path)
        rows.append(
            {
                "timestamp": summary.get("timestamp"),
                "label": _run_label(summary),
                "artifact_type": artifact_type,
                "status": _recent_activity_status(run=run, prior=prior),
                "run_id": run.run_id,
                "path": str(path) if path is not None else "-",
                "related_paths": "; ".join(related_paths) if related_paths else "-",
            }
        )
    return rows[: max(int(limit), 0)]


def _load_review_run_from_entry(root_dir: Path, entry: Mapping[str, Any]) -> ReviewRun | None:
    manifest_path: Path | None = None
    manifest: dict[str, Any] = {}
    try:
        manifest_path = resolve_manifest_path(entry, root_dir=root_dir)
    except Exception:
        manifest_path = None

    if manifest_path is not None:
        loaded_manifest = _load_json_dict(manifest_path)
        if loaded_manifest is not None:
            manifest = loaded_manifest
    if not manifest:
        manifest = dict(entry)

    return _build_review_run(root_dir=root_dir, manifest_path=manifest_path, manifest=manifest)


def _scan_review_runs_from_manifests(root_dir: Path, *, limit: int) -> list[ReviewRun]:
    runs_dir = root_dir / "runs"
    if not runs_dir.exists():
        return []

    loaded: list[ReviewRun] = []
    for manifest_path in runs_dir.glob("*/*/manifest.json"):
        manifest = _load_json_dict(manifest_path)
        if manifest is None:
            continue
        run = _build_review_run(root_dir=root_dir, manifest_path=manifest_path, manifest=manifest)
        loaded.append(run)

    loaded.sort(key=lambda item: str(item.manifest.get("timestamp", "")), reverse=True)
    return loaded[:limit]


def _build_review_run(*, root_dir: Path, manifest_path: Path | None, manifest: dict[str, Any]) -> ReviewRun:
    payloads: dict[str, dict[str, Any]] = {}
    artifact_paths = _as_dict(manifest.get("artifact_paths"))
    for key in ("execution_plan_json", "order_intents_json_path", "signal_payload", "next_action_payload"):
        raw_path = artifact_paths.get(key)
        if not isinstance(raw_path, str) or not raw_path:
            continue
        path = Path(raw_path)
        if not path.is_absolute() and manifest_path is not None:
            path = manifest_path.parent / path
        loaded = _load_json_dict(path)
        if loaded is not None:
            payloads[key] = loaded
    return ReviewRun(
        archive_root=root_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        payloads=payloads,
    )


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _extract_execution_plan_trades(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for item in _iter_dict_list(plan.get("items")):
        delta = item.get("delta_shares")
        quantity = item.get("quantity")
        if quantity is None and isinstance(delta, (int, float)):
            quantity = abs(int(delta))
        side = _first_present(item.get("side"), item.get("classification"))
        if side is None and isinstance(delta, (int, float)):
            if delta > 0:
                side = "BUY"
            elif delta < 0:
                side = "SELL"
            else:
                side = "HOLD"
        trades.append(
            {
                "symbol": item.get("symbol"),
                "side": side,
                "quantity": quantity,
                "reference_price": item.get("reference_price"),
                "estimated_notional": item.get("estimated_notional"),
                "current_broker_shares": item.get("current_broker_shares"),
                "desired_target_shares": item.get("desired_target_shares"),
            }
        )
    return trades


def _extract_order_intent_trades(order_intents: Mapping[str, Any]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for intent in _iter_dict_list(order_intents.get("intents")):
        trades.append(
            {
                "symbol": intent.get("symbol"),
                "side": intent.get("side"),
                "quantity": intent.get("quantity"),
                "reference_price": intent.get("reference_price"),
                "estimated_notional": intent.get("estimated_notional"),
                "current_broker_shares": intent.get("current_broker_shares"),
                "desired_target_shares": intent.get("desired_target_shares"),
            }
        )
    return trades


def _trade_signatures(trades: list[dict[str, Any]]) -> list[str]:
    normalized: list[tuple[tuple[str, str, str, str], str]] = []
    for trade in trades:
        identity = _trade_intent_identity(trade)
        normalized.append((identity, _trade_intent_signature(identity)))
    normalized.sort(key=lambda item: item[0])
    return [signature for _, signature in normalized]


def _trade_intent_identity(trade: Mapping[str, Any]) -> tuple[str, str, str, str]:
    side = _format_value(trade.get("side"))
    symbol = _format_value(trade.get("symbol"))
    quantity = trade.get("quantity")
    target_quantity = trade.get("desired_target_shares")

    if quantity is not None and quantity != "":
        return (side, symbol, "quantity", _format_value(quantity))
    if target_quantity is not None and target_quantity != "":
        return (side, symbol, "target", _format_value(target_quantity))
    return (side, symbol, "quantity", "-")


def _trade_intent_signature(identity: tuple[str, str, str, str]) -> str:
    side, symbol, quantity_kind, quantity_value = identity
    if quantity_value == "-":
        return " ".join(part for part in (side, symbol) if part and part != "-") or "-"
    if quantity_kind == "target":
        return " ".join(part for part in (side, "target", quantity_value, symbol) if part and part != "-")
    return " ".join(part for part in (side, quantity_value, symbol) if part and part != "-")


def _review_artifact_paths(run: ReviewRun) -> dict[str, Path]:
    resolved = run.resolved_artifact_paths()
    review_paths: dict[str, Path] = {}
    for key, path in resolved.items():
        if key in REVIEW_ARTIFACT_PRIORITY or key.endswith("_markdown") or "checklist" in key or "review" in key:
            review_paths[key] = path
    return review_paths


def _has_plan_artifact(run: ReviewRun) -> bool:
    resolved = run.resolved_artifact_paths()
    return "execution_plan_json" in resolved or "order_intents_json_path" in resolved


def _preferred_review_path(run: ReviewRun) -> Path | None:
    review_paths = _review_artifact_paths(run)
    for key in REVIEW_ARTIFACT_PRIORITY:
        path = review_paths.get(key)
        if path is not None:
            return path
    return _preferred_plan_path(run)


def _preferred_plan_path(run: ReviewRun) -> Path | None:
    resolved = run.resolved_artifact_paths()
    for key in ("execution_plan_json", "order_intents_json_path", "signal_payload", "next_action_payload"):
        path = resolved.get(key)
        if path is not None:
            return path
    return run.manifest_path


def _primary_activity_artifact(run: ReviewRun) -> tuple[str, Path | None]:
    resolved = run.resolved_artifact_paths()
    for key in PRIMARY_ACTIVITY_ARTIFACT_PRIORITY:
        path = resolved.get(key)
        if path is not None:
            return key, path
    return "manifest", run.manifest_path


def _related_activity_paths(*, run: ReviewRun, primary_path: Path | None) -> list[str]:
    candidates: list[str] = []
    for path in list(_review_artifact_paths(run).values()) + list(run.resolved_artifact_paths().values()):
        if primary_path is not None and path == primary_path:
            continue
        candidates.append(str(path))
    if run.manifest_path is not None and (primary_path is None or run.manifest_path != primary_path):
        candidates.append(str(run.manifest_path))
    return _dedupe_preserve_order(candidates)


def _find_prior_comparable_run(current: ReviewRun, previous_runs: list[ReviewRun]) -> ReviewRun | None:
    target_key = _comparison_key(current)
    if target_key is None:
        return None
    for candidate in previous_runs:
        if _comparison_key(candidate) == target_key:
            return candidate
    return None


def _comparison_key(run: ReviewRun) -> tuple[str, str, str, str, str, str] | None:
    summary = summarize_run(run)
    source_label = summary.get("source_label")
    strategy = summary.get("strategy")
    symbol = summary.get("symbol")
    if all(_is_blank(value) for value in (source_label, strategy, symbol)):
        return None
    return (
        str(summary.get("run_kind") or "-"),
        str(summary.get("mode") or "-"),
        str(source_label or "-"),
        str(strategy or "-"),
        str(symbol or "-"),
        str(summary.get("broker_account_id") or "-"),
    )


def _recent_activity_status(*, run: ReviewRun, prior: ReviewRun | None) -> str:
    blockers = run.blockers()
    if blockers:
        return "Archived run contains blockers"
    warnings = run.warnings()
    if warnings:
        return "Archived run contains warnings"
    if _has_plan_artifact(run) and not _review_artifact_paths(run):
        return "New plan found; no review artifact detected"
    if prior is not None:
        current_trade_signatures = _trade_signatures(run.proposed_trades())
        previous_trade_signatures = _trade_signatures(prior.proposed_trades())
        if current_trade_signatures != previous_trade_signatures:
            return "New execution plan with trade changes vs prior plan"
        if _build_capital_change_detail(run=run, previous=prior) is not None:
            return "Capital allocation changed from prior comparable run"
    trade_count = len(run.proposed_trades())
    if trade_count == 1:
        return "Archived run with 1 proposed trade"
    if trade_count > 1:
        return f"Archived run with {trade_count} proposed trades"
    return "Archived run available for review"


def _build_needs_review_row(
    *,
    priority_key: str,
    run: ReviewRun,
    summary: dict[str, Any],
    headline: str,
    detail: str,
    path: Path | None,
    compare_to_run: ReviewRun | None = None,
    compare_to_path: Path | None = None,
) -> dict[str, Any]:
    return {
        "_priority": NEEDS_REVIEW_PRIORITIES[priority_key],
        "priority": _priority_label(priority_key),
        "timestamp": summary.get("timestamp"),
        "label": _run_label(summary),
        "headline": headline,
        "detail": detail,
        "run_id": run.run_id,
        "path": str(path) if path is not None else "-",
        "compare_to_run_id": compare_to_run.run_id if compare_to_run is not None else "-",
        "compare_to_path": str(compare_to_path) if compare_to_path is not None else "-",
    }


def _run_label(summary: Mapping[str, Any]) -> str:
    return _format_value(
        _first_present(
            summary.get("source_label"),
            summary.get("strategy"),
            summary.get("symbol"),
            summary.get("run_kind"),
            summary.get("run_id"),
        )
    )


def _priority_label(priority_key: str) -> str:
    if priority_key == "blockers":
        return "high"
    if priority_key in {"warnings", "missing_review"}:
        return "medium"
    return "low"


def _build_trade_change_detail(
    *,
    current_trade_signatures: list[str],
    previous_trade_signatures: list[str],
) -> str:
    current_text = "; ".join(current_trade_signatures) or "no proposed trades"
    previous_text = "; ".join(previous_trade_signatures) or "no proposed trades"
    return f"Current: {current_text}. Previous: {previous_text}."


def _build_capital_change_detail(*, run: ReviewRun, previous: ReviewRun) -> str | None:
    current_summary = summarize_run(run)
    previous_summary = summarize_run(previous)
    changes: list[str] = []
    for key, label in (
        ("effective_capital", "effective_capital"),
        ("buying_power_available", "buying_power_available"),
    ):
        current_value = current_summary.get(key)
        previous_value = previous_summary.get(key)
        if _normalize_for_compare(current_value) == _normalize_for_compare(previous_value):
            continue
        changes.append(f"{label}: {_format_value(previous_value)} -> {_format_value(current_value)}")

    current_notional = _estimated_notional_total(run)
    previous_notional = _estimated_notional_total(previous)
    if _normalize_for_compare(current_notional) != _normalize_for_compare(previous_notional):
        changes.append(f"estimated_notional: {_format_value(previous_notional)} -> {_format_value(current_notional)}")

    if not changes:
        return None
    return "; ".join(changes)


def _estimated_notional_total(run: ReviewRun) -> float | None:
    execution_totals = _as_dict(run.execution_plan().get("totals"))
    value = _first_present(
        execution_totals.get("net_notional"),
        execution_totals.get("buy_notional"),
    )
    if isinstance(value, (int, float)):
        return float(value)

    total = 0.0
    found = False
    for trade in run.proposed_trades():
        estimated = trade.get("estimated_notional")
        if not isinstance(estimated, (int, float)):
            continue
        total += float(estimated)
        found = True
    if found:
        return round(total, 8)
    return None


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _iter_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value == "":
            continue
        return value
    return None


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value == "")


def _normalize_for_compare(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 8)
    if isinstance(value, list):
        return [_normalize_for_compare(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_for_compare(item) for key, item in sorted(value.items())}
    return value


def _format_value(value: Any) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, float):
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    if isinstance(value, list):
        return ", ".join(_format_value(item) for item in value) or "-"
    return str(value)
