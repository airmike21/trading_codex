from __future__ import annotations

import json
from pathlib import Path

from trading_codex.review_dashboard_data import (
    build_artifact_rows,
    build_needs_review_rows,
    build_recent_activity_rows,
    build_run_comparison_rows,
    build_run_history_rows,
    load_review_runs,
    summarize_run,
)
from trading_codex.run_archive import write_run_archive


def _execution_plan_payload(
    *,
    symbol: str = "EFA",
    action: str = "BUY",
    quantity: int = 24,
    price: float = 99.16,
    effective_capital: float = 2455.99,
    buying_power: float = 2455.99,
    target_shares: int = 100,
    resize_new_shares: int | None = None,
    leverage: float | None = None,
    vol_target: float | None = None,
    source_label: str = "dual_mom_core",
    account_id: str = "paper-1",
    warnings: list[str] | None = None,
    blockers: list[str] | None = None,
    trade_warnings: list[str] | None = None,
    trade_blockers: list[str] | None = None,
) -> dict[str, object]:
    warnings = ["warning_from_plan"] if warnings is None else list(warnings)
    blockers = [] if blockers is None else list(blockers)
    trade_warnings = ["warning_from_trade"] if trade_warnings is None else list(trade_warnings)
    trade_blockers = [] if trade_blockers is None else list(trade_blockers)
    return {
        "schema_name": "execution_plan",
        "schema_version": 2,
        "generated_at_chicago": "2026-03-11T15:47:32-05:00",
        "warnings": warnings,
        "blockers": blockers,
        "signal": {
            "strategy": "dual_mom",
            "action": action,
            "symbol": symbol,
            "target_shares": target_shares,
            "resize_new_shares": resize_new_shares,
            "next_rebalance": "2026-03-31",
            "event_id": f"2026-03-11:dual_mom:{action}:{symbol}:{target_shares}:{resize_new_shares or ''}:2026-03-31",
            "leverage": leverage,
            "vol_target": vol_target,
        },
        "broker_snapshot": {
            "account_id": account_id,
            "buying_power": buying_power,
        },
        "sizing": {
            "effective_capital_used": effective_capital,
            "buying_power_cap_applied": True,
        },
        "items": [
            {
                "classification": action,
                "delta_shares": quantity,
                "current_broker_shares": 0,
                "desired_target_shares": quantity,
                "estimated_notional": round(quantity * price, 2),
                "reference_price": price,
                "symbol": symbol,
                "warnings": trade_warnings,
                "blockers": trade_blockers,
            }
        ],
        "source": {
            "kind": "preset",
            "label": source_label,
            "ref": "/tmp/presets.json",
        },
        "live_submission_preview": {
            "broker_account_id": account_id,
            "effective_capital_used": effective_capital,
            "event_id": f"2026-03-11:dual_mom:{action}:{symbol}:{target_shares}:{resize_new_shares or ''}:2026-03-31",
            "rebalance_date": "2026-03-31",
            "strategy": "dual_mom",
        },
    }


def _archive_review_run(
    *,
    archive_root: Path,
    temp_root: Path,
    timestamp: str,
    identity: str,
    execution_plan: dict[str, object],
    include_review_markdown: bool = False,
    manifest_fields: dict[str, object] | None = None,
) -> None:
    signal = execution_plan["signal"]
    source = execution_plan["source"]
    source_artifacts: dict[str, Path] = {}
    if include_review_markdown:
        review_markdown = temp_root / f"{identity}_execution_plan.md"
        review_markdown.write_text(f"# Review for {identity}\n", encoding="utf-8")
        source_artifacts["execution_plan_markdown"] = review_markdown

    write_run_archive(
        timestamp=timestamp,
        run_kind="execution_plan",
        mode="managed_sleeve",
        label=str(source["label"]),
        identity_parts=[identity],
        manifest_fields={
            "strategy": signal["strategy"],
            "symbol": signal["symbol"],
            "action": signal["action"],
            "target_shares": signal["target_shares"],
            "effective_capital": execution_plan["sizing"]["effective_capital_used"],
            "buying_power_available": execution_plan["broker_snapshot"]["buying_power"],
            "plan_sha256": identity,
            "source": source,
            **(manifest_fields or {}),
        },
        source_artifacts=source_artifacts,
        json_artifacts={"execution_plan_json": execution_plan},
        preferred_root=archive_root,
    )


def test_load_review_runs_uses_manifest_scan_and_falls_back_to_artifact_fields(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    execution_plan = _execution_plan_payload(leverage=0.94, vol_target=0.12)

    archived = write_run_archive(
        timestamp="2026-03-11T15:47:32-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_core",
        identity_parts=["event-1", "plan-1"],
        manifest_fields={
            "strategy": "dual_mom",
            "symbol": "EFA",
            "action": "BUY",
            "warnings": ["warning_from_manifest"],
        },
        json_artifacts={
            "execution_plan_json": execution_plan,
        },
        preferred_root=archive_root,
    )
    (archive_root / "index" / "runs.jsonl").unlink()

    resolved_root, runs = load_review_runs(limit=10, root_dir=archive_root)
    assert resolved_root == archive_root
    assert len(runs) == 1

    summary = summarize_run(runs[0])
    assert summary["target_shares"] == 100
    assert summary["leverage"] == 0.94
    assert summary["vol_target"] == 0.12
    assert summary["source_label"] == "dual_mom_core"
    assert summary["buying_power_available"] == 2455.99
    assert summary["effective_capital"] == 2455.99
    assert runs[0].manifest_path == archived.paths.manifest_path
    assert runs[0].warnings() == [
        "warning_from_manifest",
        "warning_from_plan",
        "warning_from_trade",
    ]
    assert runs[0].proposed_trades()[0]["quantity"] == 24


def test_load_review_runs_tolerates_missing_or_malformed_artifacts(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archived = write_run_archive(
        timestamp="2026-03-11T15:47:32-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_core",
        identity_parts=["event-1", "plan-1"],
        manifest_fields={
            "strategy": "dual_mom",
            "symbol": "EFA",
            "action": "BUY",
            "target_shares": 100,
            "plan_sha256": "abc123",
        },
        preferred_root=archive_root,
    )
    manifest = json.loads(archived.paths.manifest_path.read_text(encoding="utf-8"))
    manifest["artifact_paths"] = {
        "execution_plan_json": "artifacts/execution_plan_json.json",
    }
    archived.paths.manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (archived.paths.run_dir / "artifacts" / "execution_plan_json.json").write_text("{broken", encoding="utf-8")

    _, runs = load_review_runs(limit=10, root_dir=archive_root)
    assert len(runs) == 1

    summary = summarize_run(runs[0])
    assert summary["target_shares"] == 100
    assert summary["plan_sha256"] == "abc123"
    assert runs[0].proposed_trades() == []
    artifact_rows = build_artifact_rows(runs[0])
    assert artifact_rows[0]["artifact"] == "manifest"
    assert any(row["artifact"] == "execution_plan_json" for row in artifact_rows)


def test_build_history_and_comparison_rows_include_changed_trade_and_key_fields(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    first_plan = _execution_plan_payload(quantity=24, effective_capital=2455.99, buying_power=2455.99)
    second_plan = _execution_plan_payload(quantity=10, effective_capital=1000.0, buying_power=1000.0)

    write_run_archive(
        timestamp="2026-03-11T15:47:32-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_core",
        identity_parts=["event-1", "plan-1"],
        manifest_fields={
            "strategy": "dual_mom",
            "symbol": "EFA",
            "action": "BUY",
            "target_shares": 100,
            "effective_capital": 2455.99,
            "buying_power_available": 2455.99,
            "plan_sha256": "plan-1",
        },
        json_artifacts={"execution_plan_json": first_plan},
        preferred_root=archive_root,
    )
    write_run_archive(
        timestamp="2026-03-11T15:48:32-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_core",
        identity_parts=["event-2", "plan-2"],
        manifest_fields={
            "strategy": "dual_mom",
            "symbol": "EFA",
            "action": "BUY",
            "target_shares": 100,
            "effective_capital": 1000.0,
            "buying_power_available": 1000.0,
            "plan_sha256": "plan-2",
        },
        json_artifacts={"execution_plan_json": second_plan},
        preferred_root=archive_root,
    )

    _, runs = load_review_runs(limit=10, root_dir=archive_root)
    history_rows = build_run_history_rows(runs)
    comparison_rows = build_run_comparison_rows(runs[0], runs[1])

    assert len(history_rows) == 2
    assert history_rows[0]["trade_count"] == 1
    assert history_rows[0]["warning_count"] == 2
    changed_fields = {row["field"] for row in comparison_rows}
    assert "effective_capital" in changed_fields
    assert "buying_power_available" in changed_fields
    assert "plan_sha256" in changed_fields
    assert "proposed_trades" in changed_fields


def test_build_needs_review_rows_flags_warnings_blockers_and_missing_review(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    execution_plan = _execution_plan_payload(
        warnings=["warning_from_plan"],
        blockers=["blocker_from_plan"],
        trade_warnings=["warning_from_trade"],
        trade_blockers=["blocker_from_trade"],
    )

    _archive_review_run(
        archive_root=archive_root,
        temp_root=tmp_path,
        timestamp="2026-03-11T15:47:32-05:00",
        identity="plan-needs-review",
        execution_plan=execution_plan,
        include_review_markdown=False,
        manifest_fields={
            "warnings": ["warning_from_manifest"],
            "blockers": ["blocker_from_manifest"],
        },
    )

    _, runs = load_review_runs(limit=10, root_dir=archive_root)
    rows = build_needs_review_rows(runs)

    assert [row["headline"] for row in rows] == [
        "Archived run contains blockers",
        "Archived run contains warnings",
        "New plan found; no review artifact detected",
    ]
    assert "blocker_from_manifest" in rows[0]["detail"]
    assert "blocker_from_trade" in rows[0]["detail"]
    assert "warning_from_manifest" in rows[1]["detail"]
    assert rows[2]["path"].endswith("execution_plan_json.json")
    assert rows[2]["compare_to_path"] == "-"


def test_build_needs_review_rows_flags_trade_and_capital_changes_vs_prior_comparable_run(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    first_plan = _execution_plan_payload(quantity=24, effective_capital=2455.99, buying_power=2455.99)
    second_plan = _execution_plan_payload(quantity=10, effective_capital=1000.0, buying_power=1000.0)

    _archive_review_run(
        archive_root=archive_root,
        temp_root=tmp_path,
        timestamp="2026-03-11T15:47:32-05:00",
        identity="plan-older",
        execution_plan=first_plan,
        include_review_markdown=True,
    )
    _archive_review_run(
        archive_root=archive_root,
        temp_root=tmp_path,
        timestamp="2026-03-11T15:48:32-05:00",
        identity="plan-newer",
        execution_plan=second_plan,
        include_review_markdown=True,
    )

    _, runs = load_review_runs(limit=10, root_dir=archive_root)
    rows = build_needs_review_rows(runs)
    headlines = [row["headline"] for row in rows]

    assert "New execution plan with trade changes vs prior comparable run" in headlines
    assert "Capital allocation changed from prior comparable run" in headlines
    trade_row = next(row for row in rows if row["headline"] == "New execution plan with trade changes vs prior comparable run")
    capital_row = next(row for row in rows if row["headline"] == "Capital allocation changed from prior comparable run")
    assert trade_row["compare_to_run_id"] == runs[1].run_id
    assert "BUY 10 EFA" in trade_row["detail"]
    assert "BUY 24 EFA" in trade_row["detail"]
    assert capital_row["compare_to_path"].endswith("execution_plan_json.json")
    assert "effective_capital: 2,455.99 -> 1,000" in capital_row["detail"]
    assert "estimated_notional: 2,379.84 -> 991.6" in capital_row["detail"]


def test_build_recent_activity_rows_orders_newest_first_and_includes_paths(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    older_plan = _execution_plan_payload(source_label="dual_mom_core")
    newer_plan = _execution_plan_payload(source_label="dual_mom_core_vt", warnings=[], trade_warnings=[])

    _archive_review_run(
        archive_root=archive_root,
        temp_root=tmp_path,
        timestamp="2026-03-11T15:47:32-05:00",
        identity="plan-older",
        execution_plan=older_plan,
        include_review_markdown=True,
    )
    _archive_review_run(
        archive_root=archive_root,
        temp_root=tmp_path,
        timestamp="2026-03-11T15:49:32-05:00",
        identity="plan-newer",
        execution_plan=newer_plan,
        include_review_markdown=True,
    )

    _, runs = load_review_runs(limit=10, root_dir=archive_root)
    rows = build_recent_activity_rows(runs, limit=10)

    assert [row["label"] for row in rows] == ["dual_mom_core_vt", "dual_mom_core"]
    assert rows[0]["artifact_type"] == "execution_plan_markdown"
    assert rows[0]["path"].endswith("execution_plan_markdown__plan-newer_execution_plan.md")
    assert "execution_plan_json.json" in rows[0]["related_paths"]
    assert rows[0]["status"] == "Archived run with 1 proposed trade"
