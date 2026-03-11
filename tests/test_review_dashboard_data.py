from __future__ import annotations

import json
from pathlib import Path

from trading_codex.review_dashboard_data import (
    build_artifact_rows,
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
) -> dict[str, object]:
    return {
        "schema_name": "execution_plan",
        "schema_version": 2,
        "generated_at_chicago": "2026-03-11T15:47:32-05:00",
        "warnings": ["warning_from_plan"],
        "blockers": [],
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
            "account_id": "paper-1",
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
                "warnings": ["warning_from_trade"],
                "blockers": [],
            }
        ],
        "live_submission_preview": {
            "broker_account_id": "paper-1",
            "effective_capital_used": effective_capital,
            "event_id": f"2026-03-11:dual_mom:{action}:{symbol}:{target_shares}:{resize_new_shares or ''}:2026-03-31",
            "rebalance_date": "2026-03-31",
            "strategy": "dual_mom",
        },
    }


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
