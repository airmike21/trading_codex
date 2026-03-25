from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.execution.live_canary_state_ops import apply_live_canary_release_approval


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_shadow_rehearsal_bundle(
    *,
    bundle_dir: Path,
    account_id: str,
    signal_payload: dict[str, object],
) -> Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    signal_path = bundle_dir / "signal.json"
    readiness_path = bundle_dir / "readiness.json"
    launch_path = bundle_dir / "launch.json"
    reconcile_path = bundle_dir / "reconcile.json"
    summary_path = bundle_dir / "summary.md"

    _write_json(signal_path, dict(signal_payload))
    _write_json(
        readiness_path,
        {
            "schema_name": "live_canary_readiness",
            "schema_version": 1,
            "verdict": "ready",
            "scope": {
                "account_id": account_id,
                "event_id": signal_payload["event_id"],
                "signal_date": signal_payload["date"],
                "strategy": signal_payload["strategy"],
            },
            "gates": [
                {
                    "gate": "pre_live_approval",
                    "status": "not_assessed",
                    "blocking_reasons": [],
                    "warnings": [],
                    "details": {
                        "approval_required": False,
                    },
                }
            ],
        },
    )
    _write_json(
        launch_path,
        {
            "schema_name": "live_canary_launch_result",
            "schema_version": 1,
            "requested_live_submit": False,
            "submit_path_invoked": False,
            "submit_outcome": "not_requested",
            "event_context": {
                "account_id": account_id,
                "event_id": signal_payload["event_id"],
                "signal_date": signal_payload["date"],
                "strategy": signal_payload["strategy"],
                "symbol": signal_payload["symbol"],
                "action": signal_payload["action"],
            },
        },
    )
    _write_json(
        reconcile_path,
        {
            "schema_name": "live_canary_reconciliation_result",
            "schema_version": 1,
            "verdict": "not_applicable",
            "mode": "preview_only",
            "context": {
                "account_id": account_id,
                "event_id": signal_payload["event_id"],
                "signal_date": signal_payload["date"],
                "strategy": signal_payload["strategy"],
            },
        },
    )
    summary_path.write_text(
        "\n".join(
            [
                "# Live Canary Shadow Rehearsal",
                "",
                f"Event ID: `{signal_payload['event_id']}`",
                "Readiness verdict: `ready`",
                "Launch outcome: `not_requested`",
                "Reconcile verdict: `not_applicable`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return bundle_dir


def seed_release_approval(
    *,
    live_canary_base_dir: Path,
    bundle_dir: Path,
    account_id: str,
    signal_payload: dict[str, object],
    timestamp: str,
) -> dict[str, Any]:
    write_shadow_rehearsal_bundle(
        bundle_dir=bundle_dir,
        account_id=account_id,
        signal_payload=signal_payload,
    )
    return apply_live_canary_release_approval(
        base_dir=live_canary_base_dir,
        account_id=account_id,
        bundle_dir=bundle_dir,
        timestamp=datetime.fromisoformat(timestamp),
    )
