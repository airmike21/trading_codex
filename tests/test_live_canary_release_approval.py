from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from scripts import live_canary_state_ops
from tests.live_canary_approval_helpers import seed_release_approval, write_shadow_rehearsal_bundle
from trading_codex.execution.live_canary import live_canary_release_approval_path
from trading_codex.execution.live_canary_readiness import build_live_canary_readiness
from trading_codex.execution.live_canary_state_ops import build_live_canary_state_status


ACCOUNT_ID = "5WT00001"
TIMESTAMP = "2026-03-23T10:45:00-04:00"
SIGNAL_DATE = "2026-03-20"
STRATEGY = "dual_mom_vol10_cash"


def _event_id(payload: dict[str, object]) -> str:
    def s(value: object) -> str:
        return "" if value is None else str(value)

    return ":".join(
        [
            s(payload.get("date")),
            s(payload.get("strategy")),
            s(payload.get("action")),
            s(payload.get("symbol")),
            s(payload.get("target_shares")),
            s(payload.get("resize_new_shares")),
            s(payload.get("next_rebalance")),
        ]
    )


def _signal_payload() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "date": SIGNAL_DATE,
        "strategy": STRATEGY,
        "action": "RESIZE",
        "symbol": "EFA",
        "price": 99.16,
        "target_shares": 100,
        "resize_prev_shares": 82,
        "resize_new_shares": 100,
        "next_rebalance": "2026-03-31",
    }
    payload["event_id"] = _event_id(payload)
    return payload


def _broker_snapshot() -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": ACCOUNT_ID,
        "as_of": "2026-03-23T10:40:00-04:00",
        "buying_power": 20_000.0,
        "cash": 20_000.0,
        "positions": [
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        ],
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_signal_and_positions(tmp_path: Path, signal_payload: dict[str, object]) -> tuple[Path, Path]:
    signal_path = _write_json(tmp_path / "signal.json", signal_payload)
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot())
    return signal_path, positions_path


def _approval_path(base_dir: Path, signal_payload: dict[str, object]) -> Path:
    return live_canary_release_approval_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        event_id=str(signal_payload["event_id"]),
    )


def test_live_canary_state_ops_approve_apply_writes_artifact_and_status_surfaces_it(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    signal_payload = _signal_payload()
    signal_path, positions_path = _write_signal_and_positions(tmp_path, signal_payload)
    base_dir = tmp_path / "live_canary"
    bundle_dir = write_shadow_rehearsal_bundle(
        bundle_dir=tmp_path / "shadow_bundle",
        account_id=ACCOUNT_ID,
        signal_payload=signal_payload,
    )

    result = live_canary_state_ops.main(
        [
            "--emit",
            "json",
            "--timestamp",
            TIMESTAMP,
            "--base-dir",
            str(base_dir),
            "approve",
            "--account-id",
            ACCOUNT_ID,
            "--bundle-dir",
            str(bundle_dir),
            "--operator",
            "ops-user",
            "--reason",
            "shadow bundle reviewed",
            "--apply",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert result == 0
    assert payload["schema_name"] == "live_canary_release_approval_operation"
    assert payload["apply"] is True
    approval_path = Path(payload["approval_path"])
    assert approval_path.exists()
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    assert approval_payload["account_id"] == ACCOUNT_ID
    assert approval_payload["event_id"] == signal_payload["event_id"]
    assert approval_payload["operator_id"] == "ops-user"
    assert approval_payload["reason"] == "shadow bundle reviewed"
    assert approval_payload["artifact_paths"]["signal_json"] == str(bundle_dir / "signal.json")

    status_payload = build_live_canary_state_status(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        event_id=signal_payload["event_id"],
    )
    assert status_payload["release_approval"]["present"] is True
    assert status_payload["release_approval"]["valid"] is True
    assert status_payload["release_approval"]["approval_path"] == str(approval_path)

    readiness_payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=base_dir,
        timestamp=datetime.fromisoformat(TIMESTAMP),
    )
    assert readiness_payload["verdict"] == "ready"
    assert next(gate for gate in readiness_payload["gates"] if gate["gate"] == "pre_live_approval")["status"] == "pass"


@pytest.mark.parametrize(
    ("field_name", "replacement", "expected_blocker"),
    [
        ("account_id", "5WT99999", "live_canary_pre_live_approval_account_mismatch:5WT99999:5WT00001"),
        (
            "event_id",
            "2026-03-20:dual_mom_vol10_cash:RESIZE:EFA:100:99:2026-03-31",
            "live_canary_pre_live_approval_event_id_mismatch:2026-03-20:dual_mom_vol10_cash:RESIZE:EFA:100:99:2026-03-31:2026-03-20:dual_mom_vol10_cash:RESIZE:EFA:100:100:2026-03-31",
        ),
        ("strategy", "other_strategy", "live_canary_pre_live_approval_strategy_mismatch:other_strategy:dual_mom_vol10_cash"),
        ("signal_date", "2026-03-19", "live_canary_pre_live_approval_signal_date_mismatch:2026-03-19:2026-03-20"),
    ],
)
def test_live_canary_readiness_rejects_mismatched_release_approval_fields(
    tmp_path: Path,
    field_name: str,
    replacement: str,
    expected_blocker: str,
) -> None:
    signal_payload = _signal_payload()
    signal_path, positions_path = _write_signal_and_positions(tmp_path, signal_payload)
    base_dir = tmp_path / "live_canary"
    seed_release_approval(
        live_canary_base_dir=base_dir,
        bundle_dir=tmp_path / "shadow_bundle",
        account_id=ACCOUNT_ID,
        signal_payload=signal_payload,
        timestamp=TIMESTAMP,
    )

    approval_path = _approval_path(base_dir, signal_payload)
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload[field_name] = replacement
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    readiness_payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=base_dir,
        timestamp=datetime.fromisoformat(TIMESTAMP),
    )

    approval_gate = next(gate for gate in readiness_payload["gates"] if gate["gate"] == "pre_live_approval")
    assert approval_gate["status"] == "fail"
    assert expected_blocker in approval_gate["blocking_reasons"]


def test_live_canary_readiness_rejects_stale_release_approval_when_bundle_contents_change(tmp_path: Path) -> None:
    signal_payload = _signal_payload()
    signal_path, positions_path = _write_signal_and_positions(tmp_path, signal_payload)
    base_dir = tmp_path / "live_canary"
    bundle_dir = tmp_path / "shadow_bundle"
    seed_release_approval(
        live_canary_base_dir=base_dir,
        bundle_dir=bundle_dir,
        account_id=ACCOUNT_ID,
        signal_payload=signal_payload,
        timestamp=TIMESTAMP,
    )

    summary_path = bundle_dir / "summary.md"
    summary_path.write_text(summary_path.read_text(encoding="utf-8") + "\nreviewed again\n", encoding="utf-8")

    readiness_payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=base_dir,
        timestamp=datetime.fromisoformat(TIMESTAMP),
    )

    approval_gate = next(gate for gate in readiness_payload["gates"] if gate["gate"] == "pre_live_approval")
    assert approval_gate["status"] == "fail"
    assert "live_canary_pre_live_approval_stale:summary_md" in approval_gate["blocking_reasons"]


@pytest.mark.parametrize(
    ("artifact_name", "malformed"),
    [
        ("reconcile.json", False),
        ("launch.json", True),
    ],
    ids=["missing_artifact", "malformed_artifact"],
)
def test_live_canary_state_ops_approve_rejects_missing_or_malformed_rehearsal_artifacts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    artifact_name: str,
    malformed: bool,
) -> None:
    signal_payload = _signal_payload()
    bundle_dir = write_shadow_rehearsal_bundle(
        bundle_dir=tmp_path / "shadow_bundle",
        account_id=ACCOUNT_ID,
        signal_payload=signal_payload,
    )
    artifact_path = bundle_dir / artifact_name
    if malformed:
        artifact_path.write_text("{not-json", encoding="utf-8")
    else:
        artifact_path.unlink()

    result = live_canary_state_ops.main(
        [
            "--emit",
            "json",
            "--timestamp",
            TIMESTAMP,
            "--base-dir",
            str(tmp_path / "live_canary"),
            "approve",
            "--account-id",
            ACCOUNT_ID,
            "--bundle-dir",
            str(bundle_dir),
            "--apply",
        ]
    )
    captured = capsys.readouterr()

    assert result == 2
    if malformed:
        assert "invalid_bundle_artifact:launch_json:" in captured.err
    else:
        assert "missing_bundle_artifact:reconcile_json" in captured.err
