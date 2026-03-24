from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from scripts import live_canary_state_ops
from trading_codex.execution.live_canary import (
    finalize_live_canary_event,
    finalize_live_canary_session,
    live_canary_event_state_path,
    live_canary_session_state_path,
)
from trading_codex.execution.live_canary_readiness import build_live_canary_readiness
from trading_codex.execution.live_canary import DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS


ACCOUNT_ID = "5WT00001"
AS_OF = "2026-03-23T10:40:00-04:00"
TIMESTAMP = "2026-03-23T10:45:00-04:00"
SIGNAL_DATE = "2026-03-20"
STRATEGY = "dual_mom_vol10_cash"


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _timestamp(value: str = TIMESTAMP) -> datetime:
    return datetime.fromisoformat(value)


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


def _signal_payload(
    *,
    date: str = SIGNAL_DATE,
    strategy: str = STRATEGY,
    action: str = "RESIZE",
    symbol: str = "EFA",
    price: float | None = 99.16,
    target_shares: int = 100,
    resize_prev_shares: int | None = 82,
    resize_new_shares: int | None = 100,
    next_rebalance: str | None = "2026-03-31",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "date": date,
        "strategy": strategy,
        "action": action,
        "symbol": symbol,
        "price": price,
        "target_shares": target_shares,
        "resize_prev_shares": resize_prev_shares,
        "resize_new_shares": resize_new_shares,
        "next_rebalance": next_rebalance,
    }
    payload["event_id"] = _event_id(payload)
    return payload


def _broker_snapshot(
    *positions: dict[str, object],
    account_id: str = ACCOUNT_ID,
    as_of: str | None = AS_OF,
    buying_power: float | None = 20_000.0,
    cash: float | None = None,
) -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": account_id,
        "as_of": as_of,
        "buying_power": buying_power,
        "cash": cash,
        "positions": list(positions),
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_signal_file(tmp_path: Path, payload: dict[str, object]) -> Path:
    return _write_json(tmp_path / "signal.json", payload)


def _write_positions_file(tmp_path: Path, payload: dict[str, object]) -> Path:
    return _write_json(tmp_path / "positions.json", payload)


def _seed_event_state(base_dir: Path, signal_payload: dict[str, object]) -> Path:
    event_path = live_canary_event_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        event_id=str(signal_payload["event_id"]),
    )
    finalize_live_canary_event(
        state_path=event_path,
        record={
            "account_id": ACCOUNT_ID,
            "decision": "live_submit_refused",
            "event_id": signal_payload["event_id"],
            "generated_at_chicago": TIMESTAMP,
            "manual_clearance_required": True,
            "response_text": "submitted",
            "result": "submitted",
        },
    )
    return event_path


def _seed_session_state(base_dir: Path, signal_payload: dict[str, object]) -> Path:
    session_path = live_canary_session_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        strategy=str(signal_payload["strategy"]),
        signal_date=str(signal_payload["date"]),
    )
    finalize_live_canary_session(
        state_path=session_path,
        record={
            "account_id": ACCOUNT_ID,
            "claimed_at_chicago": TIMESTAMP,
            "decision": "live_submit_refused",
            "event_id": signal_payload["event_id"],
            "generated_at_chicago": TIMESTAMP,
            "manual_clearance_required": True,
            "response_text": "submitted",
            "result": "submitted",
            "signal_date": signal_payload["date"],
            "strategy": signal_payload["strategy"],
            "updated_at_chicago": TIMESTAMP,
        },
    )
    return session_path


def _seed_legacy_submit_tracking(base_dir: Path, *, fingerprint: str = "legacy-fingerprint-001") -> tuple[Path, Path]:
    ledger_path = base_dir / "broker_live_submission_fingerprints.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "account_id": ACCOUNT_ID,
                "generated_at_chicago": TIMESTAMP,
                "live_submission_fingerprint": fingerprint,
                "manual_clearance_required": True,
                "plan_sha256": "legacy-plan-sha",
                "result": "ambiguous_attempted_submit_manual_clearance_required",
                "submission_succeeded": False,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    claim_path = base_dir / "claims" / f"{fingerprint}.json"
    claim_path.parent.mkdir(parents=True, exist_ok=True)
    claim_path.write_text(
        json.dumps(
            {
                "claim_path": str(claim_path),
                "generated_at_chicago": TIMESTAMP,
                "live_submission_fingerprint": fingerprint,
                "manual_clearance_required": True,
                "plan_sha256": "legacy-plan-sha",
                "result": "claim_pending_manual_clearance_required",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return ledger_path, claim_path


def _run_readiness_cli(
    capsys: pytest.CaptureFixture[str],
    *,
    base_dir: Path | None,
    signal_path: Path,
    positions_path: Path | None,
    account_id: str = ACCOUNT_ID,
    arm_live_canary: str | None = ACCOUNT_ID,
    timestamp: str = TIMESTAMP,
    emit: str = "json",
) -> tuple[int, str, str]:
    argv = [
        "--emit",
        emit,
        "--timestamp",
        timestamp,
        "readiness",
        "--signal-json-file",
        str(signal_path),
        "--broker",
        "file",
        "--account-id",
        account_id,
    ]
    if positions_path is not None:
        argv.extend(["--positions-file", str(positions_path)])
    if arm_live_canary is not None:
        argv.extend(["--arm-live-canary", arm_live_canary])
    if base_dir is not None:
        argv.extend(["--base-dir", str(base_dir)])

    result = live_canary_state_ops.main(argv)
    captured = capsys.readouterr()
    return result, captured.out, captured.err


def test_live_canary_readiness_ready_path(tmp_path: Path) -> None:
    signal_path = _write_signal_file(tmp_path, _signal_payload())
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        ),
    )

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(),
    )

    assert payload["verdict"] == "ready"
    assert payload["blocking_reasons"] == []
    assert payload["evaluation"]["decision"] == "ready_live_submit"
    assert payload["next_actions"][0]["action_id"] == "run_live_canary_submit"
    assert payload["state_status"]["summary"]["blocking_artifact_count"] == 0


def test_live_canary_readiness_fail_closed_when_positions_file_is_missing(tmp_path: Path) -> None:
    signal_path = _write_signal_file(tmp_path, _signal_payload())
    missing_positions = tmp_path / "missing_positions.json"

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=missing_positions,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(),
    )

    assert payload["verdict"] == "not_ready"
    assert payload["gates"][0]["gate"] == "input_readiness"
    assert payload["gates"][0]["status"] == "fail"
    assert payload["blocking_reasons"][0].startswith("live_canary_broker_snapshot_load_error:")
    assert payload["gates"][3]["status"] == "not_assessed"


def test_live_canary_readiness_manual_arming_blocker(tmp_path: Path) -> None:
    signal_path = _write_signal_file(tmp_path, _signal_payload())
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}),
    )

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=None,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(),
    )

    manual_gate = next(gate for gate in payload["gates"] if gate["gate"] == "manual_arming")
    assert payload["verdict"] == "not_ready"
    assert manual_gate["status"] == "fail"
    assert manual_gate["blocking_reasons"] == ["live_canary_not_armed"]
    assert any(action["action_id"] == "arm_live_canary" for action in payload["next_actions"])


def test_live_canary_readiness_account_binding_blocker(tmp_path: Path) -> None:
    signal_path = _write_signal_file(tmp_path, _signal_payload())
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id="5WT99999",
        ),
    )

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(),
    )

    account_gate = next(gate for gate in payload["gates"] if gate["gate"] == "account_binding")
    assert payload["verdict"] == "not_ready"
    assert account_gate["status"] == "fail"
    assert account_gate["blocking_reasons"] == ["live_canary_account_binding_mismatch"]


def test_live_canary_readiness_stale_data_blocker(tmp_path: Path) -> None:
    signal_path = _write_signal_file(tmp_path, _signal_payload())
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            as_of="2026-03-23T10:25:00-04:00",
        ),
    )

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(),
    )

    session_gate = next(gate for gate in payload["gates"] if gate["gate"] == "session_readiness")
    assert payload["verdict"] == "not_ready"
    assert "live_canary_broker_snapshot_stale:1200:900" in session_gate["blocking_reasons"]


@pytest.mark.parametrize(
    ("timestamp", "signal_date", "as_of", "expected_blocker"),
    [
        ("2026-03-23T09:29:59-04:00", SIGNAL_DATE, "2026-03-23T09:25:00-04:00", "live_canary_submit_outside_regular_session"),
        ("2026-07-03T10:45:00-04:00", "2026-07-02", "2026-07-03T10:40:00-04:00", "live_canary_submit_market_holiday:2026-07-03"),
    ],
    ids=["outside_session", "market_holiday"],
)
def test_live_canary_readiness_non_session_and_holiday_blockers(
    tmp_path: Path,
    timestamp: str,
    signal_date: str,
    as_of: str,
    expected_blocker: str,
) -> None:
    signal_path = _write_signal_file(tmp_path, _signal_payload(date=signal_date))
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            as_of=as_of,
        ),
    )

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(timestamp),
    )

    session_gate = next(gate for gate in payload["gates"] if gate["gate"] == "session_readiness")
    assert payload["verdict"] == "not_ready"
    assert expected_blocker in session_gate["blocking_reasons"]


@pytest.mark.parametrize(
    ("signal_payload", "snapshot_payload", "gate_name", "expected_blocker"),
    [
        (
            _signal_payload(),
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                buying_power=50.0,
            ),
            "affordability",
            "live_canary_buy_order_notional_exceeds_available:EFA:99.16:50.00",
        ),
        (
            _signal_payload(),
            _broker_snapshot(
                {"symbol": "BIL", "shares": 7, "price": 91.20, "instrument_type": "Equity"},
            ),
            "canary_order_readiness",
            "live_canary_existing_position_exceeds_cap:BIL:7:1",
        ),
    ],
    ids=["affordability", "position_cap"],
)
def test_live_canary_readiness_affordability_and_canary_size_blockers(
    tmp_path: Path,
    signal_payload: dict[str, object],
    snapshot_payload: dict[str, object],
    gate_name: str,
    expected_blocker: str,
) -> None:
    signal_path = _write_signal_file(tmp_path, signal_payload)
    positions_path = _write_positions_file(tmp_path, snapshot_payload)

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=tmp_path / "live_canary",
        timestamp=_timestamp(),
    )

    gate = next(gate for gate in payload["gates"] if gate["gate"] == gate_name)
    assert payload["verdict"] == "not_ready"
    assert expected_blocker in gate["blocking_reasons"]


def test_live_canary_readiness_duplicate_and_session_state_blockers(tmp_path: Path) -> None:
    signal_payload = _signal_payload()
    signal_path = _write_signal_file(tmp_path, signal_payload)
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}),
    )
    base_dir = tmp_path / "live_canary"
    _seed_event_state(base_dir, signal_payload)
    _seed_session_state(base_dir, signal_payload)

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=base_dir,
        timestamp=_timestamp(),
    )

    duplicate_gate = next(gate for gate in payload["gates"] if gate["gate"] == "duplicate_state")
    assert payload["verdict"] == "not_ready"
    assert duplicate_gate["status"] == "fail"
    assert {
        artifact["artifact_kind"]
        for artifact in duplicate_gate["details"]["blocking_artifacts"]
    } == {"event_state", "session_state"}
    assert any(action["action_id"].startswith("clear_event_state:") for action in payload["next_actions"])
    assert any(action["action_id"].startswith("clear_session_state:") for action in payload["next_actions"])


def test_live_canary_readiness_surfaces_legacy_operator_blockers(tmp_path: Path) -> None:
    signal_payload = _signal_payload()
    signal_path = _write_signal_file(tmp_path, signal_payload)
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}),
    )
    base_dir = tmp_path / "live_canary"
    ledger_path, claim_path = _seed_legacy_submit_tracking(base_dir)

    payload = build_live_canary_readiness(
        signal_json_file=signal_path,
        broker="file",
        positions_file=positions_path,
        account_id=ACCOUNT_ID,
        arm_live_canary=ACCOUNT_ID,
        base_dir=base_dir,
        timestamp=_timestamp(),
    )

    operator_gate = next(gate for gate in payload["gates"] if gate["gate"] == "operator_state_ops")
    assert payload["verdict"] == "not_ready"
    assert operator_gate["status"] == "fail"
    assert {artifact["path"] for artifact in payload["state_status"]["blocking_artifacts"]} == {
        str(claim_path),
        str(ledger_path),
    }
    assert {
        artifact["scope_precision"]
        for artifact in payload["state_status"]["blocking_artifacts"]
    } == {"legacy_unscoped"}
    assert any(action["action_id"].startswith("inspect_legacy_submit_tracking:") for action in payload["next_actions"])


def test_live_canary_readiness_cli_json_and_text_are_stable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    signal_payload = _signal_payload()
    signal_path = _write_signal_file(tmp_path, signal_payload)
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}),
    )
    base_dir = tmp_path / "live_canary"
    _seed_session_state(base_dir, signal_payload)

    json_result, json_stdout, json_stderr = _run_readiness_cli(
        capsys,
        base_dir=base_dir,
        signal_path=signal_path,
        positions_path=positions_path,
        arm_live_canary=None,
        emit="json",
    )
    assert json_result == 0
    assert json_stderr == ""
    payload = json.loads(json_stdout)
    assert payload["schema_name"] == "live_canary_readiness"
    assert payload["verdict"] == "not_ready"
    assert payload["summary"]["blocking_reason_count"] >= 2

    text_result, text_stdout, text_stderr = _run_readiness_cli(
        capsys,
        base_dir=base_dir,
        signal_path=signal_path,
        positions_path=positions_path,
        arm_live_canary=None,
        emit="text",
    )
    assert text_result == 0
    assert text_stderr == ""
    assert "Verdict not_ready" in text_stdout
    assert "Blocking reasons:" in text_stdout
    assert "Next actions:" in text_stdout
    assert "Gates:" in text_stdout
    assert "manual_arming=fail" in text_stdout
    assert "duplicate_state=fail" in text_stdout


def test_live_canary_readiness_cli_smoke_subprocess(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = _write_signal_file(tmp_path, _signal_payload())
    positions_path = _write_positions_file(
        tmp_path,
        _broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}),
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_state_ops.py"),
            "--emit",
            "json",
            "--timestamp",
            TIMESTAMP,
            "readiness",
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--account-id",
            ACCOUNT_ID,
            "--arm-live-canary",
            ACCOUNT_ID,
            "--base-dir",
            str(tmp_path / "live_canary"),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["schema_name"] == "live_canary_readiness"
    assert payload["verdict"] == "ready"
