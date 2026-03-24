from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import live_canary_guardrails, live_canary_state_ops
from trading_codex.execution import live_canary_readiness as live_canary_readiness_module
from trading_codex.execution import parse_broker_snapshot
from trading_codex.execution.live_canary import (
    finalize_live_canary_event,
    finalize_live_canary_session,
    live_canary_event_state_path,
    live_canary_session_state_path,
)


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


def _seed_event_state(base_dir: Path, signal_payload: dict[str, object]) -> None:
    event_path = live_canary_event_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        event_id=str(signal_payload["event_id"]),
    )
    finalize_live_canary_event(
        state_path=event_path,
        record={
            "account_id": ACCOUNT_ID,
            "decision": "live_submitted",
            "event_id": signal_payload["event_id"],
            "generated_at_chicago": TIMESTAMP,
            "manual_clearance_required": False,
            "response_text": "submitted",
            "result": "submitted",
        },
    )


def _seed_session_state(base_dir: Path, signal_payload: dict[str, object]) -> None:
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
            "decision": "live_submitted",
            "event_id": signal_payload["event_id"],
            "generated_at_chicago": TIMESTAMP,
            "manual_clearance_required": False,
            "response_text": "submitted",
            "result": "submitted",
            "signal_date": signal_payload["date"],
            "strategy": signal_payload["strategy"],
            "updated_at_chicago": TIMESTAMP,
        },
    )


class _SnapshotSequenceAdapter:
    def __init__(
        self,
        *snapshots: dict[str, object],
        submit_result: object | None = None,
        submit_error: Exception | None = None,
    ) -> None:
        self._snapshots = [parse_broker_snapshot(snapshot) for snapshot in snapshots]
        self._submit_result = submit_result
        self._submit_error = submit_error
        self.load_calls = 0
        self.submit_calls = 0

    def load_snapshot(self):
        self.load_calls += 1
        if self.load_calls > len(self._snapshots):
            raise AssertionError("No broker snapshot prepared for this load.")
        return self._snapshots[self.load_calls - 1]

    def submit_live_orders(self, **kwargs: object):
        del kwargs
        self.submit_calls += 1
        if self._submit_error is not None:
            raise self._submit_error
        if self._submit_result is None:
            raise AssertionError("submit_live_orders should not be called in this test.")
        return self._submit_result


def _live_submission_response(
    *,
    attempted: bool,
    succeeded: bool,
    submission_result: str,
    refusal_reasons: list[str] | None = None,
    manual_clearance_required: bool = False,
    order_error: str | None = None,
    live_submission_fingerprint: str | None = None,
    duplicate_submit_refusal: dict[str, object] | None = None,
    durable_state: dict[str, object] | None = None,
    generated_at_chicago: str = "2026-03-23T10:45:30-04:00",
) -> SimpleNamespace:
    return SimpleNamespace(
        generated_at_chicago=generated_at_chicago,
        live_submit_attempted=attempted,
        manual_clearance_required=manual_clearance_required,
        orders=[
            SimpleNamespace(
                attempted=attempted,
                broker_order_id="order-123" if attempted and succeeded else None,
                broker_status="accepted" if succeeded else None,
                error=order_error,
                quantity=1,
                side="BUY",
                succeeded=succeeded,
                symbol="EFA",
            )
        ],
        refusal_reasons=list(refusal_reasons or []),
        live_submission_fingerprint=live_submission_fingerprint,
        duplicate_submit_refusal=duplicate_submit_refusal,
        durable_state=durable_state,
        submission_result=submission_result,
        submission_succeeded=succeeded,
    )


def _run_launch(
    capsys: pytest.CaptureFixture[str],
    *,
    tmp_path: Path,
    broker: str,
    signal_payload: dict[str, object] | None = None,
    positions_payload: dict[str, object] | None = None,
    account_id: str = ACCOUNT_ID,
    base_dir: Path | None = None,
    live_submit: bool = True,
    arm_live_canary: str | None = ACCOUNT_ID,
) -> tuple[int, dict[str, object], Path]:
    signal_path = _write_json(tmp_path / "signal.json", signal_payload or _signal_payload())
    positions_path = None
    if positions_payload is not None:
        positions_path = _write_json(tmp_path / "positions.json", positions_payload)

    resolved_base_dir = base_dir or (tmp_path / "live_canary")
    argv = [
        "--emit",
        "json",
        "--timestamp",
        TIMESTAMP,
        "launch",
        "--signal-json-file",
        str(signal_path),
        "--broker",
        broker,
        "--account-id",
        account_id,
        "--base-dir",
        str(resolved_base_dir),
    ]
    if positions_path is not None:
        argv.extend(["--positions-file", str(positions_path)])
    if live_submit:
        argv.append("--live-submit")
    if arm_live_canary is not None:
        argv.extend(["--arm-live-canary", arm_live_canary])

    result = live_canary_state_ops.main(argv)
    captured = capsys.readouterr()
    return result, json.loads(captured.out), resolved_base_dir


def test_live_canary_launch_live_submit_success_path_records_preflight_and_submit_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"
    signal_payload = _signal_payload()
    ready_snapshot = _broker_snapshot(
        {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        account_id=ACCOUNT_ID,
        as_of="2026-03-23T10:40:00-04:00",
    )
    refreshed_snapshot = _broker_snapshot(
        {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        account_id=ACCOUNT_ID,
        as_of="2026-03-23T10:44:00-04:00",
    )
    submit_result = _live_submission_response(
        attempted=True,
        succeeded=True,
        submission_result="submitted",
        live_submission_fingerprint="live-fingerprint-launch-success",
        durable_state={
            "claim_path": str(base_dir / "claims" / "live-fingerprint-launch-success.json"),
            "ledger_path": str(base_dir / "broker_live_submission_fingerprints.jsonl"),
            "lock_path": str(base_dir / "live_submission_state.lock"),
            "state_dir": str(base_dir),
        },
    )

    created_position_adapters: list[object] = []
    created_execution_adapters: list[_SnapshotSequenceAdapter] = []

    class FakeTastytradeBrokerPositionAdapter:
        def __init__(self, *, account_id: str, client: object | None = None) -> None:
            del account_id, client
            self.load_calls = 0
            created_position_adapters.append(self)

        def load_snapshot(self):
            self.load_calls += 1
            return parse_broker_snapshot(ready_snapshot)

    class FakeTastytradeBrokerExecutionAdapter(_SnapshotSequenceAdapter):
        def __init__(self, *, account_id: str, client: object | None = None) -> None:
            del account_id, client
            super().__init__(ready_snapshot, refreshed_snapshot, submit_result=submit_result)
            created_execution_adapters.append(self)

    monkeypatch.setattr(live_canary_readiness_module, "load_tastytrade_secrets", lambda *, secrets_file=None: None)
    monkeypatch.setattr(live_canary_guardrails, "load_tastytrade_secrets", lambda *, secrets_file=None: None)
    monkeypatch.setattr(
        live_canary_readiness_module,
        "TastytradeBrokerPositionAdapter",
        FakeTastytradeBrokerPositionAdapter,
    )
    monkeypatch.setattr(
        live_canary_guardrails,
        "TastytradeBrokerExecutionAdapter",
        FakeTastytradeBrokerExecutionAdapter,
    )

    result, payload, _resolved_base_dir = _run_launch(
        capsys,
        tmp_path=tmp_path,
        broker="tastytrade",
        signal_payload=signal_payload,
        base_dir=base_dir,
        live_submit=True,
        arm_live_canary=ACCOUNT_ID,
    )

    assert result == 0
    assert len(created_position_adapters) == 1
    assert len(created_execution_adapters) == 1
    assert created_position_adapters[0].load_calls == 1
    assert created_execution_adapters[0].load_calls == 2
    assert created_execution_adapters[0].submit_calls == 1
    assert payload["schema_name"] == "live_canary_launch_result"
    assert payload["readiness_verdict"] == "ready"
    assert payload["submit_path_invoked"] is True
    assert payload["submit_outcome"] == "live_submitted"
    assert payload["submit_result"]["decision"] == "live_submitted"
    assert payload["event_context"]["event_id"] == _event_id(signal_payload)
    assert payload["event_context"]["live_submission_fingerprint"] == "live-fingerprint-launch-success"
    assert payload["submit_result"]["event_id"] == _event_id(signal_payload)
    assert payload["submit_result"]["live_submission"]["live_submission_fingerprint"] == "live-fingerprint-launch-success"
    assert payload["artifact_paths"]["submit_audit_path"] == str(base_dir / "audit.jsonl")
    assert payload["artifact_paths"]["submit_claim_path"] == str(base_dir / "claims" / "live-fingerprint-launch-success.json")
    assert payload["artifact_paths"]["submit_ledger_path"] == str(base_dir / "broker_live_submission_fingerprints.jsonl")
    assert payload["artifact_paths"]["submit_lock_path"] == str(base_dir / "live_submission_state.lock")
    result_path = Path(payload["artifact_paths"]["result_path"])
    assert result_path.exists()
    stored = json.loads(result_path.read_text(encoding="utf-8"))
    assert stored == payload


def test_live_canary_launch_fails_closed_on_readiness_blockers_without_submit_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        live_canary_guardrails,
        "run_guardrails",
        lambda args: (_ for _ in ()).throw(AssertionError("guardrails should not be invoked")),
    )

    result, payload, _base_dir = _run_launch(
        capsys,
        tmp_path=tmp_path,
        broker="file",
        positions_payload=_broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id=ACCOUNT_ID,
            as_of="2026-03-23T10:25:00-04:00",
        ),
        live_submit=True,
        arm_live_canary=ACCOUNT_ID,
    )

    assert result == 2
    assert payload["readiness_verdict"] == "not_ready"
    assert payload["submit_path_invoked"] is False
    assert payload["submit_result"] is None
    assert payload["submit_outcome"] == "not_attempted_readiness_blocked"
    assert payload["readiness"]["blocking_reasons"] == ["live_canary_broker_snapshot_stale:1200:900"]
    stored = json.loads(Path(payload["artifact_paths"]["result_path"]).read_text(encoding="utf-8"))
    assert stored == payload


def test_live_canary_launch_malformed_state_fails_closed_without_submit_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"
    broken_session_path = base_dir / "sessions" / "broken.json"
    broken_session_path.parent.mkdir(parents=True, exist_ok=True)
    broken_session_path.write_text("{not-json", encoding="utf-8")

    monkeypatch.setattr(
        live_canary_guardrails,
        "run_guardrails",
        lambda args: (_ for _ in ()).throw(AssertionError("guardrails should not be invoked")),
    )

    result, payload, _resolved_base_dir = _run_launch(
        capsys,
        tmp_path=tmp_path,
        broker="file",
        positions_payload=_broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        ),
        base_dir=base_dir,
        live_submit=True,
        arm_live_canary=ACCOUNT_ID,
    )

    assert result == 2
    assert payload["readiness_verdict"] == "not_ready"
    assert payload["submit_path_invoked"] is False
    assert payload["submit_outcome"] == "not_attempted_readiness_blocked"
    blocker = payload["readiness"]["blocking_reasons"][0]
    assert blocker.startswith("live_canary_state_status_error:")
    assert "Expecting property name enclosed in double quotes" in blocker


@pytest.mark.parametrize(
    ("protection", "configured_account_id", "broker_account_id", "seed_kind", "expected_blocker"),
    [
        ("binding", "5WT99999", ACCOUNT_ID, None, "live_canary_account_binding_mismatch"),
        (
            "duplicate_event",
            ACCOUNT_ID,
            ACCOUNT_ID,
            "event",
            "existing event state blocks exact-event retries for this account/event_id",
        ),
        (
            "session_lock",
            ACCOUNT_ID,
            ACCOUNT_ID,
            "session",
            "existing session state blocks same-session retries for this account/strategy/signal_date",
        ),
    ],
)
def test_live_canary_launch_honors_existing_binding_duplicate_and_session_protections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    protection: str,
    configured_account_id: str,
    broker_account_id: str,
    seed_kind: str | None,
    expected_blocker: str,
) -> None:
    del protection
    signal_payload = _signal_payload()
    base_dir = tmp_path / "live_canary"
    if seed_kind == "event":
        _seed_event_state(base_dir, signal_payload)
    elif seed_kind == "session":
        _seed_session_state(base_dir, signal_payload)

    monkeypatch.setattr(
        live_canary_guardrails,
        "run_guardrails",
        lambda args: (_ for _ in ()).throw(AssertionError("guardrails should not be invoked")),
    )

    result, payload, _resolved_base_dir = _run_launch(
        capsys,
        tmp_path=tmp_path,
        broker="file",
        signal_payload=signal_payload,
        positions_payload=_broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id=broker_account_id,
        ),
        account_id=configured_account_id,
        base_dir=base_dir,
        live_submit=True,
        arm_live_canary=configured_account_id,
    )

    assert result == 2
    assert payload["readiness_verdict"] == "not_ready"
    assert payload["submit_path_invoked"] is False
    assert payload["submit_outcome"] == "not_attempted_readiness_blocked"
    assert expected_blocker in payload["readiness"]["blocking_reasons"]


def test_live_canary_launch_preserves_event_id_contract_when_no_submit_is_requested(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    signal_payload = _signal_payload(
        action="ROTATE",
        symbol="BIL",
        resize_prev_shares=None,
        resize_new_shares=None,
    )
    expected_event_id = _event_id(signal_payload)

    result, payload, _base_dir = _run_launch(
        capsys,
        tmp_path=tmp_path,
        broker="file",
        signal_payload=signal_payload,
        positions_payload=_broker_snapshot(
            {"symbol": "BIL", "shares": 0, "price": 91.20, "instrument_type": "Equity"},
        ),
        live_submit=False,
        arm_live_canary=ACCOUNT_ID,
    )

    assert result == 0
    assert payload["readiness_verdict"] == "ready"
    assert payload["submit_outcome"] == "not_requested"
    assert payload["submit_path_invoked"] is False
    assert payload["event_context"]["event_id"] == expected_event_id
    assert payload["readiness"]["signal"]["event_id"] == expected_event_id
    assert payload["readiness"]["signal"]["raw"]["event_id"] == expected_event_id


def test_live_canary_launch_cli_smoke_invokes_existing_guarded_flow(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = _write_json(tmp_path / "signal.json", _signal_payload())
    positions_path = _write_json(
        tmp_path / "positions.json",
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        ),
    )
    base_dir = tmp_path / "live_canary"

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_state_ops.py"),
            "--emit",
            "json",
            "--timestamp",
            TIMESTAMP,
            "launch",
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--account-id",
            ACCOUNT_ID,
            "--live-submit",
            "--arm-live-canary",
            ACCOUNT_ID,
            "--base-dir",
            str(base_dir),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 2, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["schema_name"] == "live_canary_launch_result"
    assert payload["readiness_verdict"] == "ready"
    assert payload["submit_path_invoked"] is True
    assert payload["submit_outcome"] == "blocked"
    assert payload["submit_result"]["decision"] == "blocked"
    assert payload["submit_result"]["blockers"] == ["live_canary_live_submit_requires_tastytrade_broker"]
    assert Path(payload["artifact_paths"]["result_path"]).exists()
