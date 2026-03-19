from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from trading_codex.execution import build_execution_plan, parse_broker_snapshot, parse_signal_payload
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    append_live_canary_audit,
    audit_rows_for_result,
    claim_live_canary_event,
    evaluate_live_canary,
    finalize_live_canary_event,
    live_canary_audit_path,
)


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
    strategy: str = "dual_mom_vol10_cash",
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
        "date": "2026-03-19",
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
    account_id: str = "5WT00001",
    buying_power: float = 20_000.0,
) -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": account_id,
        "buying_power": buying_power,
        "positions": list(positions),
    }


def _build_plan(
    signal_payload: dict[str, object],
    *,
    positions: list[dict[str, object]],
    account_id: str = "5WT00001",
) -> object:
    signal = parse_signal_payload(signal_payload)
    broker = parse_broker_snapshot(_broker_snapshot(*positions, account_id=account_id))
    return build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="canary_test",
        source_ref="signal.json",
        broker_source_ref=f"tastytrade:{account_id}",
        data_dir=None,
    )


def test_live_canary_blocks_missing_account_binding() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account=None,
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_requires_account_binding" in evaluation.blockers


def test_live_canary_blocks_mismatched_account_binding() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT99999",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_account_binding_mismatch" in evaluation.blockers


def test_live_canary_blocks_missing_arming() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_not_armed" in evaluation.blockers


@pytest.mark.parametrize(
    ("signal_payload", "expected_blocker"),
    [
        (
            _signal_payload(symbol="TLT", target_shares=10, resize_prev_shares=None, resize_new_shares=None),
            "live_canary_symbol_not_allowed:TLT",
        ),
        (
            _signal_payload(action="SOMETHING_ELSE", target_shares=10, resize_prev_shares=None, resize_new_shares=None),
            "live_canary_unsupported_action:SOMETHING_ELSE",
        ),
    ],
)
def test_live_canary_blocks_unsupported_symbols_and_actions(
    signal_payload: dict[str, object],
    expected_blocker: str,
) -> None:
    plan = _build_plan(
        signal_payload,
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert expected_blocker in evaluation.blockers


def test_live_canary_caps_buy_to_one_share_deterministically() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "dry_run_ready"
    assert len(evaluation.orders) == 1
    order = evaluation.orders[0]
    assert order.symbol == "EFA"
    assert order.side == "BUY"
    assert order.requested_qty == 100
    assert order.executable_qty == 1
    assert order.cap_applied is True
    assert "live_canary_qty_capped:EFA:100:1" in evaluation.warnings


def test_live_canary_duplicate_event_state_blocks_repeat_event_for_same_account(tmp_path: Path) -> None:
    record = {
        "account_id": "5WT00001",
        "decision": "live_submitted",
        "event_id": "2026-03-19:dual_mom_vol10_cash:ROTATE:EFA:100::2026-03-31",
        "generated_at_chicago": "2026-03-19T10:00:00-05:00",
        "manual_clearance_required": False,
        "response_text": "submitted",
        "result": "submitted",
    }
    claimed, prior_record, state_path = claim_live_canary_event(
        base_dir=tmp_path,
        account_id="5WT00001",
        event_id=record["event_id"],
        record=record,
    )
    assert claimed is True
    assert prior_record is None
    finalize_live_canary_event(state_path=state_path, record=record)

    claimed, prior_record, second_path = claim_live_canary_event(
        base_dir=tmp_path,
        account_id="5WT00001",
        event_id=record["event_id"],
        record=record,
    )

    assert claimed is False
    assert second_path == state_path
    assert prior_record is not None
    assert prior_record["decision"] == "live_submitted"
    assert prior_record["event_id"] == record["event_id"]


def test_live_canary_audit_writes_expected_fields(tmp_path: Path) -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
    )
    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )
    audit_path = live_canary_audit_path(tmp_path)
    rows = audit_rows_for_result(
        evaluation=evaluation,
        decision=evaluation.decision,
        duplicate=False,
        response_text="dry-run only",
    )

    append_live_canary_audit(audit_path=audit_path, rows=rows)

    written = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert len(written) == 1
    row = written[0]
    assert row["account"] == "5WT00001"
    assert row["event_id"] == evaluation.signal.event_id
    assert row["action"] == "RESIZE"
    assert row["symbol"] == "EFA"
    assert row["requested_qty"] == 100
    assert row["executable_qty"] == 1
    assert row["decision"] == "dry_run_ready"
    assert row["armed"] is False
    assert row["duplicate"] is False
    assert row["response_text"] == "dry-run only"


def test_live_canary_guardrails_cli_smoke_with_realistic_dual_mom_vol10_cash_payload(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "dual_mom_vol10_cash_signal.json"
    signal_path.write_text(
        json.dumps(
            _signal_payload(
                strategy="dual_mom_vol10_cash",
                action="ROTATE",
                symbol="EFA",
                target_shares=100,
                resize_prev_shares=None,
                resize_new_shares=None,
            )
        ),
        encoding="utf-8",
    )
    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            _broker_snapshot(
                {"symbol": "BIL", "shares": 1, "price": 91.20, "instrument_type": "Equity"},
                account_id="5WT00001",
            )
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "live_canary"

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_guardrails.py"),
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--live-canary-account",
            "5WT00001",
            "--base-dir",
            str(base_dir),
            "--emit",
            "json",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["schema_name"] == "live_canary_guardrail_result"
    assert payload["decision"] == "dry_run_ready"
    assert payload["account_id"] == "5WT00001"
    assert payload["event_id"].startswith("2026-03-19:dual_mom_vol10_cash:ROTATE:EFA:")
    assert [order["symbol"] for order in payload["orders"]] == ["BIL", "EFA"]
    assert [order["executable_qty"] for order in payload["orders"]] == [1, 1]
    assert payload["audit_path"] == str(base_dir / "audit.jsonl")

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 2
    assert audit_rows[0]["event_id"] == payload["event_id"]
    assert {row["symbol"] for row in audit_rows} == {"BIL", "EFA"}
