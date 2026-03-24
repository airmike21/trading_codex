from __future__ import annotations

import getpass
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.execution.broker import (
    LIVE_SUBMISSION_BLOCKING_RESULTS,
    LIVE_SUBMISSION_RESULT_OPERATOR_CLEARED,
    _append_live_submission_ledger_record,
    _load_live_submission_claim,
    _load_live_submission_ledger,
    _live_submission_state_lock,
)
from trading_codex.execution.live_canary import live_canary_state_lock
from trading_codex.run_archive import resolve_archive_root

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


LIVE_CANARY_STATE_OPS_CONFIRM_PREFIX = "live-canary-clear"
LIVE_CANARY_STATE_OPS_AUDIT_FILENAME = "operator_state_ops_audit.jsonl"
LIVE_CANARY_STATE_OPS_ARCHIVE_DIRNAME = "operator_archive"
LIVE_CANARY_SUBMIT_TRACKING_LEDGER_FILENAME = "broker_live_submission_fingerprints.jsonl"


@dataclass(frozen=True)
class LiveCanaryStateScope:
    account_id: str
    strategy: str | None = None
    signal_date: str | None = None
    event_id: str | None = None
    live_submission_fingerprint: str | None = None


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected a string value.")
    stripped = value.strip()
    return stripped or None


def _normalize_scope(
    *,
    account_id: object,
    strategy: object = None,
    signal_date: object = None,
    event_id: object = None,
    live_submission_fingerprint: object = None,
) -> LiveCanaryStateScope:
    normalized_account_id = _normalize_text(account_id)
    if normalized_account_id is None:
        raise ValueError("--account-id is required.")

    normalized_strategy = _normalize_text(strategy)
    normalized_signal_date = _normalize_text(signal_date)
    normalized_event_id = _normalize_text(event_id)
    normalized_fingerprint = _normalize_text(live_submission_fingerprint)

    if normalized_event_id is not None:
        event_signal_date, event_strategy = parse_live_canary_event_scope(normalized_event_id)
        if event_signal_date is None or event_strategy is None:
            raise ValueError("--event-id must use the existing Trading Codex event_id format.")
        if normalized_signal_date is not None and normalized_signal_date != event_signal_date:
            raise ValueError("--signal-date must match the signal date encoded in --event-id.")
        if normalized_strategy is not None and normalized_strategy != event_strategy:
            raise ValueError("--strategy must match the strategy encoded in --event-id.")
        normalized_signal_date = normalized_signal_date or event_signal_date
        normalized_strategy = normalized_strategy or event_strategy

    return LiveCanaryStateScope(
        account_id=normalized_account_id,
        strategy=normalized_strategy,
        signal_date=normalized_signal_date,
        event_id=normalized_event_id,
        live_submission_fingerprint=normalized_fingerprint,
    )


def validate_status_scope(scope: LiveCanaryStateScope) -> None:
    if scope.strategy is None and scope.event_id is None and scope.live_submission_fingerprint is None:
        raise ValueError("Status requires --strategy, --event-id, or --live-submission-fingerprint.")
    if scope.signal_date is not None and scope.strategy is None and scope.event_id is None:
        raise ValueError("--signal-date requires --strategy or --event-id.")


def validate_clear_scope(scope: LiveCanaryStateScope, *, clear_scopes: set[str]) -> None:
    if not clear_scopes:
        raise ValueError("Clear requires at least one explicit --clear scope.")
    unknown = sorted(scope_name for scope_name in clear_scopes if scope_name not in {"event", "session", "submit-tracking"})
    if unknown:
        raise ValueError(f"Unsupported clear scope(s): {', '.join(unknown)}")
    if "event" in clear_scopes and scope.event_id is None:
        raise ValueError("Clearing event state requires --event-id.")
    if "session" in clear_scopes and (scope.strategy is None or scope.signal_date is None):
        raise ValueError("Clearing session state requires --strategy and --signal-date.")
    if (
        "submit-tracking" in clear_scopes
        and scope.live_submission_fingerprint is None
        and scope.event_id is None
        and (scope.strategy is None or scope.signal_date is None)
    ):
        raise ValueError(
            "Clearing submit-tracking requires --live-submission-fingerprint, --event-id, "
            "or --strategy plus --signal-date."
        )


def parse_live_canary_event_scope(event_id: str) -> tuple[str | None, str | None]:
    parts = event_id.split(":", 6)
    if len(parts) != 7:
        return None, None
    signal_date = parts[0].strip() or None
    strategy = parts[1].strip() or None
    return signal_date, strategy


def resolve_live_canary_state_base_dir(base_dir: Path | None = None, *, create: bool) -> Path:
    if base_dir is not None:
        path = Path(base_dir)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path
    path = resolve_archive_root(create=create) / "live_canary"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def live_canary_submit_tracking_ledger_path(base_dir: Path | None = None, *, create: bool = False) -> Path:
    return resolve_live_canary_state_base_dir(base_dir, create=create) / LIVE_CANARY_SUBMIT_TRACKING_LEDGER_FILENAME


def live_canary_state_ops_audit_path(base_dir: Path | None = None, *, create: bool = False) -> Path:
    return resolve_live_canary_state_base_dir(base_dir, create=create) / LIVE_CANARY_STATE_OPS_AUDIT_FILENAME


def build_live_canary_clear_confirmation_token(
    *,
    scope: LiveCanaryStateScope,
    clear_scopes: set[str],
) -> str:
    strategy = scope.strategy or "-"
    signal_date = scope.signal_date or "-"
    event_id = scope.event_id or "-"
    fingerprint = scope.live_submission_fingerprint or "-"
    scopes_slug = ",".join(sorted(clear_scopes))
    return (
        f"{LIVE_CANARY_STATE_OPS_CONFIRM_PREFIX}:"
        f"{scope.account_id}:{strategy}:{signal_date}:{event_id}:{fingerprint}:{scopes_slug}"
    )


def _fsync_directory(path: Path) -> None:
    try:
        dir_fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _append_jsonl_record(path: Path, *, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_directory(path.parent)


def _timestamp_slug(timestamp: datetime) -> str:
    return timestamp.strftime("%Y%m%dT%H%M%S%z")


def _read_json_object(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8").strip()
    if raw == "":
        raise ValueError(f"State file {path} is empty.")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"State file {path} must contain a JSON object.")
    return payload


def _safe_record_summary(record: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "decision": record.get("decision"),
        "manual_clearance_required": record.get("manual_clearance_required"),
        "response_text": record.get("response_text"),
        "result": record.get("result"),
    }
    if record.get("event_id") is not None:
        summary["event_id"] = record.get("event_id")
    if record.get("signal_date") is not None:
        summary["signal_date"] = record.get("signal_date")
    if record.get("strategy") is not None:
        summary["strategy"] = record.get("strategy")
    return summary


def _record_fingerprints(record: dict[str, Any]) -> list[str]:
    fingerprints: set[str] = set()
    stack: list[dict[str, Any]] = [record]
    seen: set[int] = set()

    while stack:
        current = stack.pop()
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)

        raw_fingerprint = current.get("live_submission_fingerprint")
        if isinstance(raw_fingerprint, str) and raw_fingerprint.strip():
            fingerprints.add(raw_fingerprint.strip())

        claim_path = current.get("claim_path")
        if isinstance(claim_path, str) and claim_path.strip():
            fingerprints.add(Path(claim_path).stem)

        durable_state = current.get("durable_state")
        if isinstance(durable_state, dict):
            stack.append(durable_state)

        duplicate_submit_refusal = current.get("duplicate_submit_refusal")
        if isinstance(duplicate_submit_refusal, dict):
            stack.append(duplicate_submit_refusal)

        prior_record = current.get("prior_record")
        if isinstance(prior_record, dict):
            stack.append(prior_record)

        live_submission = current.get("live_submission")
        if isinstance(live_submission, dict):
            stack.append(live_submission)

        session_guard = current.get("session_guard")
        if isinstance(session_guard, dict):
            stack.append(session_guard)

        guarded_record = current.get("record")
        if isinstance(guarded_record, dict):
            stack.append(guarded_record)

    return sorted(fingerprints)


def _file_artifact(
    *,
    artifact_kind: str,
    clear_scope: str,
    path: Path,
    scope: dict[str, Any],
    record: dict[str, Any],
    blocking_reason: str,
) -> dict[str, Any]:
    return {
        "artifact_kind": artifact_kind,
        "blocking": True,
        "blocking_reason": blocking_reason,
        "clear_scope": clear_scope,
        "live_submission_fingerprints": _record_fingerprints(record),
        "path": str(path),
        "record": record,
        "scope": scope,
        "summary": _safe_record_summary(record),
    }


def _event_artifacts(base_dir: Path, scope: LiveCanaryStateScope) -> list[dict[str, Any]]:
    events_dir = base_dir / "events"
    if not events_dir.exists():
        return []

    artifacts: list[dict[str, Any]] = []
    for path in sorted(events_dir.glob("*.json")):
        record = _read_json_object(path)
        if record.get("account_id") != scope.account_id:
            continue
        event_id = record.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            continue
        signal_date, strategy = parse_live_canary_event_scope(event_id)
        if scope.strategy is not None and strategy != scope.strategy:
            continue
        if scope.signal_date is not None and signal_date != scope.signal_date:
            continue
        if scope.event_id is not None and event_id != scope.event_id:
            continue
        artifacts.append(
            _file_artifact(
                artifact_kind="event_state",
                clear_scope="event",
                path=path,
                scope={
                    "account_id": scope.account_id,
                    "event_id": event_id,
                    "signal_date": signal_date,
                    "strategy": strategy,
                },
                record=record,
                blocking_reason="existing event state blocks exact-event retries for this account/event_id",
            )
        )
    return artifacts


def _session_artifacts(base_dir: Path, scope: LiveCanaryStateScope) -> list[dict[str, Any]]:
    sessions_dir = base_dir / "sessions"
    if not sessions_dir.exists():
        return []

    artifacts: list[dict[str, Any]] = []
    for path in sorted(sessions_dir.glob("*.json")):
        record = _read_json_object(path)
        if record.get("account_id") != scope.account_id:
            continue
        strategy = record.get("strategy")
        signal_date = record.get("signal_date")
        event_id = record.get("event_id")
        if scope.strategy is not None and strategy != scope.strategy:
            continue
        if scope.signal_date is not None and signal_date != scope.signal_date:
            continue
        if scope.event_id is not None and event_id != scope.event_id:
            continue
        artifacts.append(
            _file_artifact(
                artifact_kind="session_state",
                clear_scope="session",
                path=path,
                scope={
                    "account_id": scope.account_id,
                    "event_id": event_id,
                    "signal_date": signal_date,
                    "strategy": strategy,
                },
                record=record,
                blocking_reason="existing session state blocks same-session retries for this account/strategy/signal_date",
            )
        )
    return artifacts


def _claim_record_matches_scope(record: dict[str, Any], scope: LiveCanaryStateScope) -> bool:
    if record.get("account_id") != scope.account_id:
        return False
    fingerprint = record.get("live_submission_fingerprint")
    if scope.live_submission_fingerprint is not None and fingerprint != scope.live_submission_fingerprint:
        return False

    event_id = record.get("event_id")
    signal_date = record.get("signal_date")
    strategy = record.get("strategy")
    if (signal_date is None or strategy is None) and isinstance(event_id, str):
        parsed_signal_date, parsed_strategy = parse_live_canary_event_scope(event_id)
        signal_date = signal_date or parsed_signal_date
        strategy = strategy or parsed_strategy

    if scope.event_id is not None and event_id != scope.event_id:
        return False
    if scope.signal_date is not None and signal_date != scope.signal_date:
        return False
    if scope.strategy is not None and strategy != scope.strategy:
        return False
    return True


def _ledger_record_matches_scope(record: dict[str, Any], scope: LiveCanaryStateScope) -> bool:
    if record.get("account_id") != scope.account_id:
        return False
    fingerprint = record.get("live_submission_fingerprint")
    if scope.live_submission_fingerprint is not None and fingerprint != scope.live_submission_fingerprint:
        return False

    event_id = record.get("event_id")
    signal_date = record.get("signal_date")
    strategy = record.get("strategy")
    if (signal_date is None or strategy is None) and isinstance(event_id, str):
        parsed_signal_date, parsed_strategy = parse_live_canary_event_scope(event_id)
        signal_date = signal_date or parsed_signal_date
        strategy = strategy or parsed_strategy

    if scope.event_id is not None and event_id != scope.event_id:
        return False
    if scope.signal_date is not None and signal_date != scope.signal_date:
        return False
    if scope.strategy is not None and strategy != scope.strategy:
        return False
    return True


def _blocking_ledger_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    for entry in reversed(entries):
        if entry.get("result") == LIVE_SUBMISSION_RESULT_OPERATOR_CLEARED or entry.get("operator_action") == "clear":
            return None
        if bool(entry.get("manual_clearance_required")):
            return entry
        if entry.get("result") in LIVE_SUBMISSION_BLOCKING_RESULTS:
            return entry
    return None


def _submit_tracking_artifacts(
    *,
    base_dir: Path,
    scope: LiveCanaryStateScope,
    seed_fingerprints: set[str],
) -> list[dict[str, Any]]:
    ledger_path = live_canary_submit_tracking_ledger_path(base_dir, create=False)
    ledger_entries = _load_live_submission_ledger(ledger_path) if ledger_path.exists() else []

    fingerprints = {fingerprint for fingerprint in seed_fingerprints if fingerprint}
    for entry in ledger_entries:
        fingerprint = entry.get("live_submission_fingerprint")
        if not isinstance(fingerprint, str) or not fingerprint.strip():
            continue
        if _ledger_record_matches_scope(entry, scope):
            fingerprints.add(fingerprint.strip())

    claims_dir = ledger_path.parent / "claims"
    claim_records: dict[str, dict[str, Any]] = {}
    if claims_dir.exists():
        for claim_path in sorted(claims_dir.glob("*.json")):
            record = _load_live_submission_claim(claim_path)
            if record is None:
                continue
            if not _claim_record_matches_scope(record, scope):
                continue
            fingerprint = record.get("live_submission_fingerprint")
            if not isinstance(fingerprint, str) or not fingerprint.strip():
                fingerprint = claim_path.stem
            normalized_fingerprint = fingerprint.strip()
            claim_records[normalized_fingerprint] = {
                "path": claim_path,
                "record": record,
            }
            fingerprints.add(normalized_fingerprint)

    artifacts: list[dict[str, Any]] = []
    for fingerprint in sorted(fingerprints):
        claim_info = claim_records.get(fingerprint)
        if claim_info is not None:
            claim_record = claim_info["record"]
            artifacts.append(
                {
                    "artifact_kind": "submit_tracking_claim",
                    "blocking": True,
                    "blocking_reason": "existing submit claim blocks duplicate fingerprint retries until explicitly cleared",
                    "clear_scope": "submit-tracking",
                    "live_submission_fingerprint": fingerprint,
                    "path": str(claim_info["path"]),
                    "record": claim_record,
                    "scope": {
                        "account_id": claim_record.get("account_id"),
                        "event_id": claim_record.get("event_id"),
                        "signal_date": claim_record.get("signal_date"),
                        "strategy": claim_record.get("strategy"),
                    },
                    "summary": _safe_record_summary(claim_record),
                }
            )

        matching_entries = [
            entry
            for entry in ledger_entries
            if entry.get("live_submission_fingerprint") == fingerprint
        ]
        if matching_entries or scope.live_submission_fingerprint == fingerprint:
            latest_entry = matching_entries[-1] if matching_entries else None
            blocking_entry = _blocking_ledger_entry(matching_entries)
            latest_result = None if latest_entry is None else latest_entry.get("result")
            artifacts.append(
                {
                    "artifact_kind": "submit_tracking_ledger",
                    "blocking": blocking_entry is not None,
                    "blocking_reason": (
                        "latest blocking ledger record will refuse duplicate fingerprint retries until explicitly cleared"
                        if blocking_entry is not None
                        else None
                    ),
                    "clear_scope": "submit-tracking",
                    "entries": matching_entries,
                    "latest_entry": latest_entry,
                    "latest_result": latest_result,
                    "live_submission_fingerprint": fingerprint,
                    "path": str(ledger_path),
                    "record": blocking_entry,
                    "scope": {
                        "account_id": scope.account_id,
                        "event_id": None if latest_entry is None else latest_entry.get("event_id"),
                        "signal_date": None if latest_entry is None else latest_entry.get("signal_date"),
                        "strategy": None if latest_entry is None else latest_entry.get("strategy"),
                    },
                    "summary": (
                        {
                            "entry_count": len(matching_entries),
                            "latest_result": latest_result,
                            "operator_cleared": latest_result == LIVE_SUBMISSION_RESULT_OPERATOR_CLEARED,
                        }
                        if latest_entry is not None
                        else {
                            "entry_count": 0,
                            "latest_result": None,
                            "operator_cleared": False,
                        }
                    ),
                }
            )
    return artifacts


def _status_summary(*, artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    blocking_artifacts = [artifact for artifact in artifacts if artifact.get("blocking")]
    return {
        "artifact_count": len(artifacts),
        "blocking_artifact_count": len(blocking_artifacts),
        "blocking_artifact_kinds": sorted({str(artifact["artifact_kind"]) for artifact in blocking_artifacts}),
        "event_state_count": sum(1 for artifact in artifacts if artifact["artifact_kind"] == "event_state"),
        "session_state_count": sum(1 for artifact in artifacts if artifact["artifact_kind"] == "session_state"),
        "submit_tracking_count": sum(
            1
            for artifact in artifacts
            if artifact["artifact_kind"] in {"submit_tracking_claim", "submit_tracking_ledger"}
        ),
    }


def build_live_canary_state_status(
    *,
    base_dir: Path | None,
    account_id: object,
    strategy: object = None,
    signal_date: object = None,
    event_id: object = None,
    live_submission_fingerprint: object = None,
) -> dict[str, Any]:
    scope = _normalize_scope(
        account_id=account_id,
        strategy=strategy,
        signal_date=signal_date,
        event_id=event_id,
        live_submission_fingerprint=live_submission_fingerprint,
    )
    validate_status_scope(scope)

    resolved_base_dir = resolve_live_canary_state_base_dir(base_dir, create=False)
    event_artifacts = _event_artifacts(resolved_base_dir, scope)
    session_artifacts = _session_artifacts(resolved_base_dir, scope)

    seed_fingerprints: set[str] = set()
    for artifact in event_artifacts + session_artifacts:
        seed_fingerprints.update(artifact.get("live_submission_fingerprints", []))
    if scope.live_submission_fingerprint is not None:
        seed_fingerprints.add(scope.live_submission_fingerprint)

    submit_tracking_artifacts = _submit_tracking_artifacts(
        base_dir=resolved_base_dir,
        scope=scope,
        seed_fingerprints=seed_fingerprints,
    )
    artifacts = event_artifacts + session_artifacts + submit_tracking_artifacts
    blocking_artifacts = [
        {
            "artifact_kind": artifact["artifact_kind"],
            "blocking_reason": artifact.get("blocking_reason"),
            "clear_scope": artifact.get("clear_scope"),
            "live_submission_fingerprint": artifact.get("live_submission_fingerprint"),
            "path": artifact["path"],
            "scope": artifact["scope"],
            "summary": artifact["summary"],
        }
        for artifact in artifacts
        if artifact.get("blocking")
    ]

    return {
        "schema_name": "live_canary_state_status",
        "schema_version": 1,
        "base_dir": str(resolved_base_dir),
        "operator_state_ops_audit_path": str(live_canary_state_ops_audit_path(resolved_base_dir, create=False)),
        "scope": {
            "account_id": scope.account_id,
            "event_id": scope.event_id,
            "live_submission_fingerprint": scope.live_submission_fingerprint,
            "signal_date": scope.signal_date,
            "strategy": scope.strategy,
        },
        "summary": _status_summary(artifacts=artifacts),
        "blocking_artifacts": blocking_artifacts,
        "artifacts": artifacts,
    }


def _archive_relative_path(*, artifact: dict[str, Any]) -> Path:
    path = Path(artifact["path"])
    artifact_kind = str(artifact["artifact_kind"])
    if artifact_kind == "event_state":
        return Path("events") / path.name
    if artifact_kind == "session_state":
        return Path("sessions") / path.name
    if artifact_kind == "submit_tracking_claim":
        return Path("claims") / path.name
    raise ValueError(f"Unsupported archive artifact kind: {artifact_kind}")


def _submit_tracking_fingerprints_for_clear(
    *,
    scope: LiveCanaryStateScope,
    status_payload: dict[str, Any],
    target_artifacts: list[dict[str, Any]],
) -> list[str]:
    if scope.live_submission_fingerprint is not None:
        return [scope.live_submission_fingerprint]

    fingerprints: set[str] = set()
    for artifact in target_artifacts:
        if artifact["artifact_kind"] in {"event_state", "session_state"}:
            fingerprints.update(artifact.get("live_submission_fingerprints", []))

    if not fingerprints:
        fingerprints.update(
            artifact["live_submission_fingerprint"]
            for artifact in status_payload["artifacts"]
            if artifact["artifact_kind"] in {"submit_tracking_claim", "submit_tracking_ledger"}
            and artifact.get("live_submission_fingerprint")
        )

    if not fingerprints:
        raise ValueError(
            "Submit-tracking clear could not resolve a live_submission_fingerprint from the requested scope. "
            "Provide --live-submission-fingerprint explicitly."
        )
    if len(fingerprints) != 1:
        raise ValueError(
            "Submit-tracking clear matched multiple fingerprints. Provide --live-submission-fingerprint explicitly."
        )
    return sorted(fingerprints)


def _clear_plan(
    *,
    base_dir: Path,
    scope: LiveCanaryStateScope,
    clear_scopes: set[str],
    timestamp: datetime,
    reason: str | None,
) -> dict[str, Any]:
    status_payload = build_live_canary_state_status(
        base_dir=base_dir,
        account_id=scope.account_id,
        strategy=scope.strategy,
        signal_date=scope.signal_date,
        event_id=scope.event_id,
        live_submission_fingerprint=scope.live_submission_fingerprint,
    )

    artifacts_by_kind: dict[str, list[dict[str, Any]]] = {}
    for artifact in status_payload["artifacts"]:
        artifacts_by_kind.setdefault(str(artifact["artifact_kind"]), []).append(artifact)

    target_artifacts: list[dict[str, Any]] = []
    if "event" in clear_scopes:
        target_artifacts.extend(artifacts_by_kind.get("event_state", []))
    if "session" in clear_scopes:
        target_artifacts.extend(artifacts_by_kind.get("session_state", []))

    planned_operations: list[dict[str, Any]] = []
    archive_root = (
        resolve_live_canary_state_base_dir(base_dir, create=True)
        / LIVE_CANARY_STATE_OPS_ARCHIVE_DIRNAME
        / _timestamp_slug(timestamp)
    )
    for artifact in target_artifacts:
        planned_operations.append(
            {
                "operation": "archive_file",
                "artifact_kind": artifact["artifact_kind"],
                "source_path": artifact["path"],
                "archive_path": str(archive_root / _archive_relative_path(artifact=artifact)),
            }
        )

    if "submit-tracking" in clear_scopes:
        fingerprints = _submit_tracking_fingerprints_for_clear(
            scope=scope,
            status_payload=status_payload,
            target_artifacts=target_artifacts,
        )
        ledger_path = live_canary_submit_tracking_ledger_path(base_dir, create=False)
        ledger_artifacts_by_fingerprint = {
            artifact["live_submission_fingerprint"]: artifact
            for artifact in status_payload["artifacts"]
            if artifact["artifact_kind"] == "submit_tracking_ledger" and artifact.get("live_submission_fingerprint")
        }
        claim_artifacts_by_fingerprint = {
            artifact["live_submission_fingerprint"]: artifact
            for artifact in status_payload["artifacts"]
            if artifact["artifact_kind"] == "submit_tracking_claim" and artifact.get("live_submission_fingerprint")
        }
        for fingerprint in fingerprints:
            claim_artifact = claim_artifacts_by_fingerprint.get(fingerprint)
            if claim_artifact is not None:
                planned_operations.append(
                    {
                        "operation": "archive_file",
                        "artifact_kind": claim_artifact["artifact_kind"],
                        "live_submission_fingerprint": fingerprint,
                        "source_path": claim_artifact["path"],
                        "archive_path": str(archive_root / _archive_relative_path(artifact=claim_artifact)),
                    }
                )
            ledger_artifact = ledger_artifacts_by_fingerprint.get(fingerprint)
            if ledger_artifact is None:
                planned_operations.append(
                    {
                        "operation": "noop",
                        "artifact_kind": "submit_tracking_ledger",
                        "live_submission_fingerprint": fingerprint,
                        "reason": "no_matching_ledger_records",
                    }
                )
                continue
            if ledger_artifact["summary"]["operator_cleared"]:
                planned_operations.append(
                    {
                        "operation": "noop",
                        "artifact_kind": "submit_tracking_ledger",
                        "live_submission_fingerprint": fingerprint,
                        "reason": "already_operator_cleared",
                    }
                )
                continue
            latest_entry = ledger_artifact.get("latest_entry")
            planned_operations.append(
                {
                    "operation": "append_submit_tracking_clear",
                    "artifact_kind": "submit_tracking_ledger",
                    "ledger_path": str(ledger_path),
                    "live_submission_fingerprint": fingerprint,
                    "operator_clear_record": {
                        "account_id": scope.account_id,
                        "artifact_path": None,
                        "attempted_order_count": 0,
                        "accepted_order_count": 0,
                        "broker_name": None if latest_entry is None else latest_entry.get("broker_name"),
                        "event_id": None if latest_entry is None else latest_entry.get("event_id"),
                        "generated_at_chicago": timestamp.isoformat(),
                        "live_submission_fingerprint": fingerprint,
                        "manual_clearance_required": False,
                        "operator_action": "clear",
                        "operator_reason": reason,
                        "operator_user": getpass.getuser(),
                        "plan_sha256": None if latest_entry is None else latest_entry.get("plan_sha256"),
                        "refusal_reasons": [],
                        "result": LIVE_SUBMISSION_RESULT_OPERATOR_CLEARED,
                        "signal_date": None if latest_entry is None else latest_entry.get("signal_date"),
                        "strategy": None if latest_entry is None else latest_entry.get("strategy"),
                        "submission_succeeded": False,
                    },
                }
            )

    confirmation_token = build_live_canary_clear_confirmation_token(scope=scope, clear_scopes=clear_scopes)
    return {
        "schema_name": "live_canary_state_clear",
        "schema_version": 1,
        "apply": False,
        "base_dir": str(base_dir),
        "clear_scopes": sorted(clear_scopes),
        "confirmation_required": True,
        "confirmation_token": confirmation_token,
        "matched_artifacts": target_artifacts,
        "operator_state_ops_audit_path": str(live_canary_state_ops_audit_path(base_dir, create=True)),
        "planned_operations": planned_operations,
        "reason": reason,
        "scope": {
            "account_id": scope.account_id,
            "event_id": scope.event_id,
            "live_submission_fingerprint": scope.live_submission_fingerprint,
            "signal_date": scope.signal_date,
            "strategy": scope.strategy,
        },
        "status": status_payload,
    }


def preview_live_canary_state_clear(
    *,
    base_dir: Path | None,
    account_id: object,
    clear_scopes: set[str],
    strategy: object = None,
    signal_date: object = None,
    event_id: object = None,
    live_submission_fingerprint: object = None,
    reason: str | None = None,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    scope = _normalize_scope(
        account_id=account_id,
        strategy=strategy,
        signal_date=signal_date,
        event_id=event_id,
        live_submission_fingerprint=live_submission_fingerprint,
    )
    validate_clear_scope(scope, clear_scopes=clear_scopes)
    resolved_base_dir = resolve_live_canary_state_base_dir(base_dir, create=True)
    resolved_reason = _normalize_text(reason)
    resolved_timestamp = timestamp or _chicago_now()
    return _clear_plan(
        base_dir=resolved_base_dir,
        scope=scope,
        clear_scopes=set(clear_scopes),
        timestamp=resolved_timestamp,
        reason=resolved_reason,
    )


def _move_to_archive(*, source_path: Path, archive_path: Path) -> bool:
    if not source_path.exists():
        return False
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(source_path), str(archive_path))
    _fsync_directory(source_path.parent)
    _fsync_directory(archive_path.parent)
    return True


def apply_live_canary_state_clear(
    *,
    base_dir: Path | None,
    account_id: object,
    clear_scopes: set[str],
    confirm: str,
    strategy: object = None,
    signal_date: object = None,
    event_id: object = None,
    live_submission_fingerprint: object = None,
    reason: str | None = None,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    preview = preview_live_canary_state_clear(
        base_dir=base_dir,
        account_id=account_id,
        clear_scopes=clear_scopes,
        strategy=strategy,
        signal_date=signal_date,
        event_id=event_id,
        live_submission_fingerprint=live_submission_fingerprint,
        reason=reason,
        timestamp=timestamp,
    )
    if confirm != preview["confirmation_token"]:
        raise ValueError("Destructive clear requires --confirm to exactly match the preview confirmation token.")

    applied_operations: list[dict[str, Any]] = []
    archive_file_ops = [
        operation
        for operation in preview["planned_operations"]
        if operation["operation"] == "archive_file"
    ]
    live_canary_archive_ops = [
        operation
        for operation in archive_file_ops
        if operation["artifact_kind"] in {"event_state", "session_state"}
    ]
    if live_canary_archive_ops:
        with live_canary_state_lock(Path(preview["base_dir"])):
            for operation in live_canary_archive_ops:
                source_path = Path(operation["source_path"])
                archive_path = Path(operation["archive_path"])
                moved = _move_to_archive(source_path=source_path, archive_path=archive_path)
                applied_operations.append(
                    {
                        **operation,
                        "applied": moved,
                        "result": "archived" if moved else "already_absent",
                    }
                )

    append_clear_ops = [
        operation
        for operation in preview["planned_operations"]
        if operation["operation"] == "append_submit_tracking_clear"
    ]
    submit_claim_archive_ops = [
        operation
        for operation in archive_file_ops
        if operation["artifact_kind"] == "submit_tracking_claim"
    ]
    if append_clear_ops or submit_claim_archive_ops:
        if append_clear_ops:
            ledger_path = Path(append_clear_ops[0]["ledger_path"])
        else:
            ledger_path = live_canary_submit_tracking_ledger_path(Path(preview["base_dir"]), create=True)
            if not ledger_path.exists():
                ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with _live_submission_state_lock(ledger_path):
            current_entries = _load_live_submission_ledger(ledger_path) if ledger_path.exists() else []
            by_fingerprint: dict[str, list[dict[str, Any]]] = {}
            for entry in current_entries:
                fingerprint = entry.get("live_submission_fingerprint")
                if isinstance(fingerprint, str) and fingerprint.strip():
                    by_fingerprint.setdefault(fingerprint.strip(), []).append(entry)
            for operation in submit_claim_archive_ops:
                source_path = Path(operation["source_path"])
                archive_path = Path(operation["archive_path"])
                moved = _move_to_archive(source_path=source_path, archive_path=archive_path)
                applied_operations.append(
                    {
                        **operation,
                        "applied": moved,
                        "result": "archived" if moved else "already_absent",
                    }
                )
            for operation in append_clear_ops:
                fingerprint = operation["live_submission_fingerprint"]
                current_for_fingerprint = by_fingerprint.get(fingerprint, [])
                if current_for_fingerprint and _blocking_ledger_entry(current_for_fingerprint) is None:
                    applied_operations.append(
                        {
                            **operation,
                            "applied": False,
                            "result": "already_operator_cleared",
                        }
                    )
                    continue
                _append_live_submission_ledger_record(
                    ledger_path,
                    record=dict(operation["operator_clear_record"]),
                )
                by_fingerprint.setdefault(fingerprint, []).append(dict(operation["operator_clear_record"]))
                applied_operations.append(
                    {
                        **operation,
                        "applied": True,
                        "result": "operator_cleared",
                    }
                )

    noop_ops = [
        {
            **operation,
            "applied": False,
            "result": operation["reason"],
        }
        for operation in preview["planned_operations"]
        if operation["operation"] == "noop"
    ]
    applied_operations.extend(noop_ops)

    audit_record = {
        "schema_name": "live_canary_state_op",
        "schema_version": 1,
        "action": "clear",
        "applied_at_chicago": (timestamp or _chicago_now()).isoformat(),
        "clear_scopes": list(preview["clear_scopes"]),
        "confirmation_token": preview["confirmation_token"],
        "operations": applied_operations,
        "reason": preview["reason"],
        "scope": preview["scope"],
    }
    _append_jsonl_record(
        live_canary_state_ops_audit_path(Path(preview["base_dir"]), create=True),
        record=audit_record,
    )

    return {
        **preview,
        "apply": True,
        "applied_operations": applied_operations,
    }
