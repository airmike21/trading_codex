from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from trading_codex.run_archive import resolve_archive_root


DEFAULT_IBKR_SHADOW_LOOP_STATE_KEY = "primary_live_candidate_v1"
IBKR_SHADOW_LOOP_STATE_SCHEMA_NAME = "ibkr_shadow_loop_state"
IBKR_SHADOW_LOOP_STATE_SCHEMA_VERSION = 1
DEFAULT_SHADOW_ACTION_FINGERPRINT_SHORT_LENGTH = 12


def _safe_slug(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or fallback


def derive_ibkr_shadow_loop_state_key(*, requested_state_key: str | None, source_label: str) -> str:
    candidate = (requested_state_key or "").strip()
    if candidate:
        return candidate

    derived = source_label.strip()
    if derived:
        return derived
    return DEFAULT_IBKR_SHADOW_LOOP_STATE_KEY


def shadow_action_fingerprint_short(
    fingerprint: str,
    *,
    length: int = DEFAULT_SHADOW_ACTION_FINGERPRINT_SHORT_LENGTH,
) -> str:
    normalized = fingerprint.strip().lower()
    if not normalized:
        raise ValueError("shadow_action_fingerprint must be a non-empty string.")
    if length <= 0:
        raise ValueError("short fingerprint length must be > 0.")
    return normalized[:length]


def resolve_ibkr_shadow_loop_state_path(
    *,
    state_key: str,
    base_dir: Path | None = None,
    state_file: Path | None = None,
    create: bool,
) -> Path:
    if state_file is not None:
        path = Path(state_file).expanduser()
        if create:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    if base_dir is not None:
        directory = Path(base_dir).expanduser()
    else:
        directory = resolve_archive_root(create=create) / "ibkr_shadow_loop"

    if create:
        directory.mkdir(parents=True, exist_ok=True)

    filename = f"ibkr_shadow_loop.{_safe_slug(state_key, fallback=DEFAULT_IBKR_SHADOW_LOOP_STATE_KEY)}.json"
    return directory / filename


def load_ibkr_shadow_loop_state(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None

    if not raw.strip():
        raise ValueError(f"IBKR shadow loop state file {path} is empty.")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"IBKR shadow loop state file {path} is malformed: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"IBKR shadow loop state file {path} must contain a JSON object.")
    return dict(payload)


def classify_ibkr_shadow_change(
    *,
    previous_fingerprint: str | None,
    current_fingerprint: str,
) -> str:
    normalized_previous = (previous_fingerprint or "").strip().lower()
    normalized_current = current_fingerprint.strip().lower()
    if not normalized_current:
        raise ValueError("current shadow_action_fingerprint must be non-empty.")
    if not normalized_previous:
        return "first_seen"
    if normalized_previous == normalized_current:
        return "unchanged"
    return "changed"


def _write_state_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            fh.write(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
        except OSError:
            dir_fd = None
        if dir_fd is not None:
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def apply_ibkr_shadow_loop_change_detection(
    *,
    payload: dict[str, Any],
    state_key: str,
    state_path: Path,
) -> dict[str, Any]:
    current_fingerprint = str(payload.get("shadow_action_fingerprint") or "").strip().lower()
    if not current_fingerprint:
        raise ValueError("Shadow payload missing non-empty shadow_action_fingerprint.")

    run_state = str(payload.get("action_state") or "").strip()
    if not run_state:
        raise ValueError("Shadow payload missing non-empty action_state.")

    previous_state = load_ibkr_shadow_loop_state(state_path)
    previous_fingerprint = None
    created_at = str(payload.get("generated_at_chicago") or "")
    if previous_state is not None:
        previous_fingerprint = str(previous_state.get("last_shadow_action_fingerprint") or "").strip().lower() or None
        created_at = str(previous_state.get("created_at_chicago") or created_at)

    change_status = classify_ibkr_shadow_change(
        previous_fingerprint=previous_fingerprint,
        current_fingerprint=current_fingerprint,
    )
    short_fingerprint = shadow_action_fingerprint_short(current_fingerprint)

    result = dict(payload)
    result["run_state"] = run_state
    result["change_status"] = change_status
    result["shadow_action_fingerprint"] = current_fingerprint
    result["shadow_action_fingerprint_short"] = short_fingerprint
    result["state_key"] = state_key
    result["state_file"] = str(state_path)

    state_payload = {
        "schema_name": IBKR_SHADOW_LOOP_STATE_SCHEMA_NAME,
        "schema_version": IBKR_SHADOW_LOOP_STATE_SCHEMA_VERSION,
        "state_key": state_key,
        "created_at_chicago": created_at,
        "updated_at_chicago": str(result.get("generated_at_chicago") or created_at),
        "last_run_state": run_state,
        "last_change_status": change_status,
        "last_shadow_action_fingerprint": current_fingerprint,
        "last_shadow_action_fingerprint_short": short_fingerprint,
        "last_decision_summary": result.get("decision_summary"),
        "last_archive_manifest_path": result.get("archive_manifest_path"),
        "last_event_id": (
            result.get("signal", {}).get("event_id")
            if isinstance(result.get("signal"), dict)
            else None
        ),
    }
    _write_state_file(state_path, state_payload)
    return result
