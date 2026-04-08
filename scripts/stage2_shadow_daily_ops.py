#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import errno
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from trading_codex.execution.paper_lane import (  # noqa: E402
    DEFAULT_PAPER_STARTING_CASH,
    DEFAULT_PAPER_STATE_KEY,
    resolve_paper_lane_paths,
)
from trading_codex.run_archive import resolve_archive_root, write_run_archive  # noqa: E402
from trading_codex.shadow import (  # noqa: E402
    PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL,
    PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS,
    PRIMARY_LIVE_CANDIDATE_V1_ID,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
)

try:
    from scripts import paper_lane_daily_ops
except ImportError:  # pragma: no cover - direct script execution path
    import paper_lane_daily_ops  # type: ignore[no-redef]


DEFAULT_PROVIDER = paper_lane_daily_ops.DEFAULT_PROVIDER
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DEFAULT_SHADOW_OPS_CONFIG = REPO_ROOT / "configs" / "stage2_shadow_ops.json"
SUPPORTED_PAIR_ID = f"{PRIMARY_LIVE_CANDIDATE_V1_ID}_vs_{PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID}"
UNCONFIGURED_SCOPE_KEY = "unconfigured"
STEP_SCHEMA_NAME = "stage2_shadow_daily_ops_step"
STEP_SCHEMA_VERSION = 1
RUN_SCHEMA_NAME = "stage2_shadow_daily_ops_run"
RUN_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_NAME = "stage2_shadow_daily_ops_log_entry"
SUMMARY_SCHEMA_VERSION = 1
CONFIG_SCHEMA_NAME = "stage2_shadow_ops_config"
CONFIG_SCHEMA_VERSION = 1
RUN_LOG_COLUMNS = (
    "schema_name",
    "schema_version",
    "run_id",
    "timestamp_chicago",
    "ops_date",
    "overall_result",
    "failed_step",
    "no_op_reason",
    "pair_id",
    "primary_strategy_id",
    "shadow_strategy_id",
    "provider",
    "shadow_ops_config_path",
    "data_dir",
    "local_replay_enabled",
    "local_replay_state_key",
    "local_replay_auto_initialized",
    "replay_skipped_reason",
    "update_exit_code",
    "update_updated_symbols",
    "compare_exit_code",
    "compare_as_of_date",
    "compare_current_decision",
    "compare_shadow_review_state",
    "compare_shadow_automation_decision",
    "compare_shadow_automation_status",
    "compare_primary_action",
    "compare_primary_symbol",
    "compare_shadow_action",
    "compare_shadow_symbol",
    "compare_shadow_next_rebalance",
    "compare_report_json",
    "compare_report_markdown",
    "compare_scoreboard_csv",
    "compare_shadow_signal_json",
    "compare_shadow_review_json",
    "compare_shadow_review_markdown",
    "replay_init_exit_code",
    "replay_init_archive_manifest_path",
    "replay_status_exit_code",
    "replay_status_signal_date",
    "replay_status_signal_action",
    "replay_status_signal_symbol",
    "replay_status_target_shares",
    "replay_status_next_rebalance",
    "replay_status_event_id",
    "replay_status_drift_present",
    "replay_status_event_already_applied",
    "replay_status_archive_manifest_path",
    "replay_apply_exit_code",
    "replay_apply_result",
    "replay_apply_duplicate_event_blocked",
    "replay_apply_event_receipt_path",
    "replay_apply_archive_manifest_path",
    "replay_state_path",
    "replay_ledger_path",
    "daily_ops_manifest_path",
    "daily_ops_jsonl_path",
    "daily_ops_csv_path",
    "daily_ops_xlsx_path",
)


class ShadowDailyOpsRunLockedError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShadowReplayConfig:
    enabled: bool
    state_key: str | None
    starting_cash: float | None


@dataclass(frozen=True)
class ActiveShadowPairConfig:
    pair_id: str
    primary_strategy_id: str
    shadow_strategy_id: str
    local_replay: ShadowReplayConfig


@dataclass(frozen=True)
class ShadowOpsConfig:
    path: Path
    active_pair: ActiveShadowPairConfig | None
    raw_payload: dict[str, Any]


def _repo_root() -> Path:
    return REPO_ROOT


def _default_update_symbols() -> list[str]:
    symbols = list(PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS)
    symbols.append(PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL)
    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        rendered = str(symbol).strip().upper()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        deduped.append(rendered)
    return deduped


def _write_csv(path: Path, *, rows: list[dict[str, Any]]) -> None:
    from io import StringIO

    sio = StringIO()
    writer = csv.DictWriter(sio, fieldnames=list(RUN_LOG_COLUMNS), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in RUN_LOG_COLUMNS})
    paper_lane_daily_ops._atomic_write_text(path, sio.getvalue())


def _write_xlsx(path: Path, *, rows: list[dict[str, Any]], timestamp) -> None:  # type: ignore[no-untyped-def]
    payload = paper_lane_daily_ops._build_xlsx_bytes(
        headers=list(RUN_LOG_COLUMNS),
        rows=rows,
        timestamp=timestamp,
    )
    paper_lane_daily_ops._atomic_write_bytes(path, payload)


def resolve_ops_paths(
    *,
    scope_key: str,
    archive_root: Path | None = None,
    create: bool,
) -> dict[str, Path]:
    resolved_archive_root = resolve_archive_root(
        preferred_root=paper_lane_daily_ops._expand_path(archive_root),
        create=create,
    )
    ops_root = resolved_archive_root / "stage2_shadow_ops" / paper_lane_daily_ops._safe_slug(
        scope_key,
        fallback=UNCONFIGURED_SCOPE_KEY,
    )
    if create:
        ops_root.mkdir(parents=True, exist_ok=True)
    return {
        "archive_root": resolved_archive_root,
        "ops_root": ops_root,
        "jsonl_path": ops_root / "stage2_shadow_daily_ops_log.jsonl",
        "csv_path": ops_root / "stage2_shadow_daily_ops_runs.csv",
        "xlsx_path": ops_root / "stage2_shadow_daily_ops_runs.xlsx",
        "lock_path": ops_root / "stage2_shadow_daily_ops.lock",
    }


def _normalize_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    rendered = value.strip()
    if not rendered:
        raise ValueError(f"{field_name} must not be empty.")
    return rendered


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"JSON file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON file {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def load_shadow_ops_config(config_path: Path) -> ShadowOpsConfig:
    resolved_path = paper_lane_daily_ops._expand_path(config_path)
    if resolved_path is None:
        raise ValueError("shadow ops config path must not be empty.")

    payload = _load_json_object(resolved_path)
    schema_name = payload.get("schema_name")
    if schema_name != CONFIG_SCHEMA_NAME:
        raise ValueError(f"shadow ops config schema_name must be {CONFIG_SCHEMA_NAME!r}.")
    schema_version = payload.get("schema_version")
    if schema_version != CONFIG_SCHEMA_VERSION:
        raise ValueError(f"shadow ops config schema_version must be {CONFIG_SCHEMA_VERSION}.")

    raw_active_pair = payload.get("active_pair")
    if raw_active_pair is None:
        return ShadowOpsConfig(path=resolved_path, active_pair=None, raw_payload=payload)
    if not isinstance(raw_active_pair, dict):
        raise ValueError("shadow ops config active_pair must be an object or null.")

    pair_id = _normalize_string(raw_active_pair.get("pair_id"), field_name="active_pair.pair_id")
    primary_strategy_id = _normalize_string(
        raw_active_pair.get("primary_strategy_id"),
        field_name="active_pair.primary_strategy_id",
    )
    shadow_strategy_id = _normalize_string(
        raw_active_pair.get("shadow_strategy_id"),
        field_name="active_pair.shadow_strategy_id",
    )

    if pair_id != SUPPORTED_PAIR_ID:
        raise ValueError(
            f"Unsupported Stage 2 shadow pair {pair_id!r}. Supported pair: {SUPPORTED_PAIR_ID!r}."
        )
    if primary_strategy_id != PRIMARY_LIVE_CANDIDATE_V1_ID:
        raise ValueError(
            "Stage 2 shadow ops only supports the approved primary candidate "
            f"{PRIMARY_LIVE_CANDIDATE_V1_ID!r}."
        )
    if shadow_strategy_id != PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID:
        raise ValueError(
            "Stage 2 shadow ops only supports the bounded shadow candidate "
            f"{PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID!r}."
        )

    raw_local_replay = raw_active_pair.get("local_replay")
    if raw_local_replay is None:
        raw_local_replay = {}
    if not isinstance(raw_local_replay, dict):
        raise ValueError("active_pair.local_replay must be an object or null.")

    enabled = bool(raw_local_replay.get("enabled", False))
    state_key = raw_local_replay.get("state_key")
    starting_cash = raw_local_replay.get("starting_cash")

    normalized_state_key = None if state_key is None else _normalize_string(
        state_key,
        field_name="active_pair.local_replay.state_key",
    )
    normalized_starting_cash = None
    if starting_cash is not None:
        try:
            normalized_starting_cash = float(starting_cash)
        except (TypeError, ValueError) as exc:
            raise ValueError("active_pair.local_replay.starting_cash must be a number.") from exc
        if normalized_starting_cash <= 0.0:
            raise ValueError("active_pair.local_replay.starting_cash must be > 0.")

    if enabled:
        if normalized_state_key is None:
            raise ValueError("active_pair.local_replay.state_key is required when local replay is enabled.")
        if normalized_state_key == DEFAULT_PAPER_STATE_KEY:
            raise ValueError(
                "active_pair.local_replay.state_key must stay separate from the primary local paper lane "
                f"({DEFAULT_PAPER_STATE_KEY!r})."
            )
        if normalized_starting_cash is None:
            raise ValueError("active_pair.local_replay.starting_cash is required when local replay is enabled.")

    active_pair = ActiveShadowPairConfig(
        pair_id=pair_id,
        primary_strategy_id=primary_strategy_id,
        shadow_strategy_id=shadow_strategy_id,
        local_replay=ShadowReplayConfig(
            enabled=enabled,
            state_key=normalized_state_key,
            starting_cash=normalized_starting_cash,
        ),
    )
    return ShadowOpsConfig(path=resolved_path, active_pair=active_pair, raw_payload=payload)


def _scope_key(config: ShadowOpsConfig) -> str:
    if config.active_pair is None:
        return UNCONFIGURED_SCOPE_KEY
    return config.active_pair.pair_id


@contextmanager
def _shadow_daily_ops_run_lock(*, lock_path: Path, scope_key: str):
    if fcntl is None:  # pragma: no cover
        raise RuntimeError("stage2_shadow_daily_ops single-instance locking requires a POSIX platform.")

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            lock_file.seek(0)
            holder = lock_file.read().strip()
            message = (
                f"another stage2_shadow_daily_ops run is already active for scope_key={scope_key}; "
                f"lock_path={lock_path}"
            )
            if holder:
                message = f"{message}; holder={holder}"
            raise ShadowDailyOpsRunLockedError(message) from exc

        lock_file.seek(0)
        lock_file.truncate(0)
        lock_file.write(
            f"pid={os.getpid()} scope_key={scope_key} acquired_at_chicago={paper_lane_daily_ops._chicago_now().isoformat()}\n"
        )
        lock_file.flush()
        os.fsync(lock_file.fileno())
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def build_stage2_shadow_compare_cmd(
    *,
    repo_root: Path,
    data_dir: Path,
    artifacts_dir: Path,
) -> list[str]:
    return [
        sys.executable,
        str(repo_root / "scripts" / "stage2_shadow_compare.py"),
        "--data-dir",
        str(data_dir),
        "--artifacts-dir",
        str(artifacts_dir),
    ]


def build_shadow_paper_lane_cmd(
    *,
    repo_root: Path,
    command: str,
    state_key: str,
    paper_base_dir: Path | None,
    timestamp: str | None,
    signal_json_file: Path | None = None,
    data_dir: Path | None = None,
    starting_cash: float | None = None,
) -> list[str]:
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "paper_lane.py"),
        "--emit",
        "json",
        "--state-key",
        state_key,
    ]
    if paper_base_dir is not None:
        cmd.extend(["--base-dir", str(paper_base_dir)])
    if timestamp is not None:
        cmd.extend(["--timestamp", timestamp])
    cmd.append(command)

    if command == "init":
        if starting_cash is not None:
            cmd.extend(["--starting-cash", str(starting_cash)])
        return cmd

    if signal_json_file is None:
        raise ValueError("signal_json_file is required for shadow paper status/apply.")
    cmd.extend(["--signal-json-file", str(signal_json_file)])
    if data_dir is not None:
        cmd.extend(["--data-dir", str(data_dir)])
    return cmd


def _run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root), env=env)


def _run_step(
    *,
    repo_root: Path,
    step_name: str,
    cmd: list[str],
    expect_json_stdout: bool,
    timestamp,
) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    started = paper_lane_daily_ops._chicago_now()
    proc = _run_process(cmd, repo_root=repo_root)
    completed = paper_lane_daily_ops._chicago_now()
    parse_error: str | None = None
    stdout_json: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None

    if expect_json_stdout:
        if proc.returncode == 0:
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError as exc:
                parse_error = f"stdout JSON decode failed: {exc}"
            else:
                if isinstance(parsed, dict):
                    stdout_json = parsed
                else:
                    parse_error = "stdout JSON payload must be an object."
    else:
        metrics = paper_lane_daily_ops._parse_update_metrics(proc.stderr)

    success = proc.returncode == 0 and parse_error is None
    return {
        "schema_name": STEP_SCHEMA_NAME,
        "schema_version": STEP_SCHEMA_VERSION,
        "step": step_name,
        "timestamp_chicago": timestamp.isoformat(),
        "started_at_chicago": started.isoformat(),
        "completed_at_chicago": completed.isoformat(),
        "duration_seconds": round((completed - started).total_seconds(), 6),
        "command": cmd,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "stdout_json": stdout_json,
        "metrics": metrics,
        "parse_error": parse_error,
        "success": success,
    }


def _internal_failed_step(
    *,
    step_name: str,
    message: str,
    timestamp,
) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    now = paper_lane_daily_ops._chicago_now().isoformat()
    return {
        "schema_name": STEP_SCHEMA_NAME,
        "schema_version": STEP_SCHEMA_VERSION,
        "step": step_name,
        "timestamp_chicago": timestamp.isoformat(),
        "started_at_chicago": now,
        "completed_at_chicago": now,
        "duration_seconds": 0.0,
        "command": [],
        "exit_code": 2,
        "stdout": "",
        "stderr": message,
        "stdout_json": None,
        "metrics": None,
        "parse_error": message,
        "success": False,
    }


def _resolve_data_dir(path: Path | None) -> Path:
    candidate = path or DEFAULT_DATA_DIR
    expanded = paper_lane_daily_ops._expand_path(candidate)
    return expanded if expanded is not None else Path(candidate).resolve()


def _load_compare_details(
    *,
    compare_summary: dict[str, Any],
    shadow_strategy_id: str,
) -> dict[str, Any]:
    report_json_path = paper_lane_daily_ops._expand_path(Path(_normalize_string(compare_summary.get("report_json"), field_name="stage2_shadow_compare.report_json")))
    report_markdown_path = paper_lane_daily_ops._expand_path(
        Path(_normalize_string(compare_summary.get("report_markdown"), field_name="stage2_shadow_compare.report_markdown"))
    )
    scoreboard_csv_path = paper_lane_daily_ops._expand_path(
        Path(_normalize_string(compare_summary.get("scoreboard_csv"), field_name="stage2_shadow_compare.scoreboard_csv"))
    )
    shadow_output_json_path = paper_lane_daily_ops._expand_path(
        Path(_normalize_string(compare_summary.get("shadow_output_json"), field_name="stage2_shadow_compare.shadow_output_json"))
    )
    shadow_review_markdown_path = paper_lane_daily_ops._expand_path(
        Path(_normalize_string(compare_summary.get("shadow_review_markdown"), field_name="stage2_shadow_compare.shadow_review_markdown"))
    )
    if report_json_path is None or report_markdown_path is None or scoreboard_csv_path is None or shadow_output_json_path is None or shadow_review_markdown_path is None:
        raise ValueError("stage2_shadow_compare summary returned an unresolved artifact path.")

    report = _load_json_object(report_json_path)
    candidates = report.get("candidates")
    if not isinstance(candidates, dict):
        raise ValueError(f"stage2_shadow_compare report candidates must be an object: {report_json_path}")
    shadow_candidate = candidates.get(shadow_strategy_id)
    if not isinstance(shadow_candidate, dict):
        raise ValueError(
            f"stage2_shadow_compare report does not contain shadow candidate {shadow_strategy_id!r}: {report_json_path}"
        )
    shadow_artifacts = shadow_candidate.get("artifacts")
    if not isinstance(shadow_artifacts, dict):
        raise ValueError(f"stage2_shadow_compare shadow candidate artifacts must be an object: {report_json_path}")
    shadow_review_json_path = paper_lane_daily_ops._expand_path(
        Path(_normalize_string(shadow_artifacts.get("review_json"), field_name="shadow_candidate.artifacts.review_json"))
    )
    if shadow_review_json_path is None:
        raise ValueError("stage2_shadow_compare shadow review_json path could not be resolved.")

    review_summary = shadow_candidate.get("review_summary")
    if not isinstance(review_summary, dict):
        review_summary = {}

    shadow_output = _load_json_object(shadow_output_json_path)
    template_output = shadow_output.get("template_output")
    if not isinstance(template_output, dict):
        raise ValueError(f"shadow candidate output is missing template_output: {shadow_output_json_path}")
    shadow_signal = template_output.get("signal")
    if not isinstance(shadow_signal, dict):
        raise ValueError(f"shadow candidate output is missing template_output.signal: {shadow_output_json_path}")

    candidate_outputs_dir = shadow_output_json_path.parent
    report_dir = candidate_outputs_dir.parent
    signals_dir = report_dir / "candidate_signals"
    shadow_signal_json_path = signals_dir / f"{shadow_strategy_id}_next_action.json"
    paper_lane_daily_ops._atomic_write_text(
        shadow_signal_json_path,
        json.dumps(shadow_signal, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
    )

    comparison = report.get("comparison")
    if not isinstance(comparison, dict):
        comparison = {}
    action_comparison = comparison.get("action_comparison")
    if not isinstance(action_comparison, dict):
        action_comparison = {}

    return {
        "report_json_path": report_json_path,
        "report_markdown_path": report_markdown_path,
        "scoreboard_csv_path": scoreboard_csv_path,
        "shadow_signal_json_path": shadow_signal_json_path,
        "shadow_review_json_path": shadow_review_json_path,
        "shadow_review_markdown_path": shadow_review_markdown_path,
        "report": report,
        "as_of_date": str(report.get("as_of_date", "")),
        "current_decision": str(report.get("current_decision", "")),
        "shadow_review_state": str(review_summary.get("shadow_review_state", "")),
        "shadow_automation_decision": str(review_summary.get("automation_decision", "")),
        "shadow_automation_status": str(review_summary.get("automation_status", "")),
        "primary_action": str(action_comparison.get("primary_action", "")),
        "primary_symbol": str(action_comparison.get("primary_symbol", "")),
        "shadow_action": str(action_comparison.get("shadow_action", "")),
        "shadow_symbol": str(action_comparison.get("shadow_symbol", "")),
        "shadow_next_rebalance": str(action_comparison.get("shadow_next_rebalance", "")),
    }


def _shadow_paper_base_dir(
    *,
    archive_root: Path,
    requested_base_dir: Path | None,
    state_key: str | None,
) -> Path | None:
    if requested_base_dir is not None:
        return requested_base_dir
    if state_key is None:
        return None
    return archive_root / "paper_lane" / paper_lane_daily_ops._safe_slug(
        state_key,
        fallback=DEFAULT_PAPER_STATE_KEY,
    )


def _shadow_paper_state_exists(*, state_key: str, paper_base_dir: Path | None) -> bool:
    paths = resolve_paper_lane_paths(state_key=state_key, base_dir=paper_base_dir, create=False)
    return paths.state_path.exists()


def _build_summary_row(
    *,
    run_id: str,
    timestamp,
    config: ShadowOpsConfig,
    provider: str,
    data_dir: Path,
    ops_paths: dict[str, Path],
    manifest_path: Path,
    step_results: dict[str, dict[str, Any]],
    compare_details: dict[str, Any] | None,
    overall_result: str,
    failed_step: str | None,
    no_op_reason: str | None,
    replay_auto_initialized: bool,
    replay_skipped_reason: str | None,
) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    active_pair = config.active_pair
    update_metrics = (step_results.get("update_data_eod") or {}).get("metrics") or {}
    status_json = (step_results.get("shadow_paper_lane_status") or {}).get("stdout_json") or {}
    apply_json = (step_results.get("shadow_paper_lane_apply") or {}).get("stdout_json") or {}
    init_json = (step_results.get("shadow_paper_lane_init") or {}).get("stdout_json") or {}
    status_paths = status_json.get("paths") if isinstance(status_json.get("paths"), dict) else {}
    apply_paths = apply_json.get("paths") if isinstance(apply_json.get("paths"), dict) else {}
    init_paths = init_json.get("paths") if isinstance(init_json.get("paths"), dict) else {}
    paper_paths = status_paths or apply_paths or init_paths
    signal = status_json.get("signal") if isinstance(status_json.get("signal"), dict) else {}
    if not signal and isinstance(apply_json.get("signal"), dict):
        signal = apply_json.get("signal")

    return {
        "schema_name": SUMMARY_SCHEMA_NAME,
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "run_id": run_id,
        "timestamp_chicago": timestamp.isoformat(),
        "ops_date": timestamp.date().isoformat(),
        "overall_result": overall_result,
        "failed_step": failed_step,
        "no_op_reason": no_op_reason or "",
        "pair_id": "" if active_pair is None else active_pair.pair_id,
        "primary_strategy_id": "" if active_pair is None else active_pair.primary_strategy_id,
        "shadow_strategy_id": "" if active_pair is None else active_pair.shadow_strategy_id,
        "provider": provider,
        "shadow_ops_config_path": str(config.path),
        "data_dir": str(data_dir),
        "local_replay_enabled": False if active_pair is None else active_pair.local_replay.enabled,
        "local_replay_state_key": (
            "" if active_pair is None or active_pair.local_replay.state_key is None else active_pair.local_replay.state_key
        ),
        "local_replay_auto_initialized": replay_auto_initialized,
        "replay_skipped_reason": replay_skipped_reason or "",
        "update_exit_code": (step_results.get("update_data_eod") or {}).get("exit_code", ""),
        "update_updated_symbols": update_metrics.get("updated_symbols", ""),
        "compare_exit_code": (step_results.get("stage2_shadow_compare") or {}).get("exit_code", ""),
        "compare_as_of_date": "" if compare_details is None else compare_details.get("as_of_date", ""),
        "compare_current_decision": "" if compare_details is None else compare_details.get("current_decision", ""),
        "compare_shadow_review_state": "" if compare_details is None else compare_details.get("shadow_review_state", ""),
        "compare_shadow_automation_decision": (
            "" if compare_details is None else compare_details.get("shadow_automation_decision", "")
        ),
        "compare_shadow_automation_status": (
            "" if compare_details is None else compare_details.get("shadow_automation_status", "")
        ),
        "compare_primary_action": "" if compare_details is None else compare_details.get("primary_action", ""),
        "compare_primary_symbol": "" if compare_details is None else compare_details.get("primary_symbol", ""),
        "compare_shadow_action": "" if compare_details is None else compare_details.get("shadow_action", ""),
        "compare_shadow_symbol": "" if compare_details is None else compare_details.get("shadow_symbol", ""),
        "compare_shadow_next_rebalance": (
            "" if compare_details is None else compare_details.get("shadow_next_rebalance", "")
        ),
        "compare_report_json": (
            "" if compare_details is None else str(compare_details.get("report_json_path", ""))
        ),
        "compare_report_markdown": (
            "" if compare_details is None else str(compare_details.get("report_markdown_path", ""))
        ),
        "compare_scoreboard_csv": (
            "" if compare_details is None else str(compare_details.get("scoreboard_csv_path", ""))
        ),
        "compare_shadow_signal_json": (
            "" if compare_details is None else str(compare_details.get("shadow_signal_json_path", ""))
        ),
        "compare_shadow_review_json": (
            "" if compare_details is None else str(compare_details.get("shadow_review_json_path", ""))
        ),
        "compare_shadow_review_markdown": (
            "" if compare_details is None else str(compare_details.get("shadow_review_markdown_path", ""))
        ),
        "replay_init_exit_code": (step_results.get("shadow_paper_lane_init") or {}).get("exit_code", ""),
        "replay_init_archive_manifest_path": init_json.get("archive_manifest_path", ""),
        "replay_status_exit_code": (step_results.get("shadow_paper_lane_status") or {}).get("exit_code", ""),
        "replay_status_signal_date": signal.get("date", ""),
        "replay_status_signal_action": signal.get("action", ""),
        "replay_status_signal_symbol": signal.get("symbol", ""),
        "replay_status_target_shares": signal.get("target_shares", ""),
        "replay_status_next_rebalance": signal.get("next_rebalance", ""),
        "replay_status_event_id": signal.get("event_id", ""),
        "replay_status_drift_present": status_json.get("drift_present", ""),
        "replay_status_event_already_applied": status_json.get("event_already_applied", ""),
        "replay_status_archive_manifest_path": status_json.get("archive_manifest_path", ""),
        "replay_apply_exit_code": (step_results.get("shadow_paper_lane_apply") or {}).get("exit_code", ""),
        "replay_apply_result": apply_json.get("result", ""),
        "replay_apply_duplicate_event_blocked": apply_json.get("duplicate_event_blocked", ""),
        "replay_apply_event_receipt_path": apply_json.get("event_receipt_path", ""),
        "replay_apply_archive_manifest_path": apply_json.get("archive_manifest_path", ""),
        "replay_state_path": paper_paths.get("state_path", ""),
        "replay_ledger_path": paper_paths.get("ledger_path", ""),
        "daily_ops_manifest_path": str(manifest_path),
        "daily_ops_jsonl_path": str(ops_paths["jsonl_path"]),
        "daily_ops_csv_path": str(ops_paths["csv_path"]),
        "daily_ops_xlsx_path": str(ops_paths["xlsx_path"]),
    }


def _render_summary_text(*, run_id: str, summary_row: dict[str, Any]) -> str:
    lines = [
        f"Stage 2 shadow daily ops run {run_id}",
        f"Result: {summary_row['overall_result']}",
    ]
    if summary_row["failed_step"]:
        lines.append(f"Failed step: {summary_row['failed_step']}")
    if summary_row["no_op_reason"]:
        lines.append(f"No-op reason: {summary_row['no_op_reason']}")
    if summary_row["pair_id"]:
        lines.extend(
            [
                f"Pair: {summary_row['pair_id']}",
                f"Current decision: {summary_row['compare_current_decision']}",
                (
                    "Shadow review: "
                    f"{summary_row['compare_shadow_review_state']} "
                    f"automation={summary_row['compare_shadow_automation_decision']} "
                    f"status={summary_row['compare_shadow_automation_status']}"
                ),
                (
                    "Latest comparison: "
                    f"primary={summary_row['compare_primary_action']}:{summary_row['compare_primary_symbol']} "
                    f"shadow={summary_row['compare_shadow_action']}:{summary_row['compare_shadow_symbol']}"
                ),
            ]
        )
    lines.append(
        f"Replay: enabled={summary_row['local_replay_enabled']} skipped_reason={summary_row['replay_skipped_reason']}"
    )
    lines.extend(
        [
            f"Update exit: {summary_row['update_exit_code']} (updated_symbols={summary_row['update_updated_symbols']})",
            f"Compare exit: {summary_row['compare_exit_code']}",
            f"Daily ops manifest: {summary_row['daily_ops_manifest_path']}",
            f"JSONL log: {summary_row['daily_ops_jsonl_path']}",
            f"CSV log: {summary_row['daily_ops_csv_path']}",
            f"XLSX workbook: {summary_row['daily_ops_xlsx_path']}",
        ]
    )
    return "\n".join(lines)


def _build_source_artifacts(
    *,
    config: ShadowOpsConfig,
    compare_details: dict[str, Any] | None,
    step_results: dict[str, dict[str, Any]],
) -> dict[str, Path]:
    artifacts: dict[str, Path] = {
        "shadow_ops_config": config.path,
    }
    if compare_details is not None:
        artifacts.update(
            {
                "compare_report_json": compare_details["report_json_path"],
                "compare_report_markdown": compare_details["report_markdown_path"],
                "compare_scoreboard_csv": compare_details["scoreboard_csv_path"],
                "shadow_signal_json": compare_details["shadow_signal_json_path"],
                "shadow_review_json": compare_details["shadow_review_json_path"],
                "shadow_review_markdown": compare_details["shadow_review_markdown_path"],
            }
        )
    for step_name, artifact_key in (
        ("shadow_paper_lane_init", "shadow_paper_lane_init_manifest"),
        ("shadow_paper_lane_status", "shadow_paper_lane_status_manifest"),
        ("shadow_paper_lane_apply", "shadow_paper_lane_apply_manifest"),
    ):
        payload = (step_results.get(step_name) or {}).get("stdout_json") or {}
        archive_manifest_path = payload.get("archive_manifest_path")
        if isinstance(archive_manifest_path, str) and archive_manifest_path.strip():
            artifacts[artifact_key] = Path(archive_manifest_path)
    return artifacts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the bounded Stage 2 shadow-only daily ops routine: refresh EOD data, "
            "refresh retained shadow comparison/report artifacts, and optionally replay the shadow signal "
            "through a separate local paper lane when explicitly configured."
        )
    )
    parser.add_argument(
        "--shadow-ops-config",
        type=Path,
        default=DEFAULT_SHADOW_OPS_CONFIG,
        help=f"Shadow ops config JSON path. Default: {DEFAULT_SHADOW_OPS_CONFIG}",
    )
    parser.add_argument(
        "--provider",
        choices=["stooq", "tiingo"],
        default=DEFAULT_PROVIDER,
        help=f"Data provider for update_data_eod. Default: {DEFAULT_PROVIDER}",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing cached parquet bars. Default: {DEFAULT_DATA_DIR}",
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Defaults to ~/.trading_codex, then ~/.cache/trading_codex, then /tmp/trading_codex.",
    )
    parser.add_argument(
        "--paper-base-dir",
        type=Path,
        default=None,
        help="Optional local shadow replay paper-lane state dir override.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format.")
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    try:
        timestamp = paper_lane_daily_ops._resolve_timestamp(args.timestamp)
        data_dir = _resolve_data_dir(args.data_dir)
        config = load_shadow_ops_config(args.shadow_ops_config)
        scope_key = _scope_key(config)
        ops_paths = resolve_ops_paths(
            scope_key=scope_key,
            archive_root=args.archive_root,
            create=True,
        )
    except Exception as exc:
        print(f"[stage2_shadow_daily_ops] ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        with _shadow_daily_ops_run_lock(lock_path=ops_paths["lock_path"], scope_key=scope_key):
            step_results: dict[str, dict[str, Any]] = {}
            compare_details: dict[str, Any] | None = None
            failed_step: str | None = None
            failed_exit_code = 0
            replay_auto_initialized = False
            replay_skipped_reason: str | None = None
            no_op_reason: str | None = None

            active_pair = config.active_pair
            resolved_paper_base_dir = _shadow_paper_base_dir(
                archive_root=ops_paths["archive_root"],
                requested_base_dir=paper_lane_daily_ops._expand_path(args.paper_base_dir),
                state_key=None if active_pair is None else active_pair.local_replay.state_key,
            )

            if active_pair is None:
                no_op_reason = "no_active_pair_configured"
            else:
                update_step = _run_step(
                    repo_root=repo_root,
                    step_name="update_data_eod",
                    cmd=paper_lane_daily_ops.build_update_data_eod_cmd(
                        repo_root=repo_root,
                        provider=args.provider,
                        data_dir=data_dir,
                        symbols=_default_update_symbols(),
                    ),
                    expect_json_stdout=False,
                    timestamp=timestamp,
                )
                step_results["update_data_eod"] = update_step
                if not update_step["success"]:
                    failed_step = "update_data_eod"
                    failed_exit_code = int(update_step["exit_code"]) or 2

                if failed_step is None:
                    compare_step = _run_step(
                        repo_root=repo_root,
                        step_name="stage2_shadow_compare",
                        cmd=build_stage2_shadow_compare_cmd(
                            repo_root=repo_root,
                            data_dir=data_dir,
                            artifacts_dir=ops_paths["archive_root"] / "stage2_shadow_compare" / paper_lane_daily_ops._safe_slug(
                                active_pair.pair_id,
                                fallback="shadow_pair",
                            ),
                        ),
                        expect_json_stdout=True,
                        timestamp=timestamp,
                    )
                    step_results["stage2_shadow_compare"] = compare_step
                    if not compare_step["success"]:
                        failed_step = "stage2_shadow_compare"
                        failed_exit_code = int(compare_step["exit_code"]) or 2

                if failed_step is None:
                    compare_summary = (step_results.get("stage2_shadow_compare") or {}).get("stdout_json") or {}
                    try:
                        compare_details = _load_compare_details(
                            compare_summary=compare_summary,
                            shadow_strategy_id=active_pair.shadow_strategy_id,
                        )
                    except Exception as exc:
                        failed_step = "stage2_shadow_compare_artifacts"
                        failed_exit_code = 2
                        step_results[failed_step] = _internal_failed_step(
                            step_name=failed_step,
                            message=str(exc),
                            timestamp=timestamp,
                        )

                if failed_step is None:
                    if not active_pair.local_replay.enabled:
                        replay_skipped_reason = "local_replay_disabled"
                    elif compare_details is None:
                        replay_skipped_reason = "compare_details_missing"
                    elif compare_details["shadow_automation_decision"] != "allow":
                        replay_skipped_reason = (
                            f"shadow_automation_decision_{compare_details['shadow_automation_decision']}"
                        )
                    else:
                        replay_state_key = active_pair.local_replay.state_key
                        if replay_state_key is None:
                            failed_step = "shadow_paper_lane_config"
                            failed_exit_code = 2
                            step_results[failed_step] = _internal_failed_step(
                                step_name=failed_step,
                                message="local replay is enabled but state_key is missing.",
                                timestamp=timestamp,
                            )
                        else:
                            if not _shadow_paper_state_exists(
                                state_key=replay_state_key,
                                paper_base_dir=resolved_paper_base_dir,
                            ):
                                init_step = _run_step(
                                    repo_root=repo_root,
                                    step_name="shadow_paper_lane_init",
                                    cmd=build_shadow_paper_lane_cmd(
                                        repo_root=repo_root,
                                        command="init",
                                        state_key=replay_state_key,
                                        paper_base_dir=resolved_paper_base_dir,
                                        timestamp=timestamp.isoformat(),
                                        starting_cash=active_pair.local_replay.starting_cash
                                        if active_pair.local_replay.starting_cash is not None
                                        else DEFAULT_PAPER_STARTING_CASH,
                                    ),
                                    expect_json_stdout=True,
                                    timestamp=timestamp,
                                )
                                step_results["shadow_paper_lane_init"] = init_step
                                if not init_step["success"]:
                                    failed_step = "shadow_paper_lane_init"
                                    failed_exit_code = int(init_step["exit_code"]) or 2
                                else:
                                    replay_auto_initialized = True

                            if failed_step is None and compare_details is not None:
                                status_step = _run_step(
                                    repo_root=repo_root,
                                    step_name="shadow_paper_lane_status",
                                    cmd=build_shadow_paper_lane_cmd(
                                        repo_root=repo_root,
                                        command="status",
                                        state_key=replay_state_key,
                                        paper_base_dir=resolved_paper_base_dir,
                                        timestamp=timestamp.isoformat(),
                                        signal_json_file=Path(compare_details["shadow_signal_json_path"]),
                                        data_dir=data_dir,
                                    ),
                                    expect_json_stdout=True,
                                    timestamp=timestamp,
                                )
                                step_results["shadow_paper_lane_status"] = status_step
                                if not status_step["success"]:
                                    failed_step = "shadow_paper_lane_status"
                                    failed_exit_code = int(status_step["exit_code"]) or 2

                            if failed_step is None and compare_details is not None:
                                apply_step = _run_step(
                                    repo_root=repo_root,
                                    step_name="shadow_paper_lane_apply",
                                    cmd=build_shadow_paper_lane_cmd(
                                        repo_root=repo_root,
                                        command="apply",
                                        state_key=replay_state_key,
                                        paper_base_dir=resolved_paper_base_dir,
                                        timestamp=timestamp.isoformat(),
                                        signal_json_file=Path(compare_details["shadow_signal_json_path"]),
                                        data_dir=data_dir,
                                    ),
                                    expect_json_stdout=True,
                                    timestamp=timestamp,
                                )
                                step_results["shadow_paper_lane_apply"] = apply_step
                                if not apply_step["success"]:
                                    failed_step = "shadow_paper_lane_apply"
                                    failed_exit_code = int(apply_step["exit_code"]) or 2

            overall_result = "failed" if failed_step else ("noop" if no_op_reason else "ok")

            manifest_fields: dict[str, Any] = {
                "failed_step": failed_step,
                "no_op_reason": no_op_reason,
                "provider": args.provider,
                "shadow_ops_config_path": str(config.path),
            }
            if active_pair is not None:
                manifest_fields.update(
                    {
                        "pair_id": active_pair.pair_id,
                        "primary_strategy_id": active_pair.primary_strategy_id,
                        "shadow_strategy_id": active_pair.shadow_strategy_id,
                        "local_replay_enabled": active_pair.local_replay.enabled,
                        "local_replay_state_key": active_pair.local_replay.state_key,
                    }
                )
            if compare_details is not None:
                manifest_fields.update(
                    {
                        "as_of_date": compare_details["as_of_date"],
                        "current_decision": compare_details["current_decision"],
                        "shadow_automation_decision": compare_details["shadow_automation_decision"],
                    }
                )

            archive = write_run_archive(
                timestamp=timestamp,
                run_kind="stage2_shadow_daily_ops",
                mode=overall_result,
                label=scope_key,
                identity_parts=[scope_key, args.provider, timestamp.date().isoformat()],
                manifest_fields=manifest_fields,
                source_artifacts=_build_source_artifacts(
                    config=config,
                    compare_details=compare_details,
                    step_results=step_results,
                ),
                json_artifacts={
                    "stage2_shadow_daily_ops_run": {
                        "schema_name": RUN_SCHEMA_NAME,
                        "schema_version": RUN_SCHEMA_VERSION,
                        "timestamp_chicago": timestamp.isoformat(),
                        "provider": args.provider,
                        "shadow_ops_config_path": str(config.path),
                        "shadow_ops_config": config.raw_payload,
                        "data_dir": str(data_dir),
                        "overall_result": overall_result,
                        "failed_step": failed_step,
                        "no_op_reason": no_op_reason,
                        "replay_auto_initialized": replay_auto_initialized,
                        "replay_skipped_reason": replay_skipped_reason,
                        "compare_details": compare_details,
                        "step_results": step_results,
                    },
                    **{step_name: payload for step_name, payload in step_results.items()},
                },
                text_artifacts={
                    "summary_text": "\n".join(
                        [
                            f"scope_key={scope_key}",
                            f"overall_result={overall_result}",
                            f"failed_step={failed_step or ''}",
                            f"no_op_reason={no_op_reason or ''}",
                            f"replay_skipped_reason={replay_skipped_reason or ''}",
                        ]
                    )
                },
                preferred_root=ops_paths["archive_root"],
            )

            summary_row = _build_summary_row(
                run_id=archive.manifest["run_id"],
                timestamp=timestamp,
                config=config,
                provider=args.provider,
                data_dir=data_dir,
                ops_paths=ops_paths,
                manifest_path=archive.paths.manifest_path,
                step_results=step_results,
                compare_details=compare_details,
                overall_result=overall_result,
                failed_step=failed_step,
                no_op_reason=no_op_reason,
                replay_auto_initialized=replay_auto_initialized,
                replay_skipped_reason=replay_skipped_reason,
            )

            paper_lane_daily_ops._append_jsonl_record(ops_paths["jsonl_path"], summary_row)
            all_rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
            _write_csv(ops_paths["csv_path"], rows=all_rows)
            _write_xlsx(ops_paths["xlsx_path"], rows=all_rows, timestamp=timestamp)

            if args.emit == "json":
                print(
                    json.dumps(
                        {
                            "schema_name": RUN_SCHEMA_NAME,
                            "schema_version": RUN_SCHEMA_VERSION,
                            "archive_manifest_path": str(archive.paths.manifest_path),
                            "summary": summary_row,
                        },
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                )
            else:
                print(_render_summary_text(run_id=archive.manifest["run_id"], summary_row=summary_row))

            if failed_step is not None:
                print(
                    f"[stage2_shadow_daily_ops] ERROR: step {failed_step} failed; see {archive.paths.manifest_path}",
                    file=sys.stderr,
                )
                return failed_exit_code
    except ShadowDailyOpsRunLockedError as exc:
        print(f"[stage2_shadow_daily_ops] ERROR: {exc}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
