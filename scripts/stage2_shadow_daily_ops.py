#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import errno
import hashlib
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
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID,
    PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
    ShadowStrategyRuntimeConfig,
    resolve_shadow_runtime_config,
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
MULTI_TARGET_SCOPE_PREFIX = "multi_target"
NO_TARGETS_CONFIGURED_REASON = "no_configured_targets"
STEP_SCHEMA_NAME = "stage2_shadow_daily_ops_step"
STEP_SCHEMA_VERSION = 1
RUN_SCHEMA_NAME = "stage2_shadow_daily_ops_run"
RUN_SCHEMA_VERSION = 2
SUMMARY_SCHEMA_NAME = "stage2_shadow_daily_ops_log_entry"
SUMMARY_SCHEMA_VERSION = 1
CONFIG_SCHEMA_NAME = "stage2_shadow_ops_config"
CONFIG_SCHEMA_VERSION = 2
LEGACY_CONFIG_SCHEMA_VERSION = 1
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
class ActiveShadowTargetConfig:
    target_id: str
    pair_id: str
    primary_strategy_id: str
    shadow_runtime: ShadowStrategyRuntimeConfig
    local_replay: ShadowReplayConfig


@dataclass(frozen=True)
class ShadowOpsConfig:
    path: Path
    targets: tuple[ActiveShadowTargetConfig, ...]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class ShadowTargetRunResult:
    target: ActiveShadowTargetConfig
    step_results: dict[str, dict[str, Any]]
    compare_details: dict[str, Any] | None
    failed_step: str | None
    failed_exit_code: int
    replay_auto_initialized: bool
    replay_skipped_reason: str | None


def _repo_root() -> Path:
    return REPO_ROOT


def _default_update_symbols() -> list[str]:
    symbols = list(PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS)
    symbols.append(PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL)
    return _dedupe_symbols(symbols)


def _update_symbols_for_targets(targets: tuple[ActiveShadowTargetConfig, ...]) -> list[str]:
    symbols = _default_update_symbols()
    for target in targets:
        symbols.extend(target.shadow_runtime.risk_symbols)
        symbols.append(target.shadow_runtime.defensive_symbol)
    return _dedupe_symbols(symbols)


def _dedupe_symbols(symbols: list[str] | tuple[str, ...]) -> list[str]:
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


def _coerce_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean.")
    return value


def _normalize_optional_string(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _normalize_string(value, field_name=field_name)


def _normalize_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc


def _normalize_optional_float(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number.") from exc


def _normalize_symbol_list(value: object, *, field_name: str) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be an array of strings.")
    return tuple(_dedupe_symbols([_normalize_string(item, field_name=f"{field_name}[]") for item in value]))


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


def _parse_local_replay(
    raw_local_replay: object,
    *,
    field_prefix: str,
) -> ShadowReplayConfig:
    if raw_local_replay is None:
        raw_local_replay = {}
    if not isinstance(raw_local_replay, dict):
        raise ValueError(f"{field_prefix} must be an object or null.")

    if "enabled" in raw_local_replay:
        enabled = _coerce_bool(
            raw_local_replay.get("enabled"),
            field_name=f"{field_prefix}.enabled",
        )
    else:
        enabled = False

    normalized_state_key = _normalize_optional_string(
        raw_local_replay.get("state_key"),
        field_name=f"{field_prefix}.state_key",
    )
    normalized_starting_cash = _normalize_optional_float(
        raw_local_replay.get("starting_cash"),
        field_name=f"{field_prefix}.starting_cash",
    )
    if normalized_starting_cash is not None and normalized_starting_cash <= 0.0:
        raise ValueError(f"{field_prefix}.starting_cash must be > 0.")

    if enabled:
        if normalized_state_key is None:
            raise ValueError(f"{field_prefix}.state_key is required when local replay is enabled.")
        if normalized_state_key == DEFAULT_PAPER_STATE_KEY:
            raise ValueError(
                f"{field_prefix}.state_key must stay separate from the primary local paper lane "
                f"({DEFAULT_PAPER_STATE_KEY!r})."
            )
        if normalized_starting_cash is None:
            raise ValueError(f"{field_prefix}.starting_cash is required when local replay is enabled.")

    return ShadowReplayConfig(
        enabled=enabled,
        state_key=normalized_state_key,
        starting_cash=normalized_starting_cash,
    )


def _build_shadow_runtime_from_payload(
    *,
    field_prefix: str,
    shadow_strategy_family: str,
    shadow_strategy_id: str,
    raw_shadow_parameters: object,
) -> ShadowStrategyRuntimeConfig:
    if raw_shadow_parameters is None:
        raw_shadow_parameters = {}
    if not isinstance(raw_shadow_parameters, dict):
        raise ValueError(f"{field_prefix}.shadow_parameters must be an object or null.")

    return resolve_shadow_runtime_config(
        shadow_strategy_family,
        strategy_id=shadow_strategy_id,
        symbols=_normalize_symbol_list(
            raw_shadow_parameters.get("risk_symbols"),
            field_name=f"{field_prefix}.shadow_parameters.risk_symbols",
        ),
        defensive_symbol=_normalize_optional_string(
            raw_shadow_parameters.get("defensive_symbol"),
            field_name=f"{field_prefix}.shadow_parameters.defensive_symbol",
        ),
        momentum_lookback=_normalize_optional_int(
            raw_shadow_parameters.get("momentum_lookback"),
            field_name=f"{field_prefix}.shadow_parameters.momentum_lookback",
        ),
        top_n=_normalize_optional_int(
            raw_shadow_parameters.get("top_n"),
            field_name=f"{field_prefix}.shadow_parameters.top_n",
        ),
        rebalance=_normalize_optional_int(
            raw_shadow_parameters.get("rebalance"),
            field_name=f"{field_prefix}.shadow_parameters.rebalance",
        ),
        vol_target=_normalize_optional_float(
            raw_shadow_parameters.get("vol_target"),
            field_name=f"{field_prefix}.shadow_parameters.vol_target",
        ),
        vol_lookback=_normalize_optional_int(
            raw_shadow_parameters.get("vol_lookback"),
            field_name=f"{field_prefix}.shadow_parameters.vol_lookback",
        ),
        vol_min=_normalize_optional_float(
            raw_shadow_parameters.get("vol_min"),
            field_name=f"{field_prefix}.shadow_parameters.vol_min",
        ),
        vol_max=_normalize_optional_float(
            raw_shadow_parameters.get("vol_max"),
            field_name=f"{field_prefix}.shadow_parameters.vol_max",
        ),
        vol_update=_normalize_optional_string(
            raw_shadow_parameters.get("vol_update"),
            field_name=f"{field_prefix}.shadow_parameters.vol_update",
        ),
    )


def _validate_target(
    *,
    target: ActiveShadowTargetConfig,
    field_prefix: str,
) -> None:
    if target.primary_strategy_id != PRIMARY_LIVE_CANDIDATE_V1_ID:
        raise ValueError(
            f"{field_prefix}.primary_strategy_id must stay on the approved primary candidate "
            f"{PRIMARY_LIVE_CANDIDATE_V1_ID!r}."
        )
    if target.shadow_runtime.primary_candidate_mapping.strategy_id != target.primary_strategy_id:
        raise ValueError(
            f"{field_prefix}.shadow_strategy_family must remain compatible with primary strategy "
            f"{target.primary_strategy_id!r}."
        )


def _parse_target(raw_target: dict[str, Any], *, index: int) -> ActiveShadowTargetConfig:
    field_prefix = f"targets[{index}]"
    target_id = _normalize_optional_string(raw_target.get("target_id"), field_name=f"{field_prefix}.target_id")
    pair_id = _normalize_string(raw_target.get("pair_id"), field_name=f"{field_prefix}.pair_id")
    primary_strategy_id = _normalize_string(
        raw_target.get("primary_strategy_id"),
        field_name=f"{field_prefix}.primary_strategy_id",
    )
    shadow_strategy_id = _normalize_string(
        raw_target.get("shadow_strategy_id"),
        field_name=f"{field_prefix}.shadow_strategy_id",
    )
    shadow_strategy_family = _normalize_string(
        raw_target.get("shadow_strategy_family"),
        field_name=f"{field_prefix}.shadow_strategy_family",
    )
    shadow_runtime = _build_shadow_runtime_from_payload(
        field_prefix=field_prefix,
        shadow_strategy_family=shadow_strategy_family,
        shadow_strategy_id=shadow_strategy_id,
        raw_shadow_parameters=raw_target.get("shadow_parameters"),
    )
    target = ActiveShadowTargetConfig(
        target_id=pair_id if target_id is None else target_id,
        pair_id=pair_id,
        primary_strategy_id=primary_strategy_id,
        shadow_runtime=shadow_runtime,
        local_replay=_parse_local_replay(
            raw_target.get("local_replay"),
            field_prefix=f"{field_prefix}.local_replay",
        ),
    )
    _validate_target(target=target, field_prefix=field_prefix)
    return target


def _parse_legacy_active_pair(raw_active_pair: dict[str, Any]) -> ActiveShadowTargetConfig:
    pair_id = _normalize_string(raw_active_pair.get("pair_id"), field_name="active_pair.pair_id")
    primary_strategy_id = _normalize_string(
        raw_active_pair.get("primary_strategy_id"),
        field_name="active_pair.primary_strategy_id",
    )
    shadow_strategy_id = _normalize_string(
        raw_active_pair.get("shadow_strategy_id"),
        field_name="active_pair.shadow_strategy_id",
    )
    target = ActiveShadowTargetConfig(
        target_id=pair_id,
        pair_id=pair_id,
        primary_strategy_id=primary_strategy_id,
        shadow_runtime=resolve_shadow_runtime_config(
            PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID,
            strategy_id=shadow_strategy_id,
        ),
        local_replay=_parse_local_replay(
            raw_active_pair.get("local_replay"),
            field_prefix="active_pair.local_replay",
        ),
    )
    _validate_target(target=target, field_prefix="active_pair")
    return target


def _validate_unique_targets(targets: tuple[ActiveShadowTargetConfig, ...]) -> None:
    seen_pair_ids: set[str] = set()
    seen_target_ids: set[str] = set()
    seen_replay_state_keys: set[str] = set()
    for target in targets:
        if target.pair_id in seen_pair_ids:
            raise ValueError(f"shadow ops config pair_id must be unique: {target.pair_id!r}")
        seen_pair_ids.add(target.pair_id)
        if target.target_id in seen_target_ids:
            raise ValueError(f"shadow ops config target_id must be unique: {target.target_id!r}")
        seen_target_ids.add(target.target_id)
        if target.local_replay.enabled and target.local_replay.state_key is not None:
            if target.local_replay.state_key in seen_replay_state_keys:
                raise ValueError(
                    "shadow ops local replay state_key must be unique across enabled targets: "
                    f"{target.local_replay.state_key!r}"
                )
            seen_replay_state_keys.add(target.local_replay.state_key)


def load_shadow_ops_config(config_path: Path) -> ShadowOpsConfig:
    resolved_path = paper_lane_daily_ops._expand_path(config_path)
    if resolved_path is None:
        raise ValueError("shadow ops config path must not be empty.")

    payload = _load_json_object(resolved_path)
    schema_name = payload.get("schema_name")
    if schema_name != CONFIG_SCHEMA_NAME:
        raise ValueError(f"shadow ops config schema_name must be {CONFIG_SCHEMA_NAME!r}.")
    schema_version = payload.get("schema_version")
    if schema_version not in {LEGACY_CONFIG_SCHEMA_VERSION, CONFIG_SCHEMA_VERSION}:
        raise ValueError(
            "shadow ops config schema_version must be "
            f"{LEGACY_CONFIG_SCHEMA_VERSION} or {CONFIG_SCHEMA_VERSION}."
        )

    if schema_version == LEGACY_CONFIG_SCHEMA_VERSION:
        raw_active_pair = payload.get("active_pair")
        if raw_active_pair is None:
            return ShadowOpsConfig(path=resolved_path, targets=(), raw_payload=payload)
        if not isinstance(raw_active_pair, dict):
            raise ValueError("shadow ops config active_pair must be an object or null.")
        targets = (_parse_legacy_active_pair(raw_active_pair),)
        _validate_unique_targets(targets)
        return ShadowOpsConfig(path=resolved_path, targets=targets, raw_payload=payload)

    raw_targets = payload.get("targets")
    if raw_targets is None:
        raise ValueError("shadow ops config targets must be an array.")
    if not isinstance(raw_targets, list):
        raise ValueError("shadow ops config targets must be an array.")
    targets = tuple(
        _parse_target(raw_target, index=index)
        for index, raw_target in enumerate(raw_targets)
        if isinstance(raw_target, dict)
    )
    if len(targets) != len(raw_targets):
        raise ValueError("shadow ops config targets entries must be objects.")
    _validate_unique_targets(targets)
    return ShadowOpsConfig(path=resolved_path, targets=targets, raw_payload=payload)


def _runner_scope_key(config: ShadowOpsConfig) -> str:
    if not config.targets:
        return UNCONFIGURED_SCOPE_KEY
    if len(config.targets) == 1:
        return config.targets[0].pair_id
    digest = hashlib.sha1(
        "|".join(target.target_id for target in config.targets).encode("utf-8")
    ).hexdigest()[:12]
    return f"{MULTI_TARGET_SCOPE_PREFIX}_{len(config.targets)}_{digest}"


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
    target: ActiveShadowTargetConfig,
    data_dir: Path,
    artifacts_dir: Path,
) -> list[str]:
    return [
        sys.executable,
        str(repo_root / "scripts" / "stage2_shadow_compare.py"),
        "--pair-id",
        target.pair_id,
        "--data-dir",
        str(data_dir),
        "--artifacts-dir",
        str(artifacts_dir),
        "--shadow-strategy-family",
        target.shadow_runtime.template_family_id,
        "--shadow-strategy-id",
        target.shadow_runtime.strategy_id,
        "--shadow-risk-symbols",
        ",".join(target.shadow_runtime.risk_symbols),
        "--shadow-defensive-symbol",
        target.shadow_runtime.defensive_symbol,
        "--shadow-momentum-lookback",
        str(target.shadow_runtime.momentum_lookback),
        "--shadow-top-n",
        str(target.shadow_runtime.top_n),
        "--shadow-rebalance",
        str(target.shadow_runtime.rebalance),
        "--shadow-vol-target",
        str(target.shadow_runtime.vol_target),
        "--shadow-vol-lookback",
        str(target.shadow_runtime.vol_lookback),
        "--shadow-vol-min",
        str(target.shadow_runtime.vol_min),
        "--shadow-vol-max",
        str(target.shadow_runtime.vol_max),
        "--shadow-vol-update",
        target.shadow_runtime.vol_update,
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
        if state_key is None:
            return requested_base_dir
        return requested_base_dir / paper_lane_daily_ops._safe_slug(
            state_key,
            fallback=DEFAULT_PAPER_STATE_KEY,
        )
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
    config_path: Path,
    target: ActiveShadowTargetConfig | None,
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
        "pair_id": "" if target is None else target.pair_id,
        "primary_strategy_id": "" if target is None else target.primary_strategy_id,
        "shadow_strategy_id": "" if target is None else target.shadow_runtime.strategy_id,
        "provider": provider,
        "shadow_ops_config_path": str(config_path),
        "data_dir": str(data_dir),
        "local_replay_enabled": False if target is None else target.local_replay.enabled,
        "local_replay_state_key": (
            "" if target is None or target.local_replay.state_key is None else target.local_replay.state_key
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


def _target_runtime_payload(target: ActiveShadowTargetConfig) -> dict[str, Any]:
    return {
        "target_id": target.target_id,
        "pair_id": target.pair_id,
        "primary_strategy_id": target.primary_strategy_id,
        "shadow_strategy_id": target.shadow_runtime.strategy_id,
        "shadow_strategy_family": target.shadow_runtime.template_family_id,
        "shadow_runtime_strategy": target.shadow_runtime.implementation_strategy,
        "shadow_implementation_label": target.shadow_runtime.implementation_label,
        "shadow_parameters": {
            "risk_symbols": list(target.shadow_runtime.risk_symbols),
            "defensive_symbol": target.shadow_runtime.defensive_symbol,
            "momentum_lookback": target.shadow_runtime.momentum_lookback,
            "top_n": target.shadow_runtime.top_n,
            "rebalance": target.shadow_runtime.rebalance,
            "vol_target": target.shadow_runtime.vol_target,
            "vol_lookback": target.shadow_runtime.vol_lookback,
            "vol_min": target.shadow_runtime.vol_min,
            "vol_max": target.shadow_runtime.vol_max,
            "vol_update": target.shadow_runtime.vol_update,
        },
        "local_replay": {
            "enabled": target.local_replay.enabled,
            "state_key": target.local_replay.state_key,
            "starting_cash": target.local_replay.starting_cash,
        },
    }


def _build_run_summary(
    *,
    run_id: str,
    timestamp,
    config: ShadowOpsConfig,
    runner_scope_key: str,
    provider: str,
    data_dir: Path,
    manifest_path: Path,
    overall_result: str,
    failed_step: str | None,
    no_op_reason: str | None,
    target_results: list[ShadowTargetRunResult],
    skipped_targets: tuple[ActiveShadowTargetConfig, ...],
) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    failed_target = next((result.target for result in target_results if result.failed_step is not None), None)
    return {
        "schema_name": RUN_SCHEMA_NAME,
        "schema_version": RUN_SCHEMA_VERSION,
        "run_id": run_id,
        "timestamp_chicago": timestamp.isoformat(),
        "ops_date": timestamp.date().isoformat(),
        "overall_result": overall_result,
        "failed_step": failed_step or "",
        "failed_pair_id": "" if failed_target is None else failed_target.pair_id,
        "no_op_reason": no_op_reason or "",
        "runner_scope_key": runner_scope_key,
        "configured_target_count": len(config.targets),
        "completed_target_count": len(target_results),
        "skipped_target_ids": [target.target_id for target in skipped_targets],
        "pair_ids": [target.pair_id for target in config.targets],
        "provider": provider,
        "shadow_ops_config_path": str(config.path),
        "data_dir": str(data_dir),
        "daily_ops_manifest_path": str(manifest_path),
    }


def _render_run_text(
    *,
    run_id: str,
    run_summary: dict[str, Any],
    target_summaries: list[dict[str, Any]],
) -> str:
    lines = [
        f"Stage 2 shadow daily ops run {run_id}",
        f"Result: {run_summary['overall_result']}",
        f"Configured targets: {run_summary['configured_target_count']}",
        f"Completed target runs: {run_summary['completed_target_count']}",
    ]
    if run_summary["failed_step"]:
        lines.append(f"Failed step: {run_summary['failed_step']}")
    if run_summary["no_op_reason"]:
        lines.append(f"No-op reason: {run_summary['no_op_reason']}")
    for summary_row in target_summaries:
        lines.append(
            "Target "
            f"{summary_row['pair_id']}: result={summary_row['overall_result']} "
            f"decision={summary_row['compare_current_decision']} "
            f"replay={summary_row['replay_skipped_reason'] or summary_row['replay_apply_result'] or 'n/a'}"
        )
    skipped_target_ids = run_summary.get("skipped_target_ids") or []
    if skipped_target_ids:
        lines.append(f"Skipped targets: {', '.join(skipped_target_ids)}")
    lines.append(f"Daily ops manifest: {run_summary['daily_ops_manifest_path']}")
    return "\n".join(lines)


def _build_source_artifacts(
    *,
    config_path: Path,
    target_results: list[ShadowTargetRunResult],
) -> dict[str, Path]:
    artifacts: dict[str, Path] = {
        "shadow_ops_config": config_path,
    }
    for target_result in target_results:
        prefix = paper_lane_daily_ops._safe_slug(target_result.target.pair_id, fallback="shadow_pair")
        compare_details = target_result.compare_details
        step_results = target_result.step_results
        if compare_details is not None:
            artifacts.update(
                {
                    f"{prefix}_compare_report_json": compare_details["report_json_path"],
                    f"{prefix}_compare_report_markdown": compare_details["report_markdown_path"],
                    f"{prefix}_compare_scoreboard_csv": compare_details["scoreboard_csv_path"],
                    f"{prefix}_shadow_signal_json": compare_details["shadow_signal_json_path"],
                    f"{prefix}_shadow_review_json": compare_details["shadow_review_json_path"],
                    f"{prefix}_shadow_review_markdown": compare_details["shadow_review_markdown_path"],
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
                artifacts[f"{prefix}_{artifact_key}"] = Path(archive_manifest_path)
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


def _run_shadow_target(
    *,
    repo_root: Path,
    timestamp,
    data_dir: Path,
    archive_root: Path,
    requested_paper_base_dir: Path | None,
    target: ActiveShadowTargetConfig,
    shared_update_step: dict[str, Any],
) -> ShadowTargetRunResult:  # type: ignore[no-untyped-def]
    step_results: dict[str, dict[str, Any]] = {
        "update_data_eod": shared_update_step,
    }
    compare_details: dict[str, Any] | None = None
    failed_step: str | None = None
    failed_exit_code = 0
    replay_auto_initialized = False
    replay_skipped_reason: str | None = None

    if not shared_update_step.get("success"):
        failed_step = "update_data_eod"
        failed_exit_code = int(shared_update_step.get("exit_code") or 2)
        return ShadowTargetRunResult(
            target=target,
            step_results=step_results,
            compare_details=compare_details,
            failed_step=failed_step,
            failed_exit_code=failed_exit_code,
            replay_auto_initialized=replay_auto_initialized,
            replay_skipped_reason=replay_skipped_reason,
        )

    resolved_paper_base_dir = _shadow_paper_base_dir(
        archive_root=archive_root,
        requested_base_dir=requested_paper_base_dir,
        state_key=target.local_replay.state_key,
    )

    compare_step = _run_step(
        repo_root=repo_root,
        step_name="stage2_shadow_compare",
        cmd=build_stage2_shadow_compare_cmd(
            repo_root=repo_root,
            target=target,
            data_dir=data_dir,
            artifacts_dir=archive_root / "stage2_shadow_compare" / paper_lane_daily_ops._safe_slug(
                target.pair_id,
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
                shadow_strategy_id=target.shadow_runtime.strategy_id,
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
        if not target.local_replay.enabled:
            replay_skipped_reason = "local_replay_disabled"
        elif compare_details is None:
            replay_skipped_reason = "compare_details_missing"
        elif compare_details["shadow_automation_decision"] != "allow":
            replay_skipped_reason = (
                f"shadow_automation_decision_{compare_details['shadow_automation_decision']}"
            )
        else:
            replay_state_key = target.local_replay.state_key
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
                            starting_cash=target.local_replay.starting_cash
                            if target.local_replay.starting_cash is not None
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

    return ShadowTargetRunResult(
        target=target,
        step_results=step_results,
        compare_details=compare_details,
        failed_step=failed_step,
        failed_exit_code=failed_exit_code,
        replay_auto_initialized=replay_auto_initialized,
        replay_skipped_reason=replay_skipped_reason,
    )


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    try:
        timestamp = paper_lane_daily_ops._resolve_timestamp(args.timestamp)
        data_dir = _resolve_data_dir(args.data_dir)
        config = load_shadow_ops_config(args.shadow_ops_config)
        runner_scope_key = _runner_scope_key(config)
        runner_ops_paths = resolve_ops_paths(
            scope_key=runner_scope_key,
            archive_root=args.archive_root,
            create=True,
        )
        requested_paper_base_dir = paper_lane_daily_ops._expand_path(args.paper_base_dir)
    except Exception as exc:
        print(f"[stage2_shadow_daily_ops] ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        with _shadow_daily_ops_run_lock(lock_path=runner_ops_paths["lock_path"], scope_key=runner_scope_key):
            failed_step: str | None = None
            failed_exit_code = 0
            no_op_reason: str | None = None
            target_results: list[ShadowTargetRunResult] = []

            if not config.targets:
                no_op_reason = NO_TARGETS_CONFIGURED_REASON
            else:
                update_step = _run_step(
                    repo_root=repo_root,
                    step_name="update_data_eod",
                    cmd=paper_lane_daily_ops.build_update_data_eod_cmd(
                        repo_root=repo_root,
                        provider=args.provider,
                        data_dir=data_dir,
                        symbols=_update_symbols_for_targets(config.targets),
                    ),
                    expect_json_stdout=False,
                    timestamp=timestamp,
                )
                shared_update_failed = not update_step["success"]
                for target in config.targets:
                    if failed_step is not None and not shared_update_failed:
                        break
                    target_result = _run_shadow_target(
                        repo_root=repo_root,
                        timestamp=timestamp,
                        data_dir=data_dir,
                        archive_root=runner_ops_paths["archive_root"],
                        requested_paper_base_dir=requested_paper_base_dir,
                        target=target,
                        shared_update_step=update_step,
                    )
                    target_results.append(target_result)
                    if target_result.failed_step is not None:
                        failed_step = target_result.failed_step
                        failed_exit_code = target_result.failed_exit_code
                        if not shared_update_failed:
                            break

            skipped_targets = config.targets[len(target_results) :]
            overall_result = "failed" if failed_step else ("noop" if no_op_reason else "ok")

            manifest_fields: dict[str, Any] = {
                "failed_step": failed_step,
                "no_op_reason": no_op_reason,
                "provider": args.provider,
                "shadow_ops_config_path": str(config.path),
                "configured_target_count": len(config.targets),
                "completed_target_count": len(target_results),
            }
            if config.targets:
                manifest_fields["pair_ids"] = [target.pair_id for target in config.targets]

            archive = write_run_archive(
                timestamp=timestamp,
                run_kind="stage2_shadow_daily_ops",
                mode=overall_result,
                label=runner_scope_key,
                identity_parts=[runner_scope_key, args.provider, timestamp.date().isoformat()],
                manifest_fields=manifest_fields,
                source_artifacts=_build_source_artifacts(
                    config_path=config.path,
                    target_results=target_results,
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
                        "runner_scope_key": runner_scope_key,
                        "configured_target_count": len(config.targets),
                        "completed_target_count": len(target_results),
                        "skipped_target_ids": [target.target_id for target in skipped_targets],
                        "targets": [
                            {
                                **_target_runtime_payload(result.target),
                                "overall_result": "failed" if result.failed_step else "ok",
                                "failed_step": result.failed_step,
                                "replay_auto_initialized": result.replay_auto_initialized,
                                "replay_skipped_reason": result.replay_skipped_reason,
                                "compare_details": result.compare_details,
                                "step_results": result.step_results,
                            }
                            for result in target_results
                        ],
                    }
                },
                text_artifacts={
                    "summary_text": "\n".join(
                        [
                            f"scope_key={runner_scope_key}",
                            f"overall_result={overall_result}",
                            f"failed_step={failed_step or ''}",
                            f"no_op_reason={no_op_reason or ''}",
                        ]
                    )
                },
                preferred_root=runner_ops_paths["archive_root"],
            )

            target_summaries: list[dict[str, Any]] = []
            emitted_summary: dict[str, Any]
            if not config.targets:
                noop_ops_paths = resolve_ops_paths(
                    scope_key=UNCONFIGURED_SCOPE_KEY,
                    archive_root=runner_ops_paths["archive_root"],
                    create=True,
                )
                emitted_summary = _build_summary_row(
                    run_id=archive.manifest["run_id"],
                    timestamp=timestamp,
                    config_path=config.path,
                    target=None,
                    provider=args.provider,
                    data_dir=data_dir,
                    ops_paths=noop_ops_paths,
                    manifest_path=archive.paths.manifest_path,
                    step_results={},
                    compare_details=None,
                    overall_result=overall_result,
                    failed_step=failed_step,
                    no_op_reason=no_op_reason,
                    replay_auto_initialized=False,
                    replay_skipped_reason=None,
                )
                paper_lane_daily_ops._append_jsonl_record(noop_ops_paths["jsonl_path"], emitted_summary)
                noop_rows = paper_lane_daily_ops._load_jsonl_records(noop_ops_paths["jsonl_path"])
                _write_csv(noop_ops_paths["csv_path"], rows=noop_rows)
                _write_xlsx(noop_ops_paths["xlsx_path"], rows=noop_rows, timestamp=timestamp)
            else:
                for result in target_results:
                    target_ops_paths = resolve_ops_paths(
                        scope_key=result.target.pair_id,
                        archive_root=runner_ops_paths["archive_root"],
                        create=True,
                    )
                    summary_row = _build_summary_row(
                        run_id=archive.manifest["run_id"],
                        timestamp=timestamp,
                        config_path=config.path,
                        target=result.target,
                        provider=args.provider,
                        data_dir=data_dir,
                        ops_paths=target_ops_paths,
                        manifest_path=archive.paths.manifest_path,
                        step_results=result.step_results,
                        compare_details=result.compare_details,
                        overall_result="failed" if result.failed_step else "ok",
                        failed_step=result.failed_step,
                        no_op_reason=None,
                        replay_auto_initialized=result.replay_auto_initialized,
                        replay_skipped_reason=result.replay_skipped_reason,
                    )
                    paper_lane_daily_ops._append_jsonl_record(target_ops_paths["jsonl_path"], summary_row)
                    target_rows = paper_lane_daily_ops._load_jsonl_records(target_ops_paths["jsonl_path"])
                    _write_csv(target_ops_paths["csv_path"], rows=target_rows)
                    _write_xlsx(target_ops_paths["xlsx_path"], rows=target_rows, timestamp=timestamp)
                    target_summaries.append(summary_row)
                emitted_summary = target_summaries[0] if len(target_summaries) == 1 else _build_run_summary(
                    run_id=archive.manifest["run_id"],
                    timestamp=timestamp,
                    config=config,
                    runner_scope_key=runner_scope_key,
                    provider=args.provider,
                    data_dir=data_dir,
                    manifest_path=archive.paths.manifest_path,
                    overall_result=overall_result,
                    failed_step=failed_step,
                    no_op_reason=no_op_reason,
                    target_results=target_results,
                    skipped_targets=skipped_targets,
                )

            run_summary = _build_run_summary(
                run_id=archive.manifest["run_id"],
                timestamp=timestamp,
                config=config,
                runner_scope_key=runner_scope_key,
                provider=args.provider,
                data_dir=data_dir,
                manifest_path=archive.paths.manifest_path,
                overall_result=overall_result,
                failed_step=failed_step,
                no_op_reason=no_op_reason,
                target_results=target_results,
                skipped_targets=skipped_targets,
            )

            if args.emit == "json":
                print(
                    json.dumps(
                        {
                            "schema_name": RUN_SCHEMA_NAME,
                            "schema_version": RUN_SCHEMA_VERSION,
                            "archive_manifest_path": str(archive.paths.manifest_path),
                            "summary": emitted_summary,
                            "run_summary": run_summary,
                            "target_summaries": target_summaries,
                        },
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                )
            else:
                if len(target_summaries) <= 1:
                    print(_render_summary_text(run_id=archive.manifest["run_id"], summary_row=emitted_summary))
                else:
                    print(
                        _render_run_text(
                            run_id=archive.manifest["run_id"],
                            run_summary=run_summary,
                            target_summaries=target_summaries,
                        )
                    )

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
