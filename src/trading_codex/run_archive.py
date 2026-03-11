from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class RunArchivePaths:
    root_dir: Path
    day_dir: Path
    run_dir: Path
    artifacts_dir: Path
    manifest_path: Path
    index_path: Path


@dataclass(frozen=True)
class ArchivedRun:
    paths: RunArchivePaths
    manifest: dict[str, Any]


def archive_root_candidates(
    *,
    home_dir: Path | None = None,
    tmp_root: Path | None = None,
) -> list[Path]:
    home = home_dir or Path.home()
    temp_root = tmp_root or Path("/tmp")
    return [
        home / ".trading_codex",
        home / ".cache" / "trading_codex",
        temp_root / "trading_codex",
    ]


def resolve_archive_root(
    *,
    preferred_root: Path | None = None,
    home_dir: Path | None = None,
    tmp_root: Path | None = None,
    create: bool = True,
) -> Path:
    if preferred_root is None:
        env_root = os.getenv("TRADING_CODEX_ARCHIVE_ROOT")
        if env_root:
            preferred_root = Path(env_root).expanduser()

    if preferred_root is not None:
        if create:
            preferred_root.mkdir(parents=True, exist_ok=True)
        return preferred_root

    candidates = archive_root_candidates(home_dir=home_dir, tmp_root=tmp_root)
    if not create:
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]

    last_error: OSError | None = None
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError as exc:
            last_error = exc
            continue

    message = "Unable to create a durable Trading Codex archive root."
    if last_error is None:
        raise OSError(message)
    raise OSError(message) from last_error


def _normalize_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    return datetime.fromisoformat(value).replace(microsecond=0)


def _timestamp_slug(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%M%S%z")


def _safe_slug(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or fallback


def _jsonify(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(key): _jsonify(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, (list, tuple, set)):
        return [_jsonify(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def build_run_id(
    timestamp: str | datetime,
    *,
    run_kind: str,
    label: str | None = None,
    identity_parts: Sequence[object] | None = None,
) -> str:
    dt = _normalize_timestamp(timestamp)
    digest_payload = json.dumps(
        {
            "run_kind": run_kind,
            "label": label,
            "identity_parts": _jsonify(list(identity_parts or ())),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    digest = hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()[:10]
    slug_parts = [
        _safe_slug(run_kind, fallback="run"),
        _safe_slug(label, fallback="run"),
    ]
    slug = "_".join(part for part in slug_parts if part and part != "run")
    if len(slug) > 72:
        slug = slug[:72].rstrip("_")
    parts = [_timestamp_slug(dt)]
    if slug:
        parts.append(slug)
    parts.append(digest)
    return "_".join(parts)


def build_run_manifest(
    *,
    run_id: str,
    timestamp: str | datetime,
    run_kind: str,
    mode: str,
    artifact_paths: Mapping[str, str] | None = None,
    manifest_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    dt = _normalize_timestamp(timestamp)
    manifest: dict[str, Any] = {
        "schema_name": "run_manifest",
        "schema_version": 1,
        "run_id": run_id,
        "timestamp": dt.isoformat(),
        "date": dt.date().isoformat(),
        "run_kind": run_kind,
        "mode": mode,
        "artifact_paths": dict(artifact_paths or {}),
    }
    for key, value in (manifest_fields or {}).items():
        if value is None:
            continue
        manifest[key] = _jsonify(value)
    return manifest


def _build_paths(root_dir: Path, *, timestamp: datetime, requested_run_id: str) -> RunArchivePaths:
    day_dir = root_dir / "runs" / timestamp.date().isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)

    run_id = requested_run_id
    suffix = 2
    while (day_dir / run_id).exists():
        run_id = f"{requested_run_id}_{suffix}"
        suffix += 1

    run_dir = day_dir / run_id
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    index_dir = root_dir / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    return RunArchivePaths(
        root_dir=root_dir,
        day_dir=day_dir,
        run_dir=run_dir,
        artifacts_dir=artifacts_dir,
        manifest_path=run_dir / "manifest.json",
        index_path=index_dir / "runs.jsonl",
    )


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


def _artifact_relative_path(paths: RunArchivePaths, artifact_path: Path) -> str:
    return str(artifact_path.relative_to(paths.run_dir))


def _copy_source_artifact(
    *,
    key: str,
    source_path: Path,
    paths: RunArchivePaths,
) -> str:
    safe_key = _safe_slug(key, fallback="artifact")
    copied_path = paths.artifacts_dir / f"{safe_key}__{source_path.name}"
    shutil.copy2(source_path, copied_path)
    return _artifact_relative_path(paths, copied_path)


def _write_json_artifact(
    *,
    key: str,
    payload: Any,
    paths: RunArchivePaths,
) -> str:
    safe_key = _safe_slug(key, fallback="artifact")
    artifact_path = paths.artifacts_dir / f"{safe_key}.json"
    _write_json(artifact_path, _jsonify(payload))
    return _artifact_relative_path(paths, artifact_path)


def _write_text_artifact(
    *,
    key: str,
    content: str,
    paths: RunArchivePaths,
) -> str:
    safe_key = _safe_slug(key, fallback="artifact")
    artifact_path = paths.artifacts_dir / f"{safe_key}.txt"
    artifact_path.write_text(content if content.endswith("\n") else content + "\n", encoding="utf-8")
    return _artifact_relative_path(paths, artifact_path)


def write_run_archive(
    *,
    timestamp: str | datetime,
    run_kind: str,
    mode: str,
    label: str | None = None,
    identity_parts: Sequence[object] | None = None,
    manifest_fields: Mapping[str, Any] | None = None,
    source_artifacts: Mapping[str, Path | str] | None = None,
    json_artifacts: Mapping[str, Any] | None = None,
    text_artifacts: Mapping[str, str] | None = None,
    preferred_root: Path | None = None,
    home_dir: Path | None = None,
    tmp_root: Path | None = None,
) -> ArchivedRun:
    dt = _normalize_timestamp(timestamp)
    requested_run_id = build_run_id(
        dt,
        run_kind=run_kind,
        label=label,
        identity_parts=identity_parts,
    )
    root_dir = resolve_archive_root(
        preferred_root=preferred_root,
        home_dir=home_dir,
        tmp_root=tmp_root,
        create=True,
    )
    paths = _build_paths(root_dir, timestamp=dt, requested_run_id=requested_run_id)

    artifact_paths: dict[str, str] = {}
    missing_source_artifacts: dict[str, str] = {}
    for key, raw_path in (source_artifacts or {}).items():
        source_path = Path(raw_path)
        if not source_path.exists():
            missing_source_artifacts[key] = str(source_path)
            continue
        artifact_paths[key] = _copy_source_artifact(key=key, source_path=source_path, paths=paths)

    for key, payload in (json_artifacts or {}).items():
        artifact_paths[key] = _write_json_artifact(key=key, payload=payload, paths=paths)

    for key, content in (text_artifacts or {}).items():
        artifact_paths[key] = _write_text_artifact(key=key, content=content, paths=paths)

    manifest = build_run_manifest(
        run_id=paths.run_dir.name,
        timestamp=dt,
        run_kind=run_kind,
        mode=mode,
        artifact_paths=artifact_paths,
        manifest_fields={
            **(manifest_fields or {}),
            "archive_missing_artifacts": missing_source_artifacts or None,
        },
    )
    _write_json(paths.manifest_path, manifest)

    index_record = dict(manifest)
    index_record["manifest_path"] = str(paths.manifest_path.relative_to(paths.root_dir))
    with paths.index_path.open("a", encoding="utf-8", newline="") as fh:
        fh.write(json.dumps(index_record, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        fh.write("\n")

    return ArchivedRun(paths=paths, manifest=manifest)


def load_run_index(
    *,
    root_dir: Path | None = None,
    home_dir: Path | None = None,
    tmp_root: Path | None = None,
) -> list[dict[str, Any]]:
    resolved_root = resolve_archive_root(
        preferred_root=root_dir,
        home_dir=home_dir,
        tmp_root=tmp_root,
        create=False,
    )
    index_path = resolved_root / "index" / "runs.jsonl"
    if not index_path.exists():
        return []

    entries: list[dict[str, Any]] = []
    for raw_line in index_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def recent_runs(
    *,
    limit: int = 10,
    root_dir: Path | None = None,
    home_dir: Path | None = None,
    tmp_root: Path | None = None,
) -> list[dict[str, Any]]:
    entries = load_run_index(root_dir=root_dir, home_dir=home_dir, tmp_root=tmp_root)
    entries.sort(key=lambda item: str(item.get("timestamp", "")), reverse=True)
    return entries[: max(int(limit), 0)]


def resolve_manifest_path(entry: Mapping[str, Any], *, root_dir: Path | None = None) -> Path:
    resolved_root = resolve_archive_root(preferred_root=root_dir, create=False)
    manifest_path = entry.get("manifest_path")
    if isinstance(manifest_path, str) and manifest_path:
        return resolved_root / manifest_path

    date_value = entry.get("date")
    run_id = entry.get("run_id")
    if isinstance(date_value, str) and isinstance(run_id, str):
        return resolved_root / "runs" / date_value / run_id / "manifest.json"
    raise ValueError("Run index entry does not contain a resolvable manifest path.")
