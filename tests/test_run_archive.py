from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from trading_codex.run_archive import (
    build_run_id,
    build_run_manifest,
    load_run_index,
    recent_runs,
    resolve_archive_root,
    resolve_manifest_path,
    write_run_archive,
)


def test_resolve_archive_root_falls_back_to_tmp_when_home_candidates_blocked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("TRADING_CODEX_ARCHIVE_ROOT", raising=False)
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    (home_dir / ".trading_codex").write_text("blocked", encoding="utf-8")
    cache_dir = home_dir / ".cache"
    cache_dir.mkdir()
    (cache_dir / "trading_codex").write_text("blocked", encoding="utf-8")

    tmp_root = tmp_path / "tmp"
    tmp_root.mkdir()

    resolved = resolve_archive_root(home_dir=home_dir, tmp_root=tmp_root)
    assert resolved == tmp_root / "trading_codex"
    assert resolved.exists()


def test_build_run_id_and_manifest_are_stable() -> None:
    timestamp = "2026-03-09T10:45:00-05:00"
    run_id = build_run_id(
        timestamp,
        run_kind="execution_plan",
        label="dual_mom_signal",
        identity_parts=["2026-03-09:dual_mom:RESIZE:EFA:100:100:2026-03-31", "sha256"],
    )
    assert run_id == build_run_id(
        timestamp,
        run_kind="execution_plan",
        label="dual_mom_signal",
        identity_parts=["2026-03-09:dual_mom:RESIZE:EFA:100:100:2026-03-31", "sha256"],
    )
    assert run_id.startswith("20260309T104500-0500_execution_plan_dual_mom_signal_")

    manifest = build_run_manifest(
        run_id=run_id,
        timestamp=timestamp,
        run_kind="execution_plan",
        mode="managed_sleeve",
        artifact_paths={"execution_plan_json": "artifacts/execution_plan_json__plan.json"},
        manifest_fields={
            "strategy": "dual_mom",
            "action": "RESIZE",
            "warnings": ["warning_a"],
        },
    )
    assert manifest["run_id"] == run_id
    assert manifest["timestamp"] == timestamp
    assert manifest["mode"] == "managed_sleeve"
    assert manifest["warnings"] == ["warning_a"]
    assert manifest["artifact_paths"]["execution_plan_json"] == "artifacts/execution_plan_json__plan.json"


def test_write_run_archive_writes_layout_manifest_and_index(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    source_markdown = tmp_path / "review.md"
    source_markdown.write_text("# Review\n", encoding="utf-8")

    archived = write_run_archive(
        timestamp="2026-03-09T10:45:00-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_signal",
        identity_parts=["event-1", "sha-1"],
        manifest_fields={
            "strategy": "dual_mom",
            "symbol": "EFA",
            "action": "RESIZE",
            "plan_sha256": "sha-1",
        },
        source_artifacts={"review_markdown": source_markdown},
        json_artifacts={"signal_payload": {"event_id": "event-1"}},
        text_artifacts={"emitted_line": "one line only"},
        preferred_root=archive_root,
    )

    manifest = json.loads(archived.paths.manifest_path.read_text(encoding="utf-8"))
    assert archived.paths.run_dir.parent == archive_root / "runs" / "2026-03-09"
    assert manifest["run_id"] == archived.paths.run_dir.name
    assert manifest["plan_sha256"] == "sha-1"
    assert manifest["artifact_paths"]["review_markdown"].startswith("artifacts/")
    assert manifest["artifact_paths"]["signal_payload"] == "artifacts/signal_payload.json"
    assert manifest["artifact_paths"]["emitted_line"] == "artifacts/emitted_line.txt"
    assert (archived.paths.run_dir / manifest["artifact_paths"]["review_markdown"]).exists()
    assert (archived.paths.run_dir / manifest["artifact_paths"]["signal_payload"]).exists()

    index_entries = recent_runs(root_dir=archive_root, limit=5)
    assert len(index_entries) == 1
    assert index_entries[0]["run_id"] == archived.paths.run_dir.name
    assert resolve_manifest_path(index_entries[0], root_dir=archive_root) == archived.paths.manifest_path


def test_load_run_index_skips_malformed_and_partial_lines(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    index_path = archive_root / "index" / "runs.jsonl"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    valid_old = {
        "run_id": "run-old",
        "date": "2026-03-09",
        "timestamp": "2026-03-09T08:25:00-05:00",
        "run_kind": "next_action_alert",
        "mode": "change_only",
    }
    valid_new = {
        "run_id": "run-new",
        "date": "2026-03-09",
        "timestamp": "2026-03-09T10:45:00-05:00",
        "run_kind": "execution_plan",
        "mode": "managed_sleeve",
    }
    index_path.write_text(
        "\n".join(
            [
                json.dumps(valid_old, separators=(",", ":"), sort_keys=True),
                '{"broken_json":',
                "not-json",
                json.dumps(valid_new, separators=(",", ":"), sort_keys=True),
                '{"partial":true',
            ]
        ),
        encoding="utf-8",
    )

    entries = recent_runs(root_dir=archive_root, limit=10)
    assert [entry["run_id"] for entry in entries] == ["run-new", "run-old"]


def test_write_run_archive_records_multiple_entries_same_second(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    first = write_run_archive(
        timestamp="2026-03-09T10:45:00-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_signal",
        identity_parts=["same-event", "same-plan"],
        manifest_fields={"strategy": "dual_mom"},
        preferred_root=archive_root,
    )
    second = write_run_archive(
        timestamp="2026-03-09T10:45:00-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_signal",
        identity_parts=["same-event", "same-plan"],
        manifest_fields={"strategy": "dual_mom"},
        preferred_root=archive_root,
    )

    entries = load_run_index(root_dir=archive_root)
    assert len(entries) == 2
    assert first.paths.run_dir.name != second.paths.run_dir.name
    assert second.paths.run_dir.name.endswith("_2")


def test_list_runs_cli_smoke(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    archive_root = tmp_path / "archive"

    write_run_archive(
        timestamp="2026-03-09T08:25:00-05:00",
        run_kind="next_action_alert",
        mode="change_only",
        label="dual_mom_EFA",
        identity_parts=["event-1"],
        manifest_fields={"strategy": "dual_mom", "symbol": "EFA", "action": "BUY"},
        preferred_root=archive_root,
    )
    latest = write_run_archive(
        timestamp="2026-03-09T10:45:00-05:00",
        run_kind="execution_plan",
        mode="managed_sleeve",
        label="dual_mom_signal",
        identity_parts=["event-2"],
        manifest_fields={"strategy": "dual_mom", "symbol": "EFA", "action": "RESIZE"},
        preferred_root=archive_root,
    )

    env = os.environ.copy()
    list_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "list_runs.py"),
        "--archive-root",
        str(archive_root),
        "--limit",
        "1",
    ]
    listed = subprocess.run(list_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert listed.returncode == 0, listed.stderr
    lines = listed.stdout.splitlines()
    assert len(lines) == 1
    assert "execution_plan" in lines[0]
    assert latest.paths.run_dir.name in lines[0]

    latest_manifest_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "list_runs.py"),
        "--archive-root",
        str(archive_root),
        "--latest-manifest-path",
    ]
    latest_manifest = subprocess.run(
        latest_manifest_cmd,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )
    assert latest_manifest.returncode == 0, latest_manifest.stderr
    assert latest_manifest.stdout.strip() == str(latest.paths.manifest_path)
