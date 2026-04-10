from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_STATE_PATH = REPO_ROOT / "docs" / "PROJECT_STATE.md"
STRATEGY_REGISTRY_PATH = REPO_ROOT / "docs" / "STRATEGY_REGISTRY.md"
WORKFLOW_PATH = REPO_ROOT / "docs" / "WORKFLOW.md"
STAGE2_SHADOW_OPS_PATH = REPO_ROOT / "configs" / "stage2_shadow_ops.json"
STAGE2_CANDIDATES_PATH = REPO_ROOT / "src" / "trading_codex" / "shadow" / "stage2_candidates.py"
RUN_BACKTEST_PATH = REPO_ROOT / "scripts" / "run_backtest.py"

PRIMARY_ID = "primary_live_candidate_v1"
VOL_MANAGED_ID = "primary_live_candidate_v1_vol_managed"
ETF_ROTATION_ID = "primary_live_candidate_v1_etf_rotation"
VOL_MANAGED_PAIR_ID = f"{PRIMARY_ID}_vs_{VOL_MANAGED_ID}"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _markdown_section(markdown_text: str, heading: str) -> str:
    pattern = rf"^## {re.escape(heading)}\n(?P<body>.*?)(?=^## |\Z)"
    match = re.search(pattern, markdown_text, flags=re.MULTILINE | re.DOTALL)
    assert match is not None, f"Missing markdown section: {heading!r}"
    return match.group("body")


def _table_row(markdown_text: str, identifier: str) -> str:
    needle = f"`{identifier}`"
    for line in markdown_text.splitlines():
        if needle in line:
            return line
    raise AssertionError(f"Missing table row containing {needle!r}")


def test_project_state_resume_snapshot_keeps_only_stable_checkpoint_semantics() -> None:
    resume_snapshot = _markdown_section(_read_text(PROJECT_STATE_PATH), "Resume Snapshot")
    bullet_lines = [line.strip() for line in resume_snapshot.splitlines() if line.strip().startswith("- ")]

    assert len(bullet_lines) == 1, (
        "Resume Snapshot should keep only stable checkpoint semantics. "
        f"Found bullets: {bullet_lines!r}"
    )
    assert bullet_lines[0].startswith("- Checkpoint reference:"), bullet_lines[0]

    for forbidden in (
        "Active Builder branch:",
        "Active slice base SHA:",
        "Workspace alignment note:",
        "Builder and Reviewer",
        "checkout positions",
    ):
        assert forbidden not in resume_snapshot, (
            "Resume Snapshot still contains volatile workspace state "
            f"marker {forbidden!r}."
        )


def test_workflow_requires_control_plane_drift_test_for_control_plane_doc_slices() -> None:
    workflow_text = _read_text(WORKFLOW_PATH)
    expected_rule = (
        "Control-plane doc slices must run `.venv/bin/python -m pytest "
        "tests/test_control_plane_docs.py` as part of required validation."
    )
    assert expected_rule in workflow_text


def test_runtime_open_shadow_targets_match_project_state_and_registry_claims() -> None:
    config = _load_json(STAGE2_SHADOW_OPS_PATH)
    targets = config["targets"]
    assert isinstance(targets, list), "stage2 shadow ops config must contain a targets list."

    pair_ids = {target["pair_id"] for target in targets}
    shadow_strategy_ids = {target["shadow_strategy_id"] for target in targets}
    shadow_strategy_families = {target["shadow_strategy_family"] for target in targets}

    assert pair_ids == {VOL_MANAGED_PAIR_ID}
    assert shadow_strategy_ids == {VOL_MANAGED_ID}
    assert shadow_strategy_families == {VOL_MANAGED_ID}
    assert ETF_ROTATION_ID not in shadow_strategy_ids
    assert ETF_ROTATION_ID not in shadow_strategy_families

    project_state = _read_text(PROJECT_STATE_PATH)
    assert (
        "Current explicitly opened shadow pair in repo live state: "
        "`primary_live_candidate_v1` vs `primary_live_candidate_v1_vol_managed`"
    ) in project_state
    assert (
        f"`{ETF_ROTATION_ID}` remains coded/runnable but not yet opened/configured in "
        "tracked runtime live state for recurring automation"
    ) in project_state

    registry_row = _table_row(_read_text(STRATEGY_REGISTRY_PATH), ETF_ROTATION_ID)
    assert "not yet explicitly opened/configured in repo runtime live state" in registry_row


def test_etf_rotation_control_plane_docs_track_coded_runnable_state_instead_of_queue_only() -> None:
    stage2_candidates_text = _read_text(STAGE2_CANDIDATES_PATH)
    run_backtest_text = _read_text(RUN_BACKTEST_PATH)
    assert ETF_ROTATION_ID in stage2_candidates_text
    assert ETF_ROTATION_ID in run_backtest_text

    registry_row = _table_row(_read_text(STRATEGY_REGISTRY_PATH), ETF_ROTATION_ID)
    assert "Coded; shadow-only; local-only; runnable; not paper-enabled" in registry_row
    for forbidden in (
        "Approved next bounded Stage 2 shadow candidate",
        "queue authorization only",
        "next bounded Stage 2 shadow-only slice",
        "next build slice",
    ):
        assert forbidden not in registry_row, (
            "ETF rotation registry row still describes the candidate as queue-only "
            f"or unopened build work via {forbidden!r}."
        )

    project_state = _read_text(PROJECT_STATE_PATH)
    expected_runtime_note = (
        f"the registered `{ETF_ROTATION_ID}` shadow candidate is now coded in promoted repo truth "
        f"and runnable locally through `scripts/run_backtest.py --strategy {ETF_ROTATION_ID}`"
    )
    assert expected_runtime_note in project_state

    for forbidden in (
        f"take the next bounded Stage 2 shadow-only build slice for `{ETF_ROTATION_ID}`",
        f"record `{ETF_ROTATION_ID}` as the next approved bounded shadow candidate",
        "queue authorization only",
        "next build slice",
    ):
        assert forbidden not in project_state, (
            "Project State still treats ETF rotation as unopened code work via "
            f"{forbidden!r}."
        )
