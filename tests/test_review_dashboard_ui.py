from __future__ import annotations

import os
from pathlib import Path

from streamlit.testing.v1 import AppTest

from trading_codex.run_archive import write_run_archive


def _archive_review_run(
    *,
    archive_root: Path,
    timestamp: str,
    identity: str,
    source_label: str,
    warnings: list[str] | None = None,
    include_review_markdown: bool = False,
    quantity: int = 24,
    effective_capital: float = 2455.99,
    buying_power: float = 2455.99,
) -> None:
    source_artifacts: dict[str, Path] = {}
    if include_review_markdown:
        review_markdown = archive_root / f"{identity}_execution_plan.md"
        review_markdown.parent.mkdir(parents=True, exist_ok=True)
        review_markdown.write_text(f"# Review for {identity}\n", encoding="utf-8")
        source_artifacts["execution_plan_markdown"] = review_markdown

    write_run_archive(
        timestamp=timestamp,
        run_kind="execution_plan",
        mode="managed_sleeve",
        label=source_label,
        identity_parts=[identity],
        manifest_fields={
            "strategy": "dual_mom",
            "symbol": "EFA",
            "action": "BUY",
            "source": {
                "kind": "preset",
                "label": source_label,
                "ref": "/tmp/presets.json",
            },
        },
        json_artifacts={
            "execution_plan_json": {
                "generated_at_chicago": timestamp,
                "warnings": [] if warnings is None else list(warnings),
                "signal": {
                    "strategy": "dual_mom",
                    "action": "BUY",
                    "symbol": "EFA",
                },
                "broker_snapshot": {
                    "account_id": "paper-1",
                    "buying_power": buying_power,
                },
                "sizing": {
                    "effective_capital_used": effective_capital,
                    "buying_power_cap_applied": True,
                },
                "items": [
                    {
                        "classification": "BUY",
                        "delta_shares": quantity,
                        "reference_price": 99.16,
                        "estimated_notional": round(quantity * 99.16, 2),
                        "symbol": "EFA",
                        "warnings": [],
                        "blockers": [],
                    }
                ],
                "source": {
                    "kind": "preset",
                    "label": source_label,
                    "ref": "/tmp/presets.json",
                },
            }
        },
        source_artifacts=source_artifacts,
        preferred_root=archive_root,
    )


def _seed_triage_filter_runs(archive_root: Path) -> None:
    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:47:32-05:00",
        identity="plan-older",
        source_label="dual_mom_core",
        include_review_markdown=True,
        quantity=24,
    )
    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:48:32-05:00",
        identity="plan-warning",
        source_label="dual_mom_core",
        warnings=["warning_from_plan"],
        include_review_markdown=True,
        quantity=24,
    )
    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:49:32-05:00",
        identity="plan-trade-change",
        source_label="dual_mom_core",
        include_review_markdown=True,
        quantity=10,
    )


def _app_dashboard_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "scripts" / "review_dashboard.py")


def _triage_frames_with_baseline(app: AppTest):
    assert len(app.dataframe) >= 4
    baseline_needs_review_df = app.dataframe[0].value
    baseline_recent_activity_df = app.dataframe[1].value
    needs_review_df = app.dataframe[2].value
    recent_activity_df = app.dataframe[3].value
    return baseline_needs_review_df, baseline_recent_activity_df, needs_review_df, recent_activity_df


def test_baseline_selector_wording_is_scoped_to_whats_new_panel(tmp_path: Path) -> None:
    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])

    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:47:32-05:00",
        identity="plan-older",
        source_label="dual_mom_core",
    )
    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:48:32-05:00",
        identity="plan-newer",
        source_label="dual_mom_core",
    )

    app = AppTest.from_file(_app_dashboard_path())
    app.run(timeout=30)

    assert app.selectbox[0].label == "Baseline run for What's New"
    assert (
        app.selectbox[0].help
        == "Scopes only the What's New Since Baseline panel. The full Needs Review Now and Recent Activity sections below still show all loaded archive items."
    )

    captions = [caption.value for caption in app.caption]
    assert "Baseline comparison is session-only and applies only to the What's New Since Baseline panel." in captions
    assert (
        "This panel is filtered by the selected baseline. The full Needs Review Now and Recent Activity sections below still show all loaded archive items."
        in captions
    )

    subheaders = [subheader.value for subheader in app.subheader]
    assert subheaders[:3] == [
        "What’s New Since Baseline",
        "Needs Review Now",
        "Recent Activity",
    ]


def test_dashboard_tables_include_direct_artifact_paths(tmp_path: Path) -> None:
    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])

    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:49:32-05:00",
        identity="plan-warning",
        source_label="dual_mom_core",
        warnings=["warning_from_plan"],
        include_review_markdown=True,
    )

    app = AppTest.from_file(_app_dashboard_path())
    app.run(timeout=30)

    needs_review_df = app.dataframe[0].value
    recent_activity_df = app.dataframe[1].value

    for frame in (needs_review_df, recent_activity_df):
        assert "review_markdown_path" in frame.columns
        assert "plan_json_path" in frame.columns
        assert "run_folder_path" in frame.columns
        assert Path(str(frame.iloc[0]["review_markdown_path"])).exists()
        assert Path(str(frame.iloc[0]["plan_json_path"])).exists()
        assert Path(str(frame.iloc[0]["run_folder_path"])).is_dir()


def test_dashboard_sidebar_exposes_triage_filter_checkboxes(tmp_path: Path) -> None:
    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])

    _archive_review_run(
        archive_root=archive_root,
        timestamp="2026-03-11T15:49:32-05:00",
        identity="plan-warning",
        source_label="dual_mom_core",
        warnings=["warning_from_plan"],
        include_review_markdown=False,
    )

    app = AppTest.from_file(_app_dashboard_path())
    app.run(timeout=30)

    labels = [checkbox.label for checkbox in app.checkbox]
    assert labels == [
        "Only rows missing review markdown",
        "Only warnings or blockers",
        "Only trade changes",
        "Hide low-priority / no-major-delta rows",
    ]
    assert all(checkbox.value is False for checkbox in app.checkbox)

    captions = [caption.value for caption in app.caption]
    assert "These filters apply only to Needs Review Now and Recent Activity." in captions


def test_triage_checkboxes_off_render_expected_unfiltered_triage_rows(tmp_path: Path) -> None:
    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])
    _seed_triage_filter_runs(archive_root)

    app = AppTest.from_file(_app_dashboard_path())
    app.run(timeout=30)

    baseline_needs_review_df, baseline_recent_activity_df, needs_review_df, recent_activity_df = _triage_frames_with_baseline(
        app
    )

    assert all(checkbox.value is False for checkbox in app.checkbox)
    assert list(baseline_needs_review_df["headline"]) == [
        "New execution plan with trade changes vs prior comparable run",
        "Capital allocation changed from prior comparable run",
    ]
    assert list(baseline_recent_activity_df["status"]) == ["New execution plan with trade changes vs prior plan"]
    assert list(needs_review_df["headline"]) == [
        "Archived run contains warnings",
        "New execution plan with trade changes vs prior comparable run",
        "Capital allocation changed from prior comparable run",
    ]
    assert list(recent_activity_df["status"]) == [
        "New execution plan with trade changes vs prior plan",
        "Archived run contains warnings",
        "Archived run with 1 proposed trade",
    ]


def test_warning_checkbox_narrows_only_main_triage_tables_and_preserves_baseline_panel(tmp_path: Path) -> None:
    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])
    _seed_triage_filter_runs(archive_root)

    app = AppTest.from_file(_app_dashboard_path())
    app.run(timeout=30)

    before_baseline_needs_review_df, before_baseline_recent_activity_df, before_needs_review_df, before_recent_activity_df = (
        _triage_frames_with_baseline(app)
    )
    warning_checkbox = next(checkbox for checkbox in app.checkbox if checkbox.label == "Only warnings or blockers")
    app = warning_checkbox.check().run(timeout=30)
    after_baseline_needs_review_df, after_baseline_recent_activity_df, after_needs_review_df, after_recent_activity_df = (
        _triage_frames_with_baseline(app)
    )

    assert list(before_baseline_needs_review_df["headline"]) == list(after_baseline_needs_review_df["headline"])
    assert list(before_baseline_recent_activity_df["status"]) == list(after_baseline_recent_activity_df["status"])
    assert list(before_needs_review_df["headline"]) == [
        "Archived run contains warnings",
        "New execution plan with trade changes vs prior comparable run",
        "Capital allocation changed from prior comparable run",
    ]
    assert list(before_recent_activity_df["status"]) == [
        "New execution plan with trade changes vs prior plan",
        "Archived run contains warnings",
        "Archived run with 1 proposed trade",
    ]
    assert list(after_needs_review_df["headline"]) == ["Archived run contains warnings"]
    assert list(after_recent_activity_df["status"]) == ["Archived run contains warnings"]


def test_recent_activity_low_priority_toggle_hides_only_low_priority_rows_in_recent_activity(tmp_path: Path) -> None:
    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])
    _seed_triage_filter_runs(archive_root)

    app = AppTest.from_file(_app_dashboard_path())
    app.run(timeout=30)

    before_baseline_needs_review_df, before_baseline_recent_activity_df, before_needs_review_df, before_recent_activity_df = (
        _triage_frames_with_baseline(app)
    )
    low_priority_checkbox = next(
        checkbox for checkbox in app.checkbox if checkbox.label == "Hide low-priority / no-major-delta rows"
    )
    app = low_priority_checkbox.check().run(timeout=30)
    after_baseline_needs_review_df, after_baseline_recent_activity_df, after_needs_review_df, after_recent_activity_df = (
        _triage_frames_with_baseline(app)
    )

    assert list(before_baseline_needs_review_df["headline"]) == list(after_baseline_needs_review_df["headline"])
    assert list(before_baseline_recent_activity_df["status"]) == list(after_baseline_recent_activity_df["status"])
    assert list(before_needs_review_df["headline"]) == list(after_needs_review_df["headline"])
    assert list(before_recent_activity_df["status"]) == [
        "New execution plan with trade changes vs prior plan",
        "Archived run contains warnings",
        "Archived run with 1 proposed trade",
    ]
    assert list(after_recent_activity_df["status"]) == [
        "New execution plan with trade changes vs prior plan",
        "Archived run contains warnings",
    ]
