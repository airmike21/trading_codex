#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pandas as pd
import streamlit as st

from trading_codex.review_dashboard_data import (
    build_artifact_rows,
    build_baseline_option_rows,
    build_needs_review_rows,
    build_recent_activity_rows,
    build_run_comparison_rows,
    build_run_history_rows,
    filter_rows_for_runs,
    filter_runs_newer_than_baseline,
    filter_triage_rows,
    load_review_runs,
    summarize_new_since_baseline,
    summarize_run,
)


def _format_value(value: object) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, float):
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return str(value)


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _render_needs_review_table(rows: list[dict[str, object]]) -> None:
    st.dataframe(
        _frame(rows),
        use_container_width=True,
        hide_index=True,
        column_order=[
            "timestamp",
            "label",
            "headline",
            "detail",
            "review_markdown_path",
            "plan_json_path",
            "run_folder_path",
            "compare_to_run_id",
        ],
        column_config={
            "timestamp": st.column_config.TextColumn("Timestamp"),
            "label": st.column_config.TextColumn("Label"),
            "headline": st.column_config.TextColumn("Needs Review"),
            "detail": st.column_config.TextColumn("Detail", width="large"),
            "review_markdown_path": st.column_config.TextColumn(
                "Review MD",
                help="Absolute local path to the archived review markdown or checklist.",
                width="large",
            ),
            "plan_json_path": st.column_config.TextColumn(
                "Plan JSON",
                help="Absolute local path to the archived execution-plan JSON when available.",
                width="large",
            ),
            "run_folder_path": st.column_config.TextColumn(
                "Folder",
                help="Absolute local path to the archived run folder.",
                width="large",
            ),
            "compare_to_run_id": st.column_config.TextColumn("Compare To"),
        },
    )


def _render_recent_activity_table(rows: list[dict[str, object]]) -> None:
    st.dataframe(
        _frame(rows),
        use_container_width=True,
        hide_index=True,
        column_order=[
            "timestamp",
            "label",
            "status",
            "review_markdown_path",
            "plan_json_path",
            "run_folder_path",
        ],
        column_config={
            "timestamp": st.column_config.TextColumn("Timestamp"),
            "label": st.column_config.TextColumn("Label"),
            "status": st.column_config.TextColumn("Status", width="medium"),
            "review_markdown_path": st.column_config.TextColumn(
                "Review MD",
                help="Absolute local path to the archived review markdown or checklist.",
                width="large",
            ),
            "plan_json_path": st.column_config.TextColumn(
                "Plan JSON",
                help="Absolute local path to the archived execution-plan JSON when available.",
                width="large",
            ),
            "run_folder_path": st.column_config.TextColumn(
                "Folder",
                help="Absolute local path to the archived run folder.",
                width="large",
            ),
        },
    )


def main() -> None:
    st.set_page_config(page_title="Trading Codex Review Dashboard", layout="wide")
    st.title("Trading Codex Review Dashboard")
    st.caption(
        "Local-only, read-only review surface for archived Trading Codex runs. "
        "This dashboard does not place trades, mutate live-submit state, or trigger jobs."
    )

    limit = st.sidebar.slider("Recent runs to load", min_value=5, max_value=50, value=15, step=5)
    archive_root, runs = load_review_runs(limit=limit)
    st.sidebar.caption("Archive root")
    st.sidebar.code(str(archive_root))
    st.sidebar.caption("Triage filters")
    only_missing_review_markdown = st.sidebar.checkbox(
        "Only rows missing review markdown",
        value=False,
        help="Show only triage rows where the archived review markdown path is unavailable.",
    )
    only_warnings_or_blockers = st.sidebar.checkbox(
        "Only warnings or blockers",
        value=False,
        help="Show only triage rows that explicitly surface archived warnings or blockers.",
    )
    only_trade_changes = st.sidebar.checkbox(
        "Only trade changes",
        value=False,
        help="Show only triage rows that highlight trade deltas versus a prior comparable run.",
    )
    st.sidebar.caption("These filters apply only to Needs Review Now and Recent Activity.")

    if not archive_root.exists():
        st.info(
            "No archive root exists yet. Expected locations are ~/.trading_codex, "
            "~/.cache/trading_codex, then /tmp/trading_codex."
        )
        return

    if not runs:
        st.info(
            "No archived runs were found. Once review/shadow runs archive manifests under this root, "
            "they will appear here automatically."
        )
        return

    latest = runs[0]
    previous = runs[1] if len(runs) > 1 else None
    latest_summary = summarize_run(latest)
    latest_trades = latest.proposed_trades()
    needs_review_rows = build_needs_review_rows(runs)
    recent_activity_rows = build_recent_activity_rows(runs, limit=max(limit * 2, 10))
    filtered_needs_review_rows = filter_triage_rows(
        needs_review_rows,
        only_missing_review_markdown=only_missing_review_markdown,
        only_warnings_or_blockers=only_warnings_or_blockers,
        only_trade_changes=only_trade_changes,
    )
    filtered_recent_activity_rows = filter_triage_rows(
        recent_activity_rows,
        only_missing_review_markdown=only_missing_review_markdown,
        only_warnings_or_blockers=only_warnings_or_blockers,
        only_trade_changes=only_trade_changes,
    )
    baseline_option_rows = build_baseline_option_rows(runs)

    selected_baseline_run_id: str | None = None
    st.sidebar.caption(
        "Baseline comparison is session-only and applies only to the What's New Since Baseline panel."
    )
    if len(baseline_option_rows) > 1:
        baseline_ids = [row["run_id"] for row in baseline_option_rows]
        baseline_labels = {row["run_id"]: row["label"] for row in baseline_option_rows}
        selected_baseline_run_id = st.sidebar.selectbox(
            "Baseline run for What's New",
            options=baseline_ids,
            index=1,
            format_func=lambda run_id: baseline_labels.get(run_id, run_id),
            help=(
                "Scopes only the What's New Since Baseline panel. "
                "The full Needs Review Now and Recent Activity sections below still show all loaded archive items."
            ),
        )

    newer_runs = filter_runs_newer_than_baseline(runs, selected_baseline_run_id)
    newer_needs_review_rows = filter_rows_for_runs(needs_review_rows, newer_runs)
    newer_recent_activity_rows = filter_rows_for_runs(recent_activity_rows, newer_runs)
    baseline_summary = summarize_new_since_baseline(
        newer_runs=newer_runs,
        newer_needs_review_rows=newer_needs_review_rows,
        newer_recent_activity_rows=newer_recent_activity_rows,
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Latest Run Kind", _format_value(latest_summary.get("run_kind")))
    metric_cols[1].metric("Mode", _format_value(latest_summary.get("mode")))
    metric_cols[2].metric("Warnings", str(len(latest.warnings())))
    metric_cols[3].metric("Blockers", str(len(latest.blockers())))

    st.subheader("What’s New Since Baseline")
    if len(baseline_option_rows) <= 1:
        st.info("Need at least two archived runs before a baseline comparison is available.")
    else:
        st.caption(
            "This panel is filtered by the selected baseline. "
            "The full Needs Review Now and Recent Activity sections below still show all loaded archive items."
        )
        baseline_metric_cols = st.columns(3)
        baseline_metric_cols[0].metric("New Runs", str(baseline_summary.get("new_run_count")))
        baseline_metric_cols[1].metric("New Review Items", str(baseline_summary.get("new_needs_review_count")))
        baseline_metric_cols[2].metric("Newest In Scope", _format_value(baseline_summary.get("newest_timestamp")))

        if not newer_runs:
            st.info("No newer archive items found after selected baseline.")
        else:
            st.caption(f"{baseline_summary.get('new_run_count')} new runs since selected baseline.")

            st.markdown("**Needs Review Since Baseline**")
            if newer_needs_review_rows:
                _render_needs_review_table(newer_needs_review_rows)
            else:
                st.success("No needs-review items were found after the selected baseline.")

            st.markdown("**Recent Activity Since Baseline**")
            if newer_recent_activity_rows:
                _render_recent_activity_table(newer_recent_activity_rows)
            else:
                st.info("No newer recent-activity rows were found after the selected baseline.")

    st.subheader("Needs Review Now")
    if filtered_needs_review_rows:
        _render_needs_review_table(filtered_needs_review_rows)
    else:
        if any((only_missing_review_markdown, only_warnings_or_blockers, only_trade_changes)):
            st.info("No needs-review rows matched the selected triage filters.")
        else:
            st.success("No loaded archived runs currently trigger review heuristics.")

    st.subheader("Recent Activity")
    if filtered_recent_activity_rows:
        _render_recent_activity_table(filtered_recent_activity_rows)
    else:
        st.info("No recent-activity rows matched the selected triage filters.")

    st.subheader("Latest Run Summary")
    summary_rows = [
        {"field": "timestamp", "value": _format_value(latest_summary.get("timestamp"))},
        {"field": "run_id", "value": latest.run_id},
        {"field": "source_label", "value": _format_value(latest_summary.get("source_label"))},
        {"field": "broker_account_id", "value": _format_value(latest_summary.get("broker_account_id"))},
        {"field": "strategy", "value": _format_value(latest_summary.get("strategy"))},
        {"field": "action", "value": _format_value(latest_summary.get("action"))},
        {"field": "symbol", "value": _format_value(latest_summary.get("symbol"))},
        {"field": "target_shares", "value": _format_value(latest_summary.get("target_shares"))},
        {"field": "resize_new_shares", "value": _format_value(latest_summary.get("resize_new_shares"))},
        {"field": "next_rebalance", "value": _format_value(latest_summary.get("next_rebalance"))},
        {"field": "buying_power_available", "value": _format_value(latest_summary.get("buying_power_available"))},
        {"field": "buying_power_cap_applied", "value": _format_value(latest_summary.get("buying_power_cap_applied"))},
        {"field": "effective_capital", "value": _format_value(latest_summary.get("effective_capital"))},
        {"field": "leverage", "value": _format_value(latest_summary.get("leverage"))},
        {"field": "vol_target", "value": _format_value(latest_summary.get("vol_target"))},
        {"field": "plan_sha256", "value": _format_value(latest_summary.get("plan_sha256"))},
        {"field": "event_id", "value": _format_value(latest_summary.get("event_id"))},
        {"field": "live_submit_state_touched", "value": _format_value(latest_summary.get("live_submit_state_touched"))},
    ]
    st.dataframe(_frame(summary_rows), use_container_width=True)

    message_cols = st.columns(2)
    with message_cols[0]:
        st.subheader("Warnings")
        warnings = latest.warnings()
        if warnings:
            for warning in warnings:
                st.warning(warning)
        else:
            st.success("No warnings recorded for the latest archived run.")
    with message_cols[1]:
        st.subheader("Blockers")
        blockers = latest.blockers()
        if blockers:
            for blocker in blockers:
                st.error(blocker)
        else:
            st.success("No blockers recorded for the latest archived run.")

    st.subheader("Proposed Trades")
    if latest_trades:
        st.dataframe(_frame(latest_trades), use_container_width=True)
    else:
        st.info("No proposed trade payload was archived for the latest run.")

    st.subheader("Artifact Paths")
    st.dataframe(_frame(build_artifact_rows(latest)), use_container_width=True)

    st.subheader("Recent Run History")
    st.dataframe(_frame(build_run_history_rows(runs)), use_container_width=True)

    st.subheader("Latest vs Previous")
    if previous is None:
        st.info("A comparison panel will appear after at least two archived runs exist.")
    else:
        comparison_rows = build_run_comparison_rows(latest, previous)
        if comparison_rows:
            st.dataframe(_frame(comparison_rows), use_container_width=True)
        else:
            st.info("No key field changes were detected between the latest two archived runs.")

    with st.expander("Latest Manifest JSON"):
        st.json(latest.manifest)


if __name__ == "__main__":
    main()
