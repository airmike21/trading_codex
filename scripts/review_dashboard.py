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
    baseline_option_rows = build_baseline_option_rows(runs)

    selected_baseline_run_id: str | None = None
    st.sidebar.caption("Baseline comparison is session-only and does not write review state.")
    if len(baseline_option_rows) > 1:
        baseline_ids = [row["run_id"] for row in baseline_option_rows]
        baseline_labels = {row["run_id"]: row["label"] for row in baseline_option_rows}
        selected_baseline_run_id = st.sidebar.selectbox(
            "Baseline run",
            options=baseline_ids,
            index=1,
            format_func=lambda run_id: baseline_labels.get(run_id, run_id),
            help="Show only archive items newer than the selected baseline run.",
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
                st.dataframe(_frame(newer_needs_review_rows), use_container_width=True)
            else:
                st.success("No needs-review items were found after the selected baseline.")

            st.markdown("**Recent Activity Since Baseline**")
            if newer_recent_activity_rows:
                st.dataframe(_frame(newer_recent_activity_rows), use_container_width=True)
            else:
                st.info("No newer recent-activity rows were found after the selected baseline.")

    st.subheader("Needs Review Now")
    if needs_review_rows:
        st.dataframe(_frame(needs_review_rows), use_container_width=True)
    else:
        st.success("No loaded archived runs currently trigger review heuristics.")

    st.subheader("Recent Activity")
    st.dataframe(_frame(recent_activity_rows), use_container_width=True)

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
