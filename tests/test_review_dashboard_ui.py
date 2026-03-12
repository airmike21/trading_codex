from __future__ import annotations

import os
from pathlib import Path

from streamlit.testing.v1 import AppTest

from trading_codex.run_archive import write_run_archive


def _archive_review_run(*, archive_root: Path, timestamp: str, identity: str, source_label: str) -> None:
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
                "signal": {
                    "strategy": "dual_mom",
                    "action": "BUY",
                    "symbol": "EFA",
                },
                "broker_snapshot": {
                    "account_id": "paper-1",
                    "buying_power": 2455.99,
                },
                "sizing": {
                    "effective_capital_used": 2455.99,
                    "buying_power_cap_applied": True,
                },
                "items": [
                    {
                        "classification": "BUY",
                        "delta_shares": 24,
                        "reference_price": 99.16,
                        "estimated_notional": 2379.84,
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
        preferred_root=archive_root,
    )


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

    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "scripts" / "review_dashboard.py"))
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
