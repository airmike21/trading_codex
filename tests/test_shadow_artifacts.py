from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.data import LocalStore
from trading_codex.backtest.shadow_artifacts import (
    build_shadow_review_bundle,
    render_shadow_review_markdown,
)


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _bars_for_index(idx: pd.DatetimeIndex, close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {"open": close, "high": close, "low": close, "close": close, "volume": 1_000},
        index=idx,
    )


def _write_synth_store(base_dir: Path) -> None:
    idx = pd.date_range("2019-01-01", periods=520, freq="B")
    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_shy = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("SHY", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_shy), index=idx)))


def _rb_args(data_dir: Path) -> list[str]:
    return [
        "--strategy",
        "valmom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "CCC",
        "--vm-defensive-symbol",
        "SHY",
        "--vm-mom-lookback",
        "63",
        "--vm-val-lookback",
        "126",
        "--vm-top-n",
        "2",
        "--vm-rebalance",
        "21",
        "--start",
        "2020-01-02",
        "--end",
        "2020-12-01",
        "--no-plot",
        "--data-dir",
        str(data_dir),
    ]


def test_run_backtest_does_not_create_shadow_artifacts_without_flag(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    shadow_dir = tmp_path / "shadow"
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--next-action-json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert len(proc.stdout.splitlines()) == 1
    assert not shadow_dir.exists()


def test_run_backtest_shadow_artifacts_create_bundle_and_preserve_next_action_stdout(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    base_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--next-action-json",
    ]
    baseline = subprocess.run(base_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert baseline.returncode == 0, f"stdout={baseline.stdout!r}\nstderr={baseline.stderr!r}"
    baseline_lines = baseline.stdout.splitlines()
    assert len(baseline_lines) == 1

    shadow_dir = tmp_path / "shadow"
    proc = subprocess.run(
        [*base_cmd, "--shadow-artifacts-dir", str(shadow_dir)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert proc.stdout == baseline.stdout
    assert proc.stderr == ""

    json_artifacts = list((shadow_dir / "plans" / "2020-12-01").glob("*_shadow_review.json"))
    markdown_artifacts = list((shadow_dir / "reviews" / "2020-12-01").glob("*_shadow_review.md"))
    assert len(json_artifacts) == 1
    assert len(markdown_artifacts) == 1

    payload = json.loads(json_artifacts[0].read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "shadow_review"
    assert payload["artifact_version"] == 1
    assert payload["strategy"] == "valmom_v1"
    assert payload["generated_at"] == "2020-12-01T00:00:00"
    assert payload["as_of_date"] == "2020-12-01"
    assert payload["shadow_status"] == "review"
    assert payload["cost_assumptions"]["slippage_bps"] == 5.0
    assert payload["metrics"]["gross_cagr"] is not None
    assert payload["rebalance_event_count"] >= 0
    assert payload["commission_trade_count"] >= 0
    assert len(payload["actions"]) == 1
    assert payload["actions"][0]["event_id"] == json.loads(proc.stdout)["event_id"]
    assert payload["warnings"] == []
    assert payload["blockers"] == []
    assert payload["warning_reasons"] == ["stale_data"]
    assert payload["blocking_reasons"] == []
    assert payload["shadow_review_state"] == "warning"

    review_text = markdown_artifacts[0].read_text(encoding="utf-8")
    assert "# Shadow Review valmom_v1" in review_text
    assert "As-of date: `2020-12-01`" in review_text
    assert "Next rebalance:" in review_text
    assert "Number of actions: `1`" in review_text
    assert "Cost assumptions:" in review_text
    assert "Gross CAGR:" in review_text
    assert "Net CAGR:" in review_text
    assert "Rebalance-event count:" in review_text
    assert "Commission-counted sleeve/order count:" in review_text
    assert "Shadow review state: `warning`" in review_text


def test_run_backtest_shadow_artifacts_can_coexist_with_metrics_out(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    metrics_out = tmp_path / "metrics.json"
    shadow_dir = tmp_path / "shadow"
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--metrics-out",
        str(metrics_out),
        "--shadow-artifacts-dir",
        str(shadow_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert metrics_out.exists()
    assert list((shadow_dir / "plans" / "2020-12-01").glob("*_shadow_review.json"))
    assert list((shadow_dir / "reviews" / "2020-12-01").glob("*_shadow_review.md"))

    metrics_payload = json.loads(metrics_out.read_text(encoding="utf-8"))
    assert metrics_payload["cost_assumptions"]["slippage_bps"] == 5.0


# ---------------------------------------------------------------------------
# Unit tests: warning_reasons / blocking_reasons markdown rendering
# ---------------------------------------------------------------------------


def _minimal_bundle(
    *,
    warning_reasons: list[str] | None = None,
    blocking_reasons: list[str] | None = None,
) -> dict:
    """Return a minimal valid shadow review bundle with injected reason lists."""
    import pandas as pd

    today = pd.Timestamp.now().normalize().date().isoformat()
    bundle = build_shadow_review_bundle(
        strategy="test_strategy",
        as_of_date=today,
        next_rebalance=None,
        actions=[{"action": "BUY", "symbol": "SPY", "price": 450.0, "target_shares": 10, "event_id": "eid1"}],
        cost_assumptions={"slippage_bps": 5.0, "commission_per_trade": 1.0, "commission_bps": 0.0},
        metrics={"gross_cagr": 0.12, "net_cagr": 0.10, "gross_sharpe": 0.9, "net_sharpe": 0.8},
    )
    # Override reason lists so tests are date-independent
    overrides: dict = {}
    if warning_reasons is not None:
        overrides["warning_reasons"] = warning_reasons
    if blocking_reasons is not None:
        overrides["blocking_reasons"] = blocking_reasons
    return {**bundle, **overrides}


def _contract_bundle(
    *,
    as_of_date: str | None = None,
    actions: list[dict] | None = None,
    expected_symbol_count: int | None = None,
    actual_symbol_count: int | None = None,
) -> dict:
    """Return a real bundle for contract/parity assertions."""
    bundle_as_of_date = as_of_date or pd.Timestamp.now().normalize().date().isoformat()
    bundle_actions = actions or [
        {
            "action": "BUY",
            "symbol": "SPY",
            "price": 450.0,
            "target_shares": 10,
            "event_id": "contract-eid",
        }
    ]
    return build_shadow_review_bundle(
        strategy="contract_strategy",
        as_of_date=bundle_as_of_date,
        next_rebalance="2026-03-31",
        actions=bundle_actions,
        cost_assumptions={"slippage_bps": 5.0, "commission_per_trade": 1.0, "commission_bps": 0.0},
        metrics={
            "gross_cagr": 0.12,
            "net_cagr": 0.10,
            "gross_sharpe": 0.9,
            "net_sharpe": 0.8,
            "rebalance_event_count": 3.0,
            "commission_trade_count": 4.0,
        },
        expected_symbol_count=expected_symbol_count,
        actual_symbol_count=actual_symbol_count,
    )


class TestRenderShadowReviewMarkdownReasons:
    """Focused unit tests for warning_reasons / blocking_reasons markdown sections."""

    def test_non_empty_warning_reasons_render_warnings_section(self) -> None:
        """Non-empty warning_reasons produces a '## Warnings' section with one bullet per reason."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" in md
        assert "- stale_data" in md

    def test_non_empty_blocking_reasons_render_blockers_section(self) -> None:
        """Non-empty blocking_reasons produces a '## Blockers' section with one bullet per reason."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        assert "## Blockers" in md
        assert "- missing_price" in md

    def test_multiple_warning_reasons_all_rendered(self) -> None:
        """Each entry in warning_reasons appears as its own bullet."""
        bundle = _minimal_bundle(warning_reasons=["stale_data", "extra_warn"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" in md
        assert "- stale_data" in md
        assert "- extra_warn" in md

    def test_multiple_blocking_reasons_all_rendered(self) -> None:
        """Each entry in blocking_reasons appears as its own bullet."""
        bundle = _minimal_bundle(
            warning_reasons=[],
            blocking_reasons=["missing_price", "symbol_count_mismatch"],
        )
        md = render_shadow_review_markdown(bundle)
        assert "## Blockers" in md
        assert "- missing_price" in md
        assert "- symbol_count_mismatch" in md

    def test_empty_warning_reasons_omits_warnings_section(self) -> None:
        """Empty warning_reasons must not produce a '## Warnings' section."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" not in md

    def test_empty_blocking_reasons_omits_blockers_section(self) -> None:
        """Empty blocking_reasons must not produce a '## Blockers' section."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "## Blockers" not in md

    def test_absent_reason_lists_omit_both_sections(self) -> None:
        """Bundle without warning_reasons/blocking_reasons keys omits both sections."""
        bundle = _minimal_bundle()
        # Remove the keys entirely to simulate absent fields
        bundle.pop("warning_reasons", None)
        bundle.pop("blocking_reasons", None)
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" not in md
        assert "## Blockers" not in md

    def test_both_empty_omit_both_sections(self) -> None:
        """Both lists empty → neither section appears."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" not in md
        assert "## Blockers" not in md

    def test_actions_section_still_present(self) -> None:
        """## Actions section must still appear regardless of reason content."""
        for warning_reasons, blocking_reasons in [
            ([], []),
            (["stale_data"], []),
            ([], ["missing_price"]),
            (["stale_data"], ["missing_price"]),
        ]:
            bundle = _minimal_bundle(
                warning_reasons=warning_reasons, blocking_reasons=blocking_reasons
            )
            md = render_shadow_review_markdown(bundle)
            assert "## Actions" in md, (
                f"## Actions missing when warning_reasons={warning_reasons!r}, "
                f"blocking_reasons={blocking_reasons!r}"
            )

    def test_existing_header_metadata_preserved(self) -> None:
        """Existing header lines must still appear even when reason sections are added."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        assert "# Shadow Review test_strategy" in md
        assert "As-of date:" in md
        assert "Gross CAGR:" in md
        assert "Rebalance-event count:" in md
        assert "Commission-counted sleeve/order count:" in md

    def test_sections_appear_before_actions(self) -> None:
        """## Warnings and ## Blockers must appear before ## Actions in the output."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        warnings_pos = md.index("## Warnings")
        blockers_pos = md.index("## Blockers")
        actions_pos = md.index("## Actions")
        assert warnings_pos < actions_pos, "## Warnings must come before ## Actions"
        assert blockers_pos < actions_pos, "## Blockers must come before ## Actions"


# ---------------------------------------------------------------------------
# Unit tests: new "Warning reasons" / "Blocking reasons" summary lines
# ---------------------------------------------------------------------------


class TestReasonSummaryLines:
    """Focused tests for the new top-level summary lines added after Warnings/Blockers."""

    def test_warning_reasons_summary_line_renders_codes_when_non_empty(self) -> None:
        """Non-empty warning_reasons renders comma-joined codes on the summary line."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "- Warning reasons: `stale_data`" in md

    def test_blocking_reasons_summary_line_renders_codes_when_non_empty(self) -> None:
        """Non-empty blocking_reasons renders comma-joined codes on the summary line."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        assert "- Blocking reasons: `missing_price`" in md

    def test_multiple_warning_reason_codes_comma_joined_in_summary(self) -> None:
        """Multiple warning reason codes are comma-joined on the summary line."""
        bundle = _minimal_bundle(warning_reasons=["stale_data", "extra_warn"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "- Warning reasons: `stale_data, extra_warn`" in md

    def test_multiple_blocking_reason_codes_comma_joined_in_summary(self) -> None:
        """Multiple blocking reason codes are comma-joined on the summary line."""
        bundle = _minimal_bundle(
            warning_reasons=[],
            blocking_reasons=["missing_price", "symbol_count_mismatch"],
        )
        md = render_shadow_review_markdown(bundle)
        assert "- Blocking reasons: `missing_price, symbol_count_mismatch`" in md

    def test_empty_warning_reasons_renders_dash_on_summary_line(self) -> None:
        """Empty warning_reasons list renders '-' on the summary line."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "- Warning reasons: `-`" in md

    def test_empty_blocking_reasons_renders_dash_on_summary_line(self) -> None:
        """Empty blocking_reasons list renders '-' on the summary line."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "- Blocking reasons: `-`" in md

    def test_absent_warning_reasons_renders_dash_on_summary_line(self) -> None:
        """Absent warning_reasons key renders '-' on the summary line."""
        bundle = _minimal_bundle()
        bundle.pop("warning_reasons", None)
        md = render_shadow_review_markdown(bundle)
        assert "- Warning reasons: `-`" in md

    def test_absent_blocking_reasons_renders_dash_on_summary_line(self) -> None:
        """Absent blocking_reasons key renders '-' on the summary line."""
        bundle = _minimal_bundle()
        bundle.pop("blocking_reasons", None)
        md = render_shadow_review_markdown(bundle)
        assert "- Blocking reasons: `-`" in md

    def test_summary_lines_appear_after_legacy_warnings_blockers_lines(self) -> None:
        """New summary lines appear after the legacy Warnings/Blockers lines."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        warnings_legacy_pos = md.index("- Warnings:")
        blockers_legacy_pos = md.index("- Blockers:")
        warning_reasons_pos = md.index("- Warning reasons:")
        blocking_reasons_pos = md.index("- Blocking reasons:")
        assert warnings_legacy_pos < warning_reasons_pos, (
            "legacy '- Warnings:' must appear before '- Warning reasons:'"
        )
        assert blockers_legacy_pos < blocking_reasons_pos, (
            "legacy '- Blockers:' must appear before '- Blocking reasons:'"
        )

    def test_summary_lines_appear_before_actions_section(self) -> None:
        """New summary lines appear in the header block, before ## Actions."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        warning_reasons_pos = md.index("- Warning reasons:")
        blocking_reasons_pos = md.index("- Blocking reasons:")
        actions_pos = md.index("## Actions")
        assert warning_reasons_pos < actions_pos
        assert blocking_reasons_pos < actions_pos

    def test_existing_detailed_warnings_section_still_renders(self) -> None:
        """The existing ## Warnings bullet-list section is unchanged."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" in md
        assert "- stale_data" in md

    def test_existing_detailed_blockers_section_still_renders(self) -> None:
        """The existing ## Blockers bullet-list section is unchanged."""
        bundle = _minimal_bundle(warning_reasons=[], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        assert "## Blockers" in md
        assert "- missing_price" in md

    def test_actions_section_preserved_with_reason_summary_lines(self) -> None:
        """## Actions section is still present when reason summary lines are rendered."""
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=["missing_price"])
        md = render_shadow_review_markdown(bundle)
        assert "## Actions" in md

    def test_stale_data_bundle_has_meaningful_top_summary(self) -> None:
        """A stale-data bundle shows 'stale_data' on the top summary line (not just ## Warnings)."""
        # Simulate a stale-data bundle by injecting the reason directly
        bundle = _minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=[])
        md = render_shadow_review_markdown(bundle)
        # The summary line must appear near the top, before any ## section headers
        first_section_pos = md.index("##")
        summary_line_pos = md.index("- Warning reasons: `stale_data`")
        assert summary_line_pos < first_section_pos, (
            "Warning reasons summary must appear before the first ## section"
        )


# ---------------------------------------------------------------------------
# Unit tests: artifact_version field and markdown line
# ---------------------------------------------------------------------------


class TestArtifactVersion:
    """Focused tests for the artifact_version field and its markdown representation."""

    def test_build_shadow_review_bundle_returns_artifact_version_1(self) -> None:
        """build_shadow_review_bundle() must include artifact_version equal to 1."""
        bundle = _minimal_bundle()
        assert bundle["artifact_version"] == 1

    def test_render_shadow_review_markdown_includes_artifact_version_line(self) -> None:
        """render_shadow_review_markdown() must include '- Artifact version: `1`' line."""
        bundle = _minimal_bundle()
        md = render_shadow_review_markdown(bundle)
        assert "- Artifact version: `1`" in md


    def test_artifact_version_line_appears_before_actions_section(self) -> None:
        """Artifact version summary line must appear in the header block, before ## Actions."""
        bundle = _minimal_bundle()
        md = render_shadow_review_markdown(bundle)
        artifact_pos = md.index("- Artifact version:")
        actions_pos = md.index("## Actions")
        assert artifact_pos < actions_pos, "Artifact version line must appear before ## Actions"

    def test_artifact_version_line_appears_before_any_section_header(self) -> None:
        """Artifact version line must appear in the top metadata block, before any ## header."""
        bundle = _minimal_bundle()
        md = render_shadow_review_markdown(bundle)
        artifact_pos = md.index("- Artifact version:")
        first_section_pos = md.index("##")
        assert artifact_pos < first_section_pos, (
            "Artifact version line must appear before the first ## section header"
        )

    def test_existing_summary_lines_still_render_with_artifact_version(self) -> None:
        """Existing summary lines are unchanged when artifact_version line is added."""
        bundle = _minimal_bundle()
        md = render_shadow_review_markdown(bundle)
        assert "- Shadow status:" in md
        assert "- Strategy:" in md
        assert "- As-of date:" in md
        assert "- Next rebalance:" in md
        assert "- Number of actions:" in md
        assert "- Gross CAGR:" in md
        assert "- Net CAGR:" in md
        assert "- Rebalance-event count:" in md
        assert "- Commission-counted sleeve/order count:" in md

    def test_existing_sections_still_render_with_artifact_version(self) -> None:
        """Existing ## Actions section is still present after adding the artifact_version line."""
        bundle = _minimal_bundle()
        md = render_shadow_review_markdown(bundle)
        assert "## Actions" in md

    def test_artifact_version_field_is_integer(self) -> None:
        """artifact_version field must be an integer, not a string."""
        bundle = _minimal_bundle()
        assert isinstance(bundle["artifact_version"], int)

    def test_integration_bundle_contains_artifact_version_1(self) -> None:
        """Integration-style check: bundle produced by build_shadow_review_bundle has artifact_version=1."""
        import pandas as pd

        today = pd.Timestamp.now().normalize().date().isoformat()
        bundle = build_shadow_review_bundle(
            strategy="integration_test",
            as_of_date=today,
            next_rebalance=None,
            actions=[{"action": "HOLD", "symbol": "BIL", "price": 91.5, "target_shares": 5, "event_id": "eid2"}],
            cost_assumptions={"slippage_bps": 2.0, "commission_per_trade": 0.0, "commission_bps": 0.0},
            metrics={"gross_cagr": 0.05, "net_cagr": 0.045, "gross_sharpe": 0.7, "net_sharpe": 0.65},
        )
        assert bundle["artifact_version"] == 1
        md = render_shadow_review_markdown(bundle)
        assert "- Artifact version: `1`" in md


class TestReadinessBooleanSummaryLines:
    """Focused tests for readiness booleans rendered in the markdown summary block."""

    def test_readiness_boolean_summary_lines_render_true_values(self) -> None:
        bundle = {
            **_minimal_bundle(),
            "ready_for_shadow_review": True,
            "stale_data_warning": True,
            "missing_price_warning": True,
            "symbol_count_mismatch_warning": True,
        }
        md = render_shadow_review_markdown(bundle)
        assert "- Ready for shadow review: `true`" in md
        assert "- Stale data warning: `true`" in md
        assert "- Missing price warning: `true`" in md
        assert "- Symbol count mismatch warning: `true`" in md

    def test_readiness_boolean_summary_lines_render_false_values(self) -> None:
        bundle = {
            **_minimal_bundle(),
            "ready_for_shadow_review": False,
            "stale_data_warning": False,
            "missing_price_warning": False,
            "symbol_count_mismatch_warning": False,
        }
        md = render_shadow_review_markdown(bundle)
        assert "- Ready for shadow review: `false`" in md
        assert "- Stale data warning: `false`" in md
        assert "- Missing price warning: `false`" in md
        assert "- Symbol count mismatch warning: `false`" in md

    def test_existing_summary_lines_remain_present_with_readiness_booleans(self) -> None:
        bundle = {
            **_minimal_bundle(),
            "ready_for_shadow_review": True,
            "stale_data_warning": False,
            "missing_price_warning": False,
            "symbol_count_mismatch_warning": False,
        }
        md = render_shadow_review_markdown(bundle)
        assert "- Artifact version: `1`" in md
        assert "- Shadow status:" in md
        assert "- Strategy:" in md
        assert "- As-of date:" in md
        assert "- Next rebalance:" in md
        assert "- Number of actions:" in md
        assert "- Warning reasons:" in md
        assert "- Blocking reasons:" in md

    def test_existing_detailed_sections_remain_intact_with_readiness_booleans(self) -> None:
        bundle = {
            **_minimal_bundle(warning_reasons=["stale_data"], blocking_reasons=["missing_price"]),
            "ready_for_shadow_review": False,
            "stale_data_warning": True,
            "missing_price_warning": True,
            "symbol_count_mismatch_warning": False,
        }
        md = render_shadow_review_markdown(bundle)
        assert "## Warnings" in md
        assert "- stale_data" in md
        assert "## Blockers" in md
        assert "- missing_price" in md
        assert "## Actions" in md

    def test_stale_data_bundle_shows_warning_boolean_and_existing_detail_behavior(self) -> None:
        stale_bundle = build_shadow_review_bundle(
            strategy="stale_data_case",
            as_of_date="2020-01-01",
            next_rebalance=None,
            actions=[
                {
                    "action": "HOLD",
                    "symbol": "BIL",
                    "price": 91.5,
                    "target_shares": 5,
                    "event_id": "eid-stale",
                }
            ],
            cost_assumptions={
                "slippage_bps": 2.0,
                "commission_per_trade": 0.0,
                "commission_bps": 0.0,
            },
            metrics={
                "gross_cagr": 0.05,
                "net_cagr": 0.045,
                "gross_sharpe": 0.7,
                "net_sharpe": 0.65,
            },
        )
        md = render_shadow_review_markdown(stale_bundle)
        assert stale_bundle["stale_data_warning"] is True
        assert "- Stale data warning: `true`" in md
        assert "- Warning reasons: `stale_data`" in md
        assert "## Warnings" in md
        assert "- stale_data" in md


class TestShadowReviewState:
    """Focused tests for the machine-readable shadow review state field."""

    def test_build_shadow_review_bundle_returns_shadow_review_state(self) -> None:
        bundle = _contract_bundle()
        assert bundle["shadow_review_state"] == "clean"

    def test_shadow_review_state_is_clean_when_reason_lists_are_empty(self) -> None:
        bundle = _contract_bundle()
        assert bundle["warning_reasons"] == []
        assert bundle["blocking_reasons"] == []
        assert bundle["shadow_review_state"] == "clean"

    def test_shadow_review_state_is_warning_when_warning_reasons_exist_without_blockers(self) -> None:
        bundle = _contract_bundle(as_of_date="2020-01-01")
        assert bundle["warning_reasons"] == ["stale_data"]
        assert bundle["blocking_reasons"] == []
        assert bundle["shadow_review_state"] == "warning"

    def test_shadow_review_state_is_blocked_when_blocking_reasons_exist(self) -> None:
        bundle = _contract_bundle(
            as_of_date="2020-01-01",
            actions=[
                {
                    "action": "BUY",
                    "symbol": "SPY",
                    "price": None,
                    "target_shares": 10,
                    "event_id": "blocked-eid",
                }
            ],
            expected_symbol_count=3,
            actual_symbol_count=2,
        )
        assert "stale_data" in bundle["warning_reasons"]
        assert "missing_price" in bundle["blocking_reasons"]
        assert "symbol_count_mismatch" in bundle["blocking_reasons"]
        assert bundle["shadow_review_state"] == "blocked"

    def test_markdown_includes_shadow_review_state_line(self) -> None:
        bundle = _contract_bundle(as_of_date="2020-01-01")
        md = render_shadow_review_markdown(bundle)
        assert "- Shadow review state: `warning`" in md

    def test_existing_summary_lines_and_sections_remain_intact_with_shadow_review_state(self) -> None:
        bundle = _contract_bundle(
            as_of_date="2020-01-01",
            actions=[
                {
                    "action": "BUY",
                    "symbol": "SPY",
                    "price": None,
                    "target_shares": 10,
                    "event_id": "state-sections-eid",
                }
            ],
            expected_symbol_count=3,
            actual_symbol_count=2,
        )
        md = render_shadow_review_markdown(bundle)
        assert "- Artifact version: `1`" in md
        assert "- Warning reasons: `stale_data`" in md
        assert "- Blocking reasons: `missing_price, symbol_count_mismatch`" in md
        assert "- Ready for shadow review: `false`" in md
        assert "## Warnings" in md
        assert "## Blockers" in md
        assert "## Actions" in md
        warnings_pos = md.index("## Warnings")
        blockers_pos = md.index("## Blockers")
        actions_pos = md.index("## Actions")
        assert warnings_pos < actions_pos
        assert blockers_pos < actions_pos


class TestShadowArtifactContractParity:
    """Contract/parity coverage for the current bundle and markdown behavior."""

    def test_bundle_contract_fields_exist_with_expected_basic_types(self) -> None:
        bundle = _contract_bundle()
        assert "artifact_version" in bundle
        assert "ready_for_shadow_review" in bundle
        assert "stale_data_warning" in bundle
        assert "missing_price_warning" in bundle
        assert "symbol_count_mismatch_warning" in bundle
        assert "warning_reasons" in bundle
        assert "blocking_reasons" in bundle

        assert isinstance(bundle["artifact_version"], int)
        assert isinstance(bundle["ready_for_shadow_review"], bool)
        assert isinstance(bundle["stale_data_warning"], bool)
        assert isinstance(bundle["missing_price_warning"], bool)
        assert isinstance(bundle["symbol_count_mismatch_warning"], bool)
        assert isinstance(bundle["warning_reasons"], list)
        assert isinstance(bundle["blocking_reasons"], list)

    def test_bundle_consistency_rules_follow_current_repo_semantics(self) -> None:
        clean_bundle = _contract_bundle()
        missing_price_bundle = _contract_bundle(
            actions=[
                {
                    "action": "BUY",
                    "symbol": "SPY",
                    "price": None,
                    "target_shares": 10,
                    "event_id": "missing-price-eid",
                }
            ]
        )
        symbol_mismatch_bundle = _contract_bundle(expected_symbol_count=3, actual_symbol_count=2)

        assert clean_bundle["ready_for_shadow_review"] is True
        assert clean_bundle["stale_data_warning"] is False
        assert clean_bundle["missing_price_warning"] is False
        assert clean_bundle["symbol_count_mismatch_warning"] is False
        assert clean_bundle["warning_reasons"] == []
        assert clean_bundle["blocking_reasons"] == []

        assert missing_price_bundle["missing_price_warning"] is True
        assert "missing_price" in missing_price_bundle["blocking_reasons"]
        assert missing_price_bundle["ready_for_shadow_review"] is False

        assert symbol_mismatch_bundle["symbol_count_mismatch_warning"] is True
        assert "symbol_count_mismatch" in symbol_mismatch_bundle["blocking_reasons"]
        assert symbol_mismatch_bundle["ready_for_shadow_review"] is False

    def test_markdown_parity_includes_current_contract_lines_and_sections(self) -> None:
        bundle = _contract_bundle(
            as_of_date="2020-01-01",
            actions=[
                {
                    "action": "BUY",
                    "symbol": "SPY",
                    "price": None,
                    "target_shares": 10,
                    "event_id": "contract-stale-missing",
                }
            ],
            expected_symbol_count=3,
            actual_symbol_count=2,
        )
        md = render_shadow_review_markdown(bundle)

        assert "- Artifact version: `1`" in md
        assert "- Ready for shadow review: `false`" in md
        assert "- Stale data warning: `true`" in md
        assert "- Missing price warning: `true`" in md
        assert "- Symbol count mismatch warning: `true`" in md
        assert "- Warning reasons: `stale_data`" in md
        assert "- Blocking reasons: `missing_price, symbol_count_mismatch`" in md
        assert "## Warnings" in md
        assert "- stale_data" in md
        assert "## Blockers" in md
        assert "- missing_price" in md
        assert "- symbol_count_mismatch" in md

    def test_legacy_summary_lines_and_actions_order_are_preserved(self) -> None:
        bundle = _contract_bundle(
            as_of_date="2020-01-01",
            actions=[
                {
                    "action": "BUY",
                    "symbol": "SPY",
                    "price": None,
                    "target_shares": 10,
                    "event_id": "order-check-eid",
                }
            ],
            expected_symbol_count=3,
            actual_symbol_count=2,
        )
        md = render_shadow_review_markdown(bundle)

        assert "- Shadow status:" in md
        assert "- Strategy:" in md
        assert "- As-of date:" in md
        assert "- Next rebalance:" in md
        assert "- Number of actions:" in md
        assert "- Warnings:" in md
        assert "- Blockers:" in md
        assert "## Actions" in md

        warnings_pos = md.index("## Warnings")
        blockers_pos = md.index("## Blockers")
        actions_pos = md.index("## Actions")
        assert warnings_pos < actions_pos
        assert blockers_pos < actions_pos

    def test_stale_data_integration_keeps_bundle_and_markdown_consistent(self) -> None:
        stale_bundle = _contract_bundle(as_of_date="2020-01-01")
        md = render_shadow_review_markdown(stale_bundle)

        assert stale_bundle["stale_data_warning"] is True
        assert "stale_data" in stale_bundle["warning_reasons"]
        assert stale_bundle["missing_price_warning"] is False
        assert stale_bundle["symbol_count_mismatch_warning"] is False
        assert stale_bundle["ready_for_shadow_review"] is False
        assert "- Stale data warning: `true`" in md
        assert "- Warning reasons: `stale_data`" in md
        assert "## Warnings" in md
        assert "- stale_data" in md
