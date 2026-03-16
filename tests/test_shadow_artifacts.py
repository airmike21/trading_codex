from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.data import LocalStore


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
