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
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000,
        },
        index=idx,
    )


def _write_synth_store(base_dir: Path) -> None:
    idx = pd.date_range("2019-01-01", periods=520, freq="B")
    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_bil = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_bil), index=idx)))


def _write_presets(path: Path, data_dir: Path) -> None:
    payload = {
        "presets": {
            "dual_mom_core": {
                "description": "test base",
                "mode": "change_only",
                "emit": "text",
                "state_file": str(path.parent / "source_core_state.json"),
                "state_key": "dual_mom_core",
                "log_csv": str(path.parent / "source_core_alerts.csv"),
                "run_backtest_args": [
                    "--strategy",
                    "dual_mom",
                    "--symbols",
                    "AAA",
                    "BBB",
                    "CCC",
                    "--defensive",
                    "BIL",
                    "--mom-lookback",
                    "63",
                    "--rebalance",
                    "M",
                    "--start",
                    "2020-01-02",
                    "--end",
                    "2020-12-01",
                    "--data-dir",
                    str(data_dir),
                    "--no-plot",
                ],
            },
            "dual_mom_core_vt": {
                "description": "test vt",
                "mode": "change_only",
                "emit": "text",
                "state_file": str(path.parent / "source_vt_state.json"),
                "state_key": "dual_mom_core_vt",
                "log_csv": str(path.parent / "source_vt_alerts.csv"),
                "run_backtest_args": [
                    "--strategy",
                    "dual_mom",
                    "--symbols",
                    "AAA",
                    "BBB",
                    "CCC",
                    "--defensive",
                    "BIL",
                    "--mom-lookback",
                    "63",
                    "--rebalance",
                    "M",
                    "--vol-target",
                    "0.12",
                    "--vol-lookback",
                    "21",
                    "--min-leverage",
                    "0.0",
                    "--max-leverage",
                    "1.0",
                    "--start",
                    "2020-01-02",
                    "--end",
                    "2020-12-01",
                    "--data-dir",
                    str(data_dir),
                    "--no-plot",
                ],
            },
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_scheduled_dual_compare_creates_runtime_artifacts_and_daily_review(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    presets_path = tmp_path / "presets.json"
    _write_presets(presets_path, data_dir)
    base_dir = tmp_path / "scheduled_runs"

    morning_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "scheduled_dual_compare.py"),
        "--presets-file",
        str(presets_path),
        "--base-dir",
        str(base_dir),
        "--window",
        "morning_0825",
        "--timestamp",
        "2026-03-06T08:25:00-06:00",
    ]
    morning = subprocess.run(morning_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert morning.returncode == 0, f"stdout={morning.stdout!r}\nstderr={morning.stderr!r}"

    runtime_presets = base_dir / "runtime" / "dual_mom_compare_presets.json"
    assert runtime_presets.exists()
    runtime_payload = json.loads(runtime_presets.read_text(encoding="utf-8"))
    assert runtime_payload["presets"]["dual_mom_core"]["state_file"].endswith("state/dual_mom_core.json")
    assert runtime_payload["presets"]["dual_mom_core_vt"]["state_file"].endswith("state/dual_mom_core_vt.json")

    machine_log = base_dir / "logs" / "scheduled_runs.jsonl"
    assert machine_log.exists()
    morning_records = [json.loads(line) for line in machine_log.read_text(encoding="utf-8").splitlines()]
    assert {record["job_name"] for record in morning_records} == {
        "morning_0825_dual_mom_core",
        "morning_0825_dual_mom_core_vt",
        "morning_0825_daily_summary_dual_compare",
    }
    assert all(record["snapshot_path"] for record in morning_records)
    assert (base_dir / "logs" / "dual_mom_core_alerts.csv").exists()
    assert (base_dir / "logs" / "dual_mom_core_vt_alerts.csv").exists()

    snapshot_path = Path(morning_records[0]["snapshot_path"])
    assert snapshot_path.exists()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert len(snapshot["records"]) == 3

    summary_record = next(record for record in morning_records if record["job_name"] == "morning_0825_daily_summary_dual_compare")
    summary_payload = json.loads(summary_record["stdout_line"])
    assert {item["preset"] for item in summary_payload} == {"dual_mom_core", "dual_mom_core_vt"}

    afternoon_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "scheduled_dual_compare.py"),
        "--presets-file",
        str(presets_path),
        "--base-dir",
        str(base_dir),
        "--window",
        "afternoon_1535",
        "--timestamp",
        "2026-03-06T15:35:00-06:00",
    ]
    afternoon = subprocess.run(afternoon_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert afternoon.returncode == 0, f"stdout={afternoon.stdout!r}\nstderr={afternoon.stderr!r}"

    all_records = [json.loads(line) for line in machine_log.read_text(encoding="utf-8").splitlines()]
    assert len(all_records) == 6
    afternoon_core = next(record for record in all_records if record["job_name"] == "afternoon_1535_dual_mom_core")
    afternoon_vt = next(record for record in all_records if record["job_name"] == "afternoon_1535_dual_mom_core_vt")
    assert afternoon_core["stdout_line"] == ""
    assert afternoon_vt["stdout_line"] == ""

    daily_review = base_dir / "daily_reviews" / "2026-03-06_dual_compare.md"
    assert daily_review.exists()
    review_text = daily_review.read_text(encoding="utf-8")
    assert "## morning_0825" in review_text
    assert "## afternoon_1535" in review_text
    assert "`dual_mom_core`" in review_text
    assert "`dual_mom_core_vt`" in review_text


def test_scheduled_dual_compare_requires_both_presets(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    presets_path = tmp_path / "presets.json"
    presets_path.write_text(
        json.dumps(
            {
                "presets": {
                    "dual_mom_core": {
                        "description": "base only",
                        "mode": "change_only",
                        "emit": "text",
                        "run_backtest_args": ["--strategy", "dual_mom"],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "scheduled_dual_compare.py"),
        "--presets-file",
        str(presets_path),
        "--base-dir",
        str(tmp_path / "scheduled_runs"),
        "--window",
        "morning_0825",
        "--timestamp",
        "2026-03-06T08:25:00-06:00",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 2
    assert "missing preset(s): dual_mom_core_vt" in proc.stderr
