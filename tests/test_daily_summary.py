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


def _vm_rb_args(data_dir: Path) -> list[str]:
    return [
        "--strategy",
        "valmom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "CCC",
        "--vm-defensive-symbol",
        "BIL",
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


def _dm_rb_args(data_dir: Path) -> list[str]:
    return [
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
        "--no-plot",
        "--data-dir",
        str(data_dir),
    ]


def _run_next_action_json(repo_root: Path, env: dict[str, str], args: list[str]) -> dict[str, object]:
    cmd = [sys.executable, str(repo_root / "scripts" / "run_backtest.py"), *args, "--next-action-json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    return json.loads(lines[0])


def _write_presets(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_daily_summary_read_only_multi_preset_and_statuses(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    vm_args = _vm_rb_args(data_dir)
    dm_args = _dm_rb_args(data_dir)
    vm_payload = _run_next_action_json(repo_root, env, vm_args)
    dm_payload = _run_next_action_json(repo_root, env, dm_args)

    vm_state = tmp_path / "vm_state.json"
    vm_state.write_text(str(vm_payload["event_id"]) + "\n", encoding="utf-8")
    vm_due_state = tmp_path / "vm_due_state.json"
    vm_due_state.write_text(json.dumps({"last_event_id": vm_payload["event_id"]}) + "\n", encoding="utf-8")
    dm_state = tmp_path / "dm_state.json"
    dm_state.write_text("DIFFERENT_EVENT_ID\n", encoding="utf-8")

    vm_csv = tmp_path / "vm.csv"
    vm_csv.write_text("seed\n", encoding="utf-8")
    vm_due_csv = tmp_path / "vm_due.csv"
    vm_due_csv.write_text("seed\n", encoding="utf-8")
    dm_csv = tmp_path / "dm.csv"
    dm_csv.write_text("seed\n", encoding="utf-8")

    presets_path = tmp_path / "presets.json"
    _write_presets(
        presets_path,
        {
            "presets": {
                "vm_core": {
                    "description": "test",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(vm_state),
                    "state_key": "vm_core",
                    "log_csv": str(vm_csv),
                    "run_backtest_args": vm_args,
                },
                "vm_core_due": {
                    "description": "test",
                    "mode": "change_or_rebalance_due",
                    "emit": "text",
                    "state_file": str(vm_due_state),
                    "state_key": "vm_core_due",
                    "log_csv": str(vm_due_csv),
                    "run_backtest_args": vm_args,
                },
                "dual_mom_core": {
                    "description": "test",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(dm_state),
                    "state_key": "dual_mom_core",
                    "log_csv": str(dm_csv),
                    "run_backtest_args": dm_args,
                },
            }
        },
    )

    before = {
        vm_state: vm_state.read_bytes(),
        vm_due_state: vm_due_state.read_bytes(),
        dm_state: dm_state.read_bytes(),
        vm_csv: vm_csv.read_bytes(),
        vm_due_csv: vm_due_csv.read_bytes(),
        dm_csv: dm_csv.read_bytes(),
    }

    cmd = [sys.executable, str(repo_root / "scripts" / "daily_summary.py"), "--presets-file", str(presets_path)]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    lines = proc.stdout.splitlines()
    assert len(lines) == 3
    assert lines[0].startswith("vm_core")
    assert "status=UNCHANGED" in lines[0]
    assert "status=DUE" in lines[1]
    assert lines[2].startswith("dual_mom_core")
    assert str(dm_payload["action"]) in lines[2]
    assert str(dm_payload["symbol"]) in lines[2]
    assert "status=NEW" in lines[2]

    after = {
        vm_state: vm_state.read_bytes(),
        vm_due_state: vm_due_state.read_bytes(),
        dm_state: dm_state.read_bytes(),
        vm_csv: vm_csv.read_bytes(),
        vm_due_csv: vm_due_csv.read_bytes(),
        dm_csv: dm_csv.read_bytes(),
    }
    assert after == before


def test_daily_summary_missing_state_and_json_output(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    vm_args = _vm_rb_args(data_dir)
    dm_args = _dm_rb_args(data_dir)
    vm_state = tmp_path / "missing_state.json"
    dm_state = tmp_path / "dm_state.json"
    dm_payload = _run_next_action_json(repo_root, env, dm_args)
    dm_state.write_text(str(dm_payload["event_id"]) + "\n", encoding="utf-8")

    presets_path = tmp_path / "presets.json"
    _write_presets(
        presets_path,
        {
            "presets": {
                "vm_core": {
                    "description": "test",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(vm_state),
                    "state_key": "vm_core",
                    "run_backtest_args": vm_args,
                },
                "dual_mom_core": {
                    "description": "test",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(dm_state),
                    "state_key": "dual_mom_core",
                    "run_backtest_args": dm_args,
                },
            }
        },
    )

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_summary.py"),
        "--presets-file",
        str(presets_path),
        "--emit",
        "json",
        "--preset",
        "vm_core",
        "--preset",
        "dual_mom_core",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert isinstance(payload, list)
    assert [item["preset"] for item in payload] == ["vm_core", "dual_mom_core"]
    assert payload[0]["status"] == "MISSING_STATE"
    assert payload[0]["would_emit"] is True
    assert payload[1]["status"] == "UNCHANGED"
    assert payload[1]["state_path"] == str(dm_state)


def test_daily_summary_error_does_not_hide_other_presets(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    vm_args = _vm_rb_args(data_dir)
    vm_payload = _run_next_action_json(repo_root, env, vm_args)
    vm_state = tmp_path / "vm_state.json"
    vm_state.write_text(str(vm_payload["event_id"]) + "\n", encoding="utf-8")

    presets_path = tmp_path / "presets.json"
    _write_presets(
        presets_path,
        {
            "presets": {
                "vm_core": {
                    "description": "test",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(vm_state),
                    "state_key": "vm_core",
                    "run_backtest_args": vm_args,
                },
                "broken_core": {
                    "description": "broken",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(tmp_path / "broken_state.json"),
                    "state_key": "broken_core",
                    "run_backtest_args": [
                        "--strategy",
                        "valmom_v1",
                        "--symbols",
                        "MISSING",
                        "--vm-defensive-symbol",
                        "BIL",
                        "--start",
                        "2020-01-02",
                        "--end",
                        "2020-12-01",
                        "--no-plot",
                        "--data-dir",
                        str(data_dir),
                    ],
                },
            }
        },
    )

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_summary.py"),
        "--presets-file",
        str(presets_path),
        "--preset",
        "vm_core",
        "--preset",
        "broken_core",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 1
    lines = proc.stdout.splitlines()
    assert len(lines) == 2
    assert "status=UNCHANGED" in lines[0]
    assert "status=ERROR" in lines[1]
    assert "broken_core" in lines[1]


def test_daily_summary_vm_core_new_classification(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    vm_args = _vm_rb_args(data_dir)
    vm_state = tmp_path / "vm_state.json"
    vm_state.write_text("DIFFERENT_EVENT_ID\n", encoding="utf-8")

    presets_path = tmp_path / "presets.json"
    _write_presets(
        presets_path,
        {
            "presets": {
                "vm_core": {
                    "description": "test",
                    "mode": "change_only",
                    "emit": "text",
                    "state_file": str(vm_state),
                    "state_key": "vm_core",
                    "run_backtest_args": vm_args,
                }
            }
        },
    )

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_summary.py"),
        "--presets-file",
        str(presets_path),
        "--preset",
        "vm_core",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert proc.stdout.splitlines() == [line for line in proc.stdout.splitlines() if "status=NEW" in line]
