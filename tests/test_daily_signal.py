from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts import daily_signal
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


def _supports_log_csv(repo_root: Path, env: dict[str, str]) -> bool:
    cmd = [sys.executable, str(repo_root / "scripts" / "next_action_alert.py"), "--help"]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    return proc.returncode == 0 and "--log-csv" in proc.stdout


def test_preset_parsing_and_command_build(tmp_path: Path) -> None:
    preset_path = tmp_path / "presets.json"
    preset_path.write_text(
        json.dumps(
            {
                "presets": {
                    "unit": {
                        "description": "unit test",
                        "mode": "change_only",
                        "emit": "text",
                        "state_file": "~/state.json",
                        "state_key": "unit",
                        "log_csv": "~/alerts.csv",
                        "run_backtest_args": ["--strategy", "valmom_v1", "--data-dir", "~/data"],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    repo_root = Path(__file__).resolve().parents[1]
    presets = daily_signal._load_presets_json(preset_path)
    assert "unit" in presets
    preset = presets["unit"]
    cmd = daily_signal.build_next_action_alert_cmd(
        repo_root=repo_root,
        preset=preset,
        mode=None,
        emit=None,
        state_file=None,
        state_key=None,
        log_csv=None,
        verbose=False,
        dry_run=False,
        no_lock=False,
        lock_timeout_seconds=None,
        lock_stale_seconds=None,
    )
    assert cmd[0] == sys.executable
    assert cmd[1].endswith("scripts/next_action_alert.py")
    assert "--mode" in cmd and "change_only" in cmd
    assert "--emit" in cmd and "text" in cmd
    assert "--state-file" in cmd
    assert "--state-key" in cmd and "unit" in cmd
    assert "--log-csv" in cmd
    assert "--" in cmd
    sep = cmd.index("--")
    assert cmd[sep + 1 : sep + 5] == ["--strategy", "valmom_v1", "--data-dir", str(Path.home() / "data")]


def test_presets_example_includes_opt_in_dual_mom_core_vt() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    presets = daily_signal._load_presets_json(repo_root / "configs" / "presets.example.json")

    assert "dual_mom_core" in presets
    assert "dual_mom_core_vt" in presets

    base_args = presets["dual_mom_core"].run_backtest_args
    vt_args = presets["dual_mom_core_vt"].run_backtest_args

    assert "--vol-target" not in base_args
    assert "--vol-lookback" not in base_args
    assert "--min-leverage" not in base_args
    assert "--max-leverage" not in base_args

    assert vt_args[vt_args.index("--vol-target") + 1] == "0.12"
    assert vt_args[vt_args.index("--vol-lookback") + 1] == "21"
    assert vt_args[vt_args.index("--min-leverage") + 1] == "0.0"
    assert vt_args[vt_args.index("--max-leverage") + 1] == "1.0"


def test_presets_example_includes_dual_mom_vol10_cash_core() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    presets = daily_signal._load_presets_json(repo_root / "configs" / "presets.example.json")

    assert "dual_mom_vol10_cash_core" in presets
    args = presets["dual_mom_vol10_cash_core"].run_backtest_args

    assert args[args.index("--strategy") + 1] == "dual_mom_vol10_cash"
    assert args[args.index("--dmv-defensive-symbol") + 1] == "BIL"
    assert args[args.index("--dmv-mom-lookback") + 1] == "63"
    assert args[args.index("--dmv-rebalance") + 1] == "21"
    assert args[args.index("--dmv-vol-lookback") + 1] == "20"
    assert args[args.index("--dmv-target-vol") + 1] == "0.10"
    assert "--end" not in args
    assert "--vol-target" not in args
    assert "--ivol" not in args


def test_daily_signal_emit_json_is_one_line_and_matches_run_backtest(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "synth"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    state_path = tmp_path / "state.json"
    state_path.write_text("DIFFERENT_EVENT_ID", encoding="utf-8")

    presets = {
        "presets": {
            "t": {
                "description": "test",
                "mode": "change_only",
                "emit": "json",
                "state_file": str(state_path),
                "state_key": "t",
                "run_backtest_args": _rb_args(data_dir),
            }
        }
    }
    presets_path = tmp_path / "presets.json"
    presets_path.write_text(json.dumps(presets), encoding="utf-8")

    rb_cmd = [sys.executable, str(repo_root / "scripts" / "run_backtest.py"), *_rb_args(data_dir), "--next-action-json"]
    rb = subprocess.run(rb_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert rb.returncode == 0, f"stdout={rb.stdout!r}\nstderr={rb.stderr!r}"
    rb_lines = rb.stdout.splitlines()
    assert len(rb_lines) == 1
    rb_line = rb_lines[0]

    ds_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_signal.py"),
        "--preset",
        "t",
        "--presets-file",
        str(presets_path),
    ]
    ds = subprocess.run(ds_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert ds.returncode == 0, f"stdout={ds.stdout!r}\nstderr={ds.stderr!r}"
    ds_lines = ds.stdout.splitlines()
    assert len(ds_lines) == 1
    assert ds_lines[0] == rb_line


def test_daily_signal_no_emit_produces_truly_empty_stdout(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "synth"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    rb_cmd = [sys.executable, str(repo_root / "scripts" / "run_backtest.py"), *_rb_args(data_dir), "--next-action-json"]
    rb = subprocess.run(rb_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert rb.returncode == 0, f"stdout={rb.stdout!r}\nstderr={rb.stderr!r}"
    payload = json.loads(rb.stdout.splitlines()[0])
    state_path = tmp_path / "state.json"
    state_path.write_text(str(payload["event_id"]), encoding="utf-8")

    presets = {
        "presets": {
            "t": {
                "description": "test",
                "mode": "change_only",
                "emit": "text",
                "state_file": str(state_path),
                "state_key": "t",
                "run_backtest_args": _rb_args(data_dir),
            }
        }
    }
    presets_path = tmp_path / "presets.json"
    presets_path.write_text(json.dumps(presets), encoding="utf-8")

    ds_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_signal.py"),
        "--preset",
        "t",
        "--presets-file",
        str(presets_path),
    ]
    ds = subprocess.run(ds_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert ds.returncode == 0, f"stdout={ds.stdout!r}\nstderr={ds.stderr!r}"
    assert ds.stdout == ""


def test_daily_signal_log_csv_appends_only_on_emit(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    if not _supports_log_csv(repo_root, env):
        pytest.skip("next_action_alert.py does not support --log-csv in this ref")

    data_dir = tmp_path / "synth"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    state_path = tmp_path / "state.json"
    state_path.write_text("DIFFERENT_EVENT_ID", encoding="utf-8")
    csv_path = tmp_path / "alerts.csv"

    presets = {
        "presets": {
            "t": {
                "description": "test",
                "mode": "change_only",
                "emit": "json",
                "state_file": str(state_path),
                "state_key": "t",
                "log_csv": str(csv_path),
                "run_backtest_args": _rb_args(data_dir),
            }
        }
    }
    presets_path = tmp_path / "presets.json"
    presets_path.write_text(json.dumps(presets), encoding="utf-8")

    ds_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_signal.py"),
        "--preset",
        "t",
        "--presets-file",
        str(presets_path),
    ]
    first = subprocess.run(ds_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert first.returncode == 0, f"stdout={first.stdout!r}\nstderr={first.stderr!r}"
    assert csv_path.exists()
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1

    second = subprocess.run(ds_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert second.returncode == 0, f"stdout={second.stdout!r}\nstderr={second.stderr!r}"
    assert second.stdout == ""
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows2 = list(csv.DictReader(f))
    assert len(rows2) == 1
