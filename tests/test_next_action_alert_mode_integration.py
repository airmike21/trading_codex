from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.data import LocalStore


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


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def _make_vm_synthetic_store(tmp_path: Path) -> None:
    idx = pd.date_range("2019-01-01", periods=520, freq="B")
    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_shy = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("SHY", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_shy), index=idx)))


def _valmom_rb_args(tmp_path: Path) -> list[str]:
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
        str(tmp_path),
    ]


def test_change_or_rebalance_due_subprocess_smoke_emits_once(tmp_path):
    _make_vm_synthetic_store(tmp_path)
    repo_root, env = _repo_root_and_env()
    rb_args = _valmom_rb_args(tmp_path)

    payload_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *rb_args,
        "--next-action-json",
    ]
    payload_proc = subprocess.run(payload_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert payload_proc.returncode == 0, f"stdout={payload_proc.stdout!r}\nstderr={payload_proc.stderr!r}"

    payload_lines = payload_proc.stdout.splitlines()
    assert len(payload_lines) == 1, (
        f"Expected 1 line, got {len(payload_lines)}: "
        f"stdout={payload_proc.stdout!r} stderr={payload_proc.stderr!r}"
    )
    payload = json.loads(payload_lines[0])
    assert isinstance(payload.get("next_rebalance"), str) and payload["next_rebalance"]

    state_file = tmp_path / "na_state.json"
    state_file.write_text(str(payload["event_id"]) + "\n", encoding="utf-8")

    alert_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "next_action_alert.py"),
        "--mode",
        "change_or_rebalance_due",
        "--emit",
        "json",
        "--state-file",
        str(state_file),
        "--",
        *rb_args,
    ]

    proc1 = subprocess.run(alert_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    proc2 = subprocess.run(alert_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc1.returncode == 0, f"stdout={proc1.stdout!r}\nstderr={proc1.stderr!r}"
    assert proc2.returncode == 0, f"stdout={proc2.stdout!r}\nstderr={proc2.stderr!r}"

    first_lines = proc1.stdout.splitlines()
    assert len(first_lines) == 1, (
        f"Expected 1 line, got {len(first_lines)}: "
        f"stdout={proc1.stdout!r} stderr={proc1.stderr!r}"
    )
    assert proc2.stdout == ""

    saved = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved["last_event_id"] == payload["event_id"]
    assert saved["last_due_fingerprint"].endswith(f":{payload['next_rebalance']}")


def test_change_only_no_emit_outputs_truly_empty_stdout_regression(tmp_path):
    _make_vm_synthetic_store(tmp_path)
    repo_root, env = _repo_root_and_env()
    rb_args = _valmom_rb_args(tmp_path)

    payload_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *rb_args,
        "--next-action-json",
    ]
    payload_proc = subprocess.run(payload_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert payload_proc.returncode == 0, f"stdout={payload_proc.stdout!r}\nstderr={payload_proc.stderr!r}"
    payload_lines = payload_proc.stdout.splitlines()
    assert len(payload_lines) == 1, (
        f"Expected 1 line, got {len(payload_lines)}: "
        f"stdout={payload_proc.stdout!r} stderr={payload_proc.stderr!r}"
    )
    payload = json.loads(payload_lines[0])

    state_file = tmp_path / "na_state_legacy.txt"
    state_file.write_text(str(payload["event_id"]) + "\n", encoding="utf-8")

    alert_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "next_action_alert.py"),
        "--mode",
        "change_only",
        "--emit",
        "text",
        "--state-file",
        str(state_file),
        "--",
        *rb_args,
    ]
    proc_no_emit = subprocess.run(alert_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc_no_emit.returncode == 0, f"stdout={proc_no_emit.stdout!r}\nstderr={proc_no_emit.stderr!r}"
    assert proc_no_emit.stdout == "", f"Expected no stdout at all, got: {proc_no_emit.stdout!r}"
    assert proc_no_emit.stdout.splitlines() == []
