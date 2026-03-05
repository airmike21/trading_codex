from __future__ import annotations

import csv
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


def _expected_event_id(payload: dict[str, object]) -> str:
    # Contract: "{date}:{strategy}:{action}:{symbol}:{target_shares}:{resize_new_shares}:{next_rebalance}"
    def s(v: object) -> str:
        return "" if v is None else str(v)

    return (
        f"{s(payload.get('date'))}:"
        f"{s(payload.get('strategy'))}:"
        f"{s(payload.get('action'))}:"
        f"{s(payload.get('symbol'))}:"
        f"{s(payload.get('target_shares'))}:"
        f"{s(payload.get('resize_new_shares'))}:"
        f"{s(payload.get('next_rebalance'))}"
    )


def test_run_backtest_next_action_json_contracts(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "synth"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--next-action-json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    # Exactly one output line
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    line = lines[0]
    assert "\r" not in line
    assert "\t" not in line
    assert ", " not in line
    assert ": " not in line

    payload = json.loads(line)
    assert "event_id" in payload
    assert payload["event_id"] == _expected_event_id(payload)
    assert line == json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def test_run_backtest_next_action_text_is_exactly_one_line(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "synth"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *_rb_args(data_dir),
        "--next-action",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert lines[0] != ""
    assert "\r" not in lines[0]


def test_next_action_alert_json_passthrough_and_csv_logs_only_on_emit(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "synth"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)
    rb_args = _rb_args(data_dir)

    rb_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *rb_args,
        "--next-action-json",
    ]
    rb_proc = subprocess.run(rb_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert rb_proc.returncode == 0, f"stdout={rb_proc.stdout!r}\nstderr={rb_proc.stderr!r}"
    rb_lines = rb_proc.stdout.splitlines()
    assert len(rb_lines) == 1, f"stdout={rb_proc.stdout!r} stderr={rb_proc.stderr!r}"
    rb_line = rb_lines[0]
    rb_payload = json.loads(rb_line)

    state_file = tmp_path / "state.txt"
    state_file.write_text("DIFFERENT_EVENT_ID", encoding="utf-8")
    csv_log = tmp_path / "alerts.csv"

    na_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "next_action_alert.py"),
        "--mode",
        "change_only",
        "--emit",
        "json",
        "--log-csv",
        str(csv_log),
        "--state-file",
        str(state_file),
        "--state-key",
        "contracts_csv",
        "--",
        *rb_args,
    ]
    first = subprocess.run(na_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert first.returncode == 0, f"stdout={first.stdout!r}\nstderr={first.stderr!r}"
    first_lines = first.stdout.splitlines()
    assert len(first_lines) == 1
    assert first_lines[0] == rb_line
    assert first.stderr == ""

    with csv_log.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["event_id"] == rb_payload["event_id"]
    assert rows[0]["emit_kind"] == "json"
    assert rows[0]["emit_line"] == rb_line

    second = subprocess.run(na_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert second.returncode == 0, f"stdout={second.stdout!r}\nstderr={second.stderr!r}"
    assert second.stdout == ""
    assert second.stderr == ""

    with csv_log.open("r", encoding="utf-8", newline="") as f:
        rows_after = list(csv.DictReader(f))
    assert len(rows_after) == 1
