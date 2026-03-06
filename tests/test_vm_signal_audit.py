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
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000,
        },
        index=idx,
    )


def test_valmom_v1_next_action_resize_uses_trading_day_rebalance_cadence(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    idx = pd.date_range("2021-01-01", periods=80, freq="B")

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(np.linspace(100.0, 180.0, len(idx)), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(np.linspace(130.0, 90.0, len(idx)), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx)))

    end_date = idx[15].date().isoformat()  # Trading-day rebalance update, but not month-end.
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        "--strategy",
        "valmom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "--vm-defensive-symbol",
        "BIL",
        "--vm-mom-lookback",
        "1",
        "--vm-val-lookback",
        "3",
        "--vm-top-n",
        "1",
        "--vm-rebalance",
        "5",
        "--vol-target",
        "0.10",
        "--vol-lookback",
        "5",
        "--max-leverage",
        "2",
        "--min-leverage",
        "0",
        "--vol-update",
        "rebalance",
        "--start",
        idx[0].date().isoformat(),
        "--end",
        end_date,
        "--no-plot",
        "--next-action-json",
        "--data-dir",
        str(tmp_path),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    payload = json.loads(lines[0])

    assert payload["date"] == end_date
    assert payload["strategy"] == "valmom_v1"
    assert payload["symbol"] == "AAA"
    assert payload["action"] == "RESIZE"
    assert payload["resize_prev_shares"] == 175
    assert payload["resize_new_shares"] == 173
    assert payload["target_shares"] == 173
    assert payload["next_rebalance"] == "2021-01-28"
    assert str(payload["event_id"]).endswith(":173:2021-01-28")


def test_daily_signal_vm_core_due_bil_defensive_emits_once_then_is_silent(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    idx = pd.date_range("2019-01-01", periods=520, freq="B")

    store = LocalStore(base_dir=tmp_path / "data")
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(np.linspace(120.0, 70.0, len(idx)), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(np.linspace(110.0, 60.0, len(idx)), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(np.linspace(100.0, 55.0, len(idx)), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(np.linspace(100.0, 103.0, len(idx)), index=idx)))

    rb_args = [
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
        str(tmp_path / "data"),
    ]

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
    assert rb_payload["symbol"] == "BIL"

    state_path = tmp_path / "vm_core_due_state.json"
    state_path.write_text(str(rb_payload["event_id"]), encoding="utf-8")
    csv_path = tmp_path / "vm_core_due_alerts.csv"

    presets_path = tmp_path / "presets.json"
    presets_path.write_text(
        json.dumps(
            {
                "presets": {
                    "vm_core_due": {
                        "description": "audit test",
                        "mode": "change_or_rebalance_due",
                        "emit": "json",
                        "state_file": str(state_path),
                        "state_key": "vm_core_due",
                        "log_csv": str(csv_path),
                        "run_backtest_args": rb_args,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    ds_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "daily_signal.py"),
        "--preset",
        "vm_core_due",
        "--presets-file",
        str(presets_path),
    ]
    first = subprocess.run(ds_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    second = subprocess.run(ds_cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert first.returncode == 0, f"stdout={first.stdout!r}\nstderr={first.stderr!r}"
    assert second.returncode == 0, f"stdout={second.stdout!r}\nstderr={second.stderr!r}"
    assert first.stdout.splitlines() == [rb_line]
    assert second.stdout == ""

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["event_id"] == str(rb_payload["event_id"])
    assert rows[0]["symbol"] == "BIL"
