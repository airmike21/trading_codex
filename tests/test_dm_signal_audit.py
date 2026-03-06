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


def _expected_event_id(payload: dict[str, object]) -> str:
    def s(value: object) -> str:
        return "" if value is None else str(value)

    return (
        f"{s(payload.get('date'))}:"
        f"{s(payload.get('strategy'))}:"
        f"{s(payload.get('action'))}:"
        f"{s(payload.get('symbol'))}:"
        f"{s(payload.get('target_shares'))}:"
        f"{s(payload.get('resize_new_shares'))}:"
        f"{s(payload.get('next_rebalance'))}"
    )


def _run_next_action_json(
    *,
    repo_root: Path,
    env: dict[str, str],
    data_dir: Path,
    extra_args: list[str],
) -> dict[str, object]:
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        *extra_args,
        "--next-action-json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    payload = json.loads(lines[0])
    assert payload["event_id"] == _expected_event_id(payload)
    return payload


def test_dual_mom_v1_next_action_enter_then_hold_contracts(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    idx = pd.date_range("2021-01-01", periods=30, freq="B")

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(np.linspace(100.0, 130.0, len(idx)), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(np.linspace(120.0, 90.0, len(idx)), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx)))

    base_args = [
        "--strategy",
        "dual_mom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "--dm-defensive-symbol",
        "BIL",
        "--dm-lookback",
        "1",
        "--dm-top-n",
        "1",
        "--dm-rebalance",
        "5",
        "--start",
        idx[0].date().isoformat(),
        "--no-plot",
        "--data-dir",
        str(tmp_path),
    ]

    enter_payload = _run_next_action_json(
        repo_root=repo_root,
        env=env,
        data_dir=tmp_path,
        extra_args=[*base_args, "--end", idx[5].date().isoformat()],
    )
    hold_payload = _run_next_action_json(
        repo_root=repo_root,
        env=env,
        data_dir=tmp_path,
        extra_args=[*base_args, "--end", idx[6].date().isoformat()],
    )

    assert enter_payload["action"] == "ENTER"
    assert enter_payload["symbol"] == "AAA"
    assert enter_payload["next_rebalance"] == "2021-01-14"

    assert hold_payload["action"] == "HOLD"
    assert hold_payload["symbol"] == "AAA"
    assert hold_payload["next_rebalance"] == "2021-01-14"


def test_dual_mom_v1_next_action_resize_uses_trading_day_rebalance_cadence(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    idx = pd.date_range("2021-01-01", periods=80, freq="B")

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(np.linspace(100.0, 180.0, len(idx)), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(np.linspace(130.0, 90.0, len(idx)), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx)))

    payload = _run_next_action_json(
        repo_root=repo_root,
        env=env,
        data_dir=tmp_path,
        extra_args=[
            "--strategy",
            "dual_mom_v1",
            "--symbols",
            "AAA",
            "BBB",
            "--dm-defensive-symbol",
            "BIL",
            "--dm-lookback",
            "1",
            "--dm-top-n",
            "1",
            "--dm-rebalance",
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
            idx[15].date().isoformat(),
            "--no-plot",
            "--data-dir",
            str(tmp_path),
        ],
    )

    assert payload["date"] == "2021-01-22"
    assert payload["action"] == "RESIZE"
    assert payload["symbol"] == "AAA"
    assert payload["resize_prev_shares"] == 175
    assert payload["resize_new_shares"] == 173
    assert payload["target_shares"] == 173
    assert payload["next_rebalance"] == "2021-01-28"


def test_dual_mom_v1_rotates_to_bil_when_risk_momentum_turns_negative(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    idx = pd.date_range("2021-01-01", periods=40, freq="B")

    aaa = pd.Series(np.r_[np.linspace(100.0, 120.0, 15), np.linspace(119.0, 80.0, 25)], index=idx)
    bbb = pd.Series(np.linspace(110.0, 90.0, len(idx)), index=idx)
    bil = pd.Series(np.linspace(100.0, 101.5, len(idx)), index=idx)

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars_for_index(idx, aaa))
    store.write_bars("BBB", _bars_for_index(idx, bbb))
    store.write_bars("BIL", _bars_for_index(idx, bil))

    payload = _run_next_action_json(
        repo_root=repo_root,
        env=env,
        data_dir=tmp_path,
        extra_args=[
            "--strategy",
            "dual_mom_v1",
            "--symbols",
            "AAA",
            "BBB",
            "--dm-defensive-symbol",
            "BIL",
            "--dm-lookback",
            "5",
            "--dm-top-n",
            "1",
            "--dm-rebalance",
            "5",
            "--start",
            idx[0].date().isoformat(),
            "--end",
            idx[20].date().isoformat(),
            "--no-plot",
            "--data-dir",
            str(tmp_path),
        ],
    )

    assert payload["date"] == "2021-01-29"
    assert payload["action"] == "ROTATE"
    assert payload["symbol"] == "BIL"
    assert payload["target_shares"] == 99
    assert payload["next_rebalance"] == "2021-02-04"


def test_dual_mom_v1_due_mode_repeated_identical_runs_emit_once_then_silent(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    idx = pd.date_range("2021-01-01", periods=30, freq="B")

    store = LocalStore(base_dir=tmp_path / "data")
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(np.linspace(100.0, 130.0, len(idx)), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(np.linspace(120.0, 90.0, len(idx)), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx)))

    rb_args = [
        "--strategy",
        "dual_mom_v1",
        "--symbols",
        "AAA",
        "BBB",
        "--dm-defensive-symbol",
        "BIL",
        "--dm-lookback",
        "1",
        "--dm-top-n",
        "1",
        "--dm-rebalance",
        "5",
        "--start",
        idx[0].date().isoformat(),
        "--end",
        idx[6].date().isoformat(),
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

    state_path = tmp_path / "dual_mom_due_state.json"
    state_path.write_text(str(rb_payload["event_id"]), encoding="utf-8")
    csv_path = tmp_path / "dual_mom_due_alerts.csv"
    presets_path = tmp_path / "presets.json"
    presets_path.write_text(
        json.dumps(
            {
                "presets": {
                    "dual_mom_due": {
                        "description": "dual_mom_v1 due audit",
                        "mode": "change_or_rebalance_due",
                        "emit": "json",
                        "state_file": str(state_path),
                        "state_key": "dual_mom_due",
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
        "dual_mom_due",
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
    assert rows[0]["symbol"] == "AAA"
