from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.data import LocalStore


def test_rebalance_anchor_keeps_next_rebalance_stable_across_start_windows(tmp_path):
    idx = pd.date_range("2021-01-01", periods=500, freq="B")

    def _bars(close: pd.Series) -> pd.DataFrame:
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

    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_shy = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=tmp_path)
    store.write_bars("AAA", _bars(pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars(pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars(pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("SHY", _bars(pd.Series(100.0 * np.cumprod(1.0 + ret_shy), index=idx)))

    repo_root = Path(__file__).resolve().parents[1]
    base_cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
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
        "--rebalance-anchor-date",
        "2021-01-01",
        "--end",
        "2022-12-01",
        "--no-plot",
        "--next-action-json",
        "--data-dir",
        str(tmp_path),
    ]

    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path

    cmd_a = [*base_cmd, "--start", "2021-01-01"]
    cmd_b = [*base_cmd, "--start", "2022-01-03"]

    proc_a = subprocess.run(cmd_a, capture_output=True, text=True, env=env, cwd=str(repo_root))
    proc_b = subprocess.run(cmd_b, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc_a.returncode == 0, f"stdout={proc_a.stdout!r}\nstderr={proc_a.stderr!r}"
    assert proc_b.returncode == 0, f"stdout={proc_b.stdout!r}\nstderr={proc_b.stderr!r}"

    lines_a = proc_a.stdout.splitlines()
    lines_b = proc_b.stdout.splitlines()
    assert len(lines_a) == 1, f"Expected 1 line, got {len(lines_a)}: stdout={proc_a.stdout!r} stderr={proc_a.stderr!r}"
    assert len(lines_b) == 1, f"Expected 1 line, got {len(lines_b)}: stdout={proc_b.stdout!r} stderr={proc_b.stderr!r}"

    obj_a = json.loads(lines_a[0])
    obj_b = json.loads(lines_b[0])

    assert isinstance(obj_a["next_rebalance"], str) and obj_a["next_rebalance"]
    assert isinstance(obj_b["next_rebalance"], str) and obj_b["next_rebalance"]
    assert obj_a["next_rebalance"] == obj_b["next_rebalance"]
    assert str(obj_a["event_id"]).endswith(f":{obj_a['next_rebalance']}")
    assert str(obj_b["event_id"]).endswith(f":{obj_b['next_rebalance']}")
