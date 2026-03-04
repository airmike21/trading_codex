from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from trading_codex.backtest.next_rebalance import compute_next_rebalance_date
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


def _write_valmom_fixture_data(base_dir: Path) -> pd.DatetimeIndex:
    idx = pd.date_range("2021-01-01", periods=500, freq="B")

    ret_a = np.full(len(idx), 0.0012)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.025, -0.02)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.015, -0.008)
    ret_shy = np.full(len(idx), 0.0002)

    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("SHY", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_shy), index=idx)))
    return idx


def _run_next_action_json(
    data_dir: Path,
    config_path: Path,
    extra_args: list[str] | None = None,
) -> dict[str, object]:
    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
        "--config",
        str(config_path),
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
        "2022-01-03",
        "--end",
        "2022-12-01",
        "--no-plot",
        "--next-action-json",
        "--data-dir",
        str(data_dir),
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"Expected 1 line, got {len(lines)}: stdout={proc.stdout!r} stderr={proc.stderr!r}"
    return json.loads(lines[0])


def test_config_rebalance_anchor_used_when_cli_missing(tmp_path):
    idx = _write_valmom_fixture_data(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text('rebalance_anchor_date = "2021-01-01"\n', encoding="utf-8")

    obj = _run_next_action_json(data_dir=tmp_path, config_path=config_path)

    assert isinstance(obj["next_rebalance"], str) and obj["next_rebalance"]
    assert str(obj["event_id"]).endswith(f":{obj['next_rebalance']}")

    idx_window = idx[(idx >= pd.Timestamp("2022-01-03")) & (idx <= pd.Timestamp("2022-12-01"))]
    expected = compute_next_rebalance_date(
        idx_window,
        pd.Timestamp(obj["date"]),
        trading_days=21,
        anchor_date="2021-01-01",
    )
    assert obj["next_rebalance"] == expected


def test_cli_rebalance_anchor_overrides_config(tmp_path):
    idx = _write_valmom_fixture_data(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text('rebalance_anchor_date = "2021-01-01"\n', encoding="utf-8")

    obj = _run_next_action_json(
        data_dir=tmp_path,
        config_path=config_path,
        extra_args=["--rebalance-anchor-date", "2021-02-01"],
    )

    assert isinstance(obj["next_rebalance"], str) and obj["next_rebalance"]
    assert str(obj["event_id"]).endswith(f":{obj['next_rebalance']}")

    idx_window = idx[(idx >= pd.Timestamp("2022-01-03")) & (idx <= pd.Timestamp("2022-12-01"))]
    expected = compute_next_rebalance_date(
        idx_window,
        pd.Timestamp(obj["date"]),
        trading_days=21,
        anchor_date="2021-02-01",
    )
    assert obj["next_rebalance"] == expected
