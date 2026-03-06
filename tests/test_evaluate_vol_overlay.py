from __future__ import annotations

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
    idx = pd.date_range("2018-01-01", periods=700, freq="B")
    ret_a = np.full(len(idx), 0.0009)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.018, -0.014)
    ret_c = np.where(np.arange(len(idx)) % 3 == 0, 0.013, -0.007)
    ret_bil = np.full(len(idx), 0.00015)

    store = LocalStore(base_dir=base_dir)
    store.write_bars("AAA", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx)))
    store.write_bars("BBB", _bars_for_index(idx, pd.Series(110.0 * np.cumprod(1.0 + ret_b), index=idx)))
    store.write_bars("CCC", _bars_for_index(idx, pd.Series(95.0 * np.cumprod(1.0 + ret_c), index=idx)))
    store.write_bars("BIL", _bars_for_index(idx, pd.Series(100.0 * np.cumprod(1.0 + ret_bil), index=idx)))


def test_evaluate_vol_overlay_writes_csv_and_summary(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_synth_store(data_dir)

    presets_path = tmp_path / "presets.json"
    presets_path.write_text(
        """
{
  "presets": {
    "vm_core": {
      "description": "test",
      "run_backtest_args": [
        "--strategy", "valmom_v1",
        "--symbols", "AAA", "BBB", "CCC",
        "--vm-defensive-symbol", "BIL",
        "--vm-mom-lookback", "63",
        "--vm-val-lookback", "126",
        "--vm-top-n", "2",
        "--vm-rebalance", "21",
        "--start", "2019-01-01",
        "--end", "2020-12-31",
        "--no-plot",
        "--data-dir", "%s"
      ]
    },
    "dual_mom_core": {
      "description": "test",
      "run_backtest_args": [
        "--strategy", "dual_mom",
        "--symbols", "AAA", "BBB", "CCC",
        "--defensive", "BIL",
        "--mom-lookback", "63",
        "--rebalance", "M",
        "--start", "2019-01-01",
        "--end", "2020-12-31",
        "--no-plot",
        "--data-dir", "%s"
      ]
    }
  }
}
"""
        % (str(data_dir), str(data_dir)),
        encoding="utf-8",
    )

    csv_out = tmp_path / "vol_overlay_eval.csv"
    summary_out = tmp_path / "vol_overlay_eval.md"
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "evaluate_vol_overlay.py"),
        "--presets-file",
        str(presets_path),
        "--target-vols",
        "0.10",
        "--vol-lookbacks",
        "21",
        "--recent-years",
        "1",
        "--csv-out",
        str(csv_out),
        "--summary-out",
        str(summary_out),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    assert csv_out.exists()
    assert summary_out.exists()

    df = pd.read_csv(csv_out)
    assert set(df["strategy"]) == {"dual_mom", "valmom_v1"}
    assert set(df["period"]) == {"full", "recent_1y"}
    assert set(df["config_label"]) == {"baseline", "tv_0.10_lb_21"}
    assert len(df) == 8
    assert {"cagr", "annualized_vol", "sharpe", "calmar", "average_leverage", "trade_count"}.issubset(df.columns)

    summary = summary_out.read_text(encoding="utf-8")
    assert "overlay default for dual_mom" in summary
    assert "overlay default for valmom_v1" in summary
    assert "single recommended default parameter set" in summary
