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


def _price_series(index: pd.DatetimeIndex, returns: np.ndarray, base: float) -> pd.Series:
    return pd.Series(base * np.cumprod(1.0 + returns.astype(float)), index=index)


def _write_symbol_bars(store: LocalStore, symbol: str, close: pd.Series) -> None:
    store.write_bars(
        symbol,
        pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=close.index,
        ),
    )


def _write_validation_store(
    base_dir: Path,
    *,
    index: pd.DatetimeIndex,
    include_bil: bool = True,
) -> None:
    store = LocalStore(base_dir=base_dir)
    alt = np.arange(len(index))
    close_map = {
        "SPY": _price_series(index, np.full(len(index), 0.0005), 100.0),
        "QQQ": _price_series(index, np.where(alt % 2 == 0, 0.0015, -0.0010), 105.0),
        "IWM": _price_series(index, np.full(len(index), -0.0001), 95.0),
        "EFA": _price_series(index, np.where(alt % 2 == 0, 0.03, -0.01), 98.0),
    }
    if include_bil:
        close_map["BIL"] = _price_series(index, np.full(len(index), 0.0001), 100.0)

    for symbol, close in close_map.items():
        _write_symbol_bars(store, symbol, close)


def _run_shadow_validate(
    *,
    repo_root: Path,
    env: dict[str, str],
    data_dir: Path,
    shadow_dir: Path,
    start: str,
    end: str,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "shadow_validate_dual_mom_vol10_cash.py"),
        "--data-dir",
        str(data_dir),
        "--shadow-artifacts-dir",
        str(shadow_dir),
        "--start",
        start,
        "--end",
        end,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))


def test_shadow_validate_dual_mom_vol10_cash_clean_case_writes_readiness_artifacts(
    tmp_path: Path,
) -> None:
    repo_root, env = _repo_root_and_env()
    end = pd.Timestamp.now().normalize()
    idx = pd.bdate_range(end=end, periods=320)

    data_dir = tmp_path / "data"
    shadow_dir = tmp_path / "shadow"
    _write_validation_store(data_dir, index=idx)

    proc = _run_shadow_validate(
        repo_root=repo_root,
        env=env,
        data_dir=data_dir,
        shadow_dir=shadow_dir,
        start=idx[0].date().isoformat(),
        end=idx[-1].date().isoformat(),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    summary = json.loads(lines[0])
    json_path = Path(summary["json_artifact"])
    markdown_path = Path(summary["markdown_artifact"])
    assert json_path.exists()
    assert markdown_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["strategy"] == "dual_mom_vol10_cash"
    assert payload["as_of_date"] == idx[-1].date().isoformat()
    assert payload["data_dir"] == str(data_dir)
    assert payload["required_symbols"] == ["SPY", "QQQ", "IWM", "EFA", "BIL"]
    assert payload["loaded_symbols"] == ["SPY", "QQQ", "IWM", "EFA", "BIL"]
    assert payload["missing_symbols"] == []
    assert payload["history_rows"] == len(idx)
    assert payload["minimum_history_rows"] == 85
    assert payload["validated_with_cached_data"] is True
    assert payload["shadow_review_state"] == "clean"
    assert payload["automation_decision"] == "allow"
    assert payload["automation_status"] == "automation_ready"
    assert payload["warning_reasons"] == []
    assert payload["blocking_reasons"] == []
    assert isinstance(payload["target_shares"], int)
    assert payload["next_action_summary"]
    assert "shadow_validate_dual_mom_vol10_cash.py" in payload["command"]
    assert "scripts/run_backtest.py" in payload["run_backtest_command"]

    markdown = markdown_path.read_text(encoding="utf-8")
    assert "- Data dir:" in markdown
    assert "- Required symbols: `SPY, QQQ, IWM, EFA, BIL`" in markdown
    assert "- Automation decision: `allow`" in markdown
    assert "- Automation status: `automation_ready`" in markdown
    assert "- Validated with cached data: `true`" in markdown
    assert "- Next action summary:" in markdown


def test_shadow_validate_dual_mom_vol10_cash_blocks_on_missing_symbol_and_insufficient_history(
    tmp_path: Path,
) -> None:
    repo_root, env = _repo_root_and_env()
    end = pd.Timestamp.now().normalize()
    idx = pd.bdate_range(end=end, periods=40)

    data_dir = tmp_path / "data"
    shadow_dir = tmp_path / "shadow"
    _write_validation_store(data_dir, index=idx, include_bil=False)

    proc = _run_shadow_validate(
        repo_root=repo_root,
        env=env,
        data_dir=data_dir,
        shadow_dir=shadow_dir,
        start=idx[0].date().isoformat(),
        end=idx[-1].date().isoformat(),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    summary = json.loads(proc.stdout.splitlines()[0])
    payload = json.loads(Path(summary["json_artifact"]).read_text(encoding="utf-8"))

    assert payload["validated_with_cached_data"] is False
    assert payload["loaded_symbols"] == ["SPY", "QQQ", "IWM", "EFA"]
    assert payload["missing_symbols"] == ["BIL"]
    assert payload["history_rows"] == 40
    assert payload["minimum_history_rows"] == 85
    assert payload["actual_symbol_count"] == 4
    assert payload["shadow_review_state"] == "blocked"
    assert payload["automation_decision"] == "block"
    assert payload["automation_status"] == "blocked"
    assert "missing_required_symbols" in payload["blocking_reasons"]
    assert "insufficient_history" in payload["blocking_reasons"]
    assert "symbol_count_mismatch" in payload["blocking_reasons"]
    assert payload["target_shares"] == 0
    assert payload["actions"][0]["action"] == "HOLD"
    assert payload["actions"][0]["symbol"] == "CASH"

    markdown = Path(summary["markdown_artifact"]).read_text(encoding="utf-8")
    assert "## Blockers" in markdown
    assert "- missing_required_symbols" in markdown
    assert "- insufficient_history" in markdown
    assert "- symbol_count_mismatch" in markdown


def test_shadow_validate_dual_mom_vol10_cash_stale_data_is_warning_not_block(
    tmp_path: Path,
) -> None:
    repo_root, env = _repo_root_and_env()
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=10)
    idx = pd.bdate_range(end=end, periods=320)

    data_dir = tmp_path / "data"
    shadow_dir = tmp_path / "shadow"
    _write_validation_store(data_dir, index=idx)

    proc = _run_shadow_validate(
        repo_root=repo_root,
        env=env,
        data_dir=data_dir,
        shadow_dir=shadow_dir,
        start=idx[0].date().isoformat(),
        end=idx[-1].date().isoformat(),
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    summary = json.loads(proc.stdout.splitlines()[0])
    payload = json.loads(Path(summary["json_artifact"]).read_text(encoding="utf-8"))

    assert payload["validated_with_cached_data"] is True
    assert payload["shadow_review_state"] == "warning"
    assert payload["automation_decision"] == "review"
    assert payload["automation_status"] == "review_required"
    assert payload["warning_reasons"] == ["stale_data"]
    assert payload["blocking_reasons"] == []
    assert payload["warnings"]
    assert "stale" in payload["warnings"][0].lower()

    markdown = Path(summary["markdown_artifact"]).read_text(encoding="utf-8")
    assert "- Automation decision: `review`" in markdown
    assert "- Automation status: `review_required`" in markdown
    assert "## Warnings" in markdown
    assert "- stale_data" in markdown
