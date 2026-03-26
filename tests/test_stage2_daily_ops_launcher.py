from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


POWERSHELL_EXE = shutil.which("powershell.exe")


def _run_wrapper(*extra_args: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL_EXE is None:
        pytest.skip("powershell.exe is required for Stage 2 daily ops wrapper tests")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "scripts" / "windows" / "trading_codex_stage2_daily_ops.ps1"),
        "-PrintOnly",
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def test_print_only_renders_wsl_daily_ops_command() -> None:
    proc = _run_wrapper(
        "-WslRepoPath",
        "/__trading_codex_stage2_ops__",
        "-WslPython",
        "/__trading_codex_stage2_ops__/.venv/bin/python",
        "-PresetsFile",
        "/tmp/trading_codex_presets.json",
        "-ArchiveRoot",
        "/tmp/trading_codex_archive",
        "-PaperBaseDir",
        "/tmp/trading_codex_paper",
        "-Timestamp",
        "2026-03-26T16:10:00-05:00",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "repo_path=/__trading_codex_stage2_ops__" in stdout
    assert "python_path=/__trading_codex_stage2_ops__/.venv/bin/python" in stdout
    assert "scripts/paper_lane_daily_ops.py" in stdout
    assert "'--preset' 'dual_mom_vol10_cash_core'" in stdout
    assert "'--provider' 'stooq'" in stdout
    assert "'--presets-file' '/tmp/trading_codex_presets.json'" in stdout
    assert "'--archive-root' '/tmp/trading_codex_archive'" in stdout
    assert "'--paper-base-dir' '/tmp/trading_codex_paper'" in stdout
    assert "'--timestamp' '2026-03-26T16:10:00-05:00'" in stdout
