from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


POWERSHELL_EXE = shutil.which("powershell.exe")


def _run_wrapper(*extra_args: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL_EXE is None:
        pytest.skip("powershell.exe is required for Stage 2 shadow daily ops wrapper tests")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "scripts" / "windows" / "trading_codex_stage2_shadow_daily_ops.ps1"),
        "-PrintOnly",
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def test_print_only_renders_wsl_shadow_daily_ops_command() -> None:
    proc = _run_wrapper(
        "-WslRepoPath",
        "/__trading_codex_shadow_ops__",
        "-WslPython",
        "/__trading_codex_shadow_ops__/.venv/bin/python",
        "-ShadowOpsConfig",
        "/tmp/stage2_shadow_ops.json",
        "-DataDir",
        "/tmp/trading_codex_data",
        "-ArchiveRoot",
        "/tmp/trading_codex_archive",
        "-PaperBaseDir",
        "/tmp/trading_codex_shadow_paper",
        "-Timestamp",
        "2026-04-08T16:10:00-05:00",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "repo_path=/__trading_codex_shadow_ops__" in stdout
    assert "python_path=/__trading_codex_shadow_ops__/.venv/bin/python" in stdout
    assert "scripts/stage2_shadow_daily_ops.py" in stdout
    assert "'--provider' 'stooq'" in stdout
    assert "'--shadow-ops-config' '/tmp/stage2_shadow_ops.json'" in stdout
    assert "'--data-dir' '/tmp/trading_codex_data'" in stdout
    assert "'--archive-root' '/tmp/trading_codex_archive'" in stdout
    assert "'--paper-base-dir' '/tmp/trading_codex_shadow_paper'" in stdout
    assert "'--timestamp' '2026-04-08T16:10:00-05:00'" in stdout


def test_print_only_converts_windows_style_shadow_path_overrides() -> None:
    proc = _run_wrapper(
        "-WslRepoPath",
        "/__trading_codex_shadow_ops__",
        "-WslPython",
        "/__trading_codex_shadow_ops__/.venv/bin/python",
        "-ShadowOpsConfig",
        "C:\\stage2\\shadow_ops.json",
        "-DataDir",
        "C:\\stage2\\data",
        "-ArchiveRoot",
        "C:\\stage2\\archive",
        "-PaperBaseDir",
        "C:\\stage2\\shadow_paper",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "'--shadow-ops-config' '/mnt/c/stage2/shadow_ops.json'" in stdout
    assert "'--data-dir' '/mnt/c/stage2/data'" in stdout
    assert "'--archive-root' '/mnt/c/stage2/archive'" in stdout
    assert "'--paper-base-dir' '/mnt/c/stage2/shadow_paper'" in stdout
    assert "C:\\stage2\\shadow_ops.json" not in stdout
    assert "C:\\stage2\\data" not in stdout
    assert "C:\\stage2\\archive" not in stdout
    assert "C:\\stage2\\shadow_paper" not in stdout
