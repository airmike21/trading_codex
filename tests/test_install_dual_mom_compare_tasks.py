from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


POWERSHELL_EXE = shutil.which("powershell.exe")


def _run_print_only(*extra_args: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL_EXE is None:
        pytest.skip("powershell.exe is required for Windows task installer tests")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "scripts" / "windows" / "install_dual_mom_compare_tasks.ps1"),
        "-PrintOnly",
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def test_hidden_mode_prints_hidden_powershell_launcher() -> None:
    proc = _run_print_only(
        "-InstallMode",
        "Hidden",
        "-WslDistro",
        "Ubuntu",
        "-WslRepoPath",
        "/tmp/trading_codex_sched_bg",
        "-WslPython",
        "/tmp/trading_codex_sched_bg/.venv/bin/python",
        "-BaseDir",
        "/tmp/trading_codex_runs",
        "-PresetsFile",
        "/tmp/trading_codex_runs/presets.json",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "# mode=Hidden" in stdout
    assert "powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden" in stdout
    assert "trading_codex_scheduled_dual_compare.ps1" in stdout
    assert "-Window morning_0825" in stdout
    assert "-Window afternoon_1535" in stdout
    assert "-BaseDir /tmp/trading_codex_runs" in stdout
    assert "-PresetsFile /tmp/trading_codex_runs/presets.json" in stdout


def test_background_mode_prints_s4u_wsl_action() -> None:
    proc = _run_print_only(
        "-InstallMode",
        "Background",
        "-WslDistro",
        "Ubuntu",
        "-WslRepoPath",
        "/__trading_codex_print_only_preview_should_not_need_repo__",
        "-WslPython",
        "/__trading_codex_print_only_preview_should_not_need_repo__/.venv/bin/python",
        "-BaseDir",
        "/tmp/trading_codex_runs",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "# mode=Background" in stdout
    assert "S4U non-interactive; local resources only" in stdout
    assert "# action=wsl.exe -d Ubuntu -- bash /__trading_codex_print_only_preview_should_not_need_repo__/scripts/windows/trading_codex_scheduled_dual_compare.sh" in stdout
    assert "--window morning_0825" in stdout
    assert "--window afternoon_1535" in stdout
    assert "schtasks.exe /Create /TN \"TradingCodex\\morning_0825_dual_compare\" /XML" in stdout
