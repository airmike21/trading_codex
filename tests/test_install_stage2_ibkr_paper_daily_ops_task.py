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
        str(repo_root / "scripts" / "windows" / "install_stage2_ibkr_paper_daily_ops_task.ps1"),
        "-PrintOnly",
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def test_print_only_renders_background_scheduler_install() -> None:
    proc = _run_print_only(
        "-StartTime",
        "16:10",
        "-IbkrAccountId",
        "DUP652353",
        "-WslRepoPath",
        "/__trading_codex_ibkr_stage2__",
        "-WslPython",
        "/__trading_codex_ibkr_stage2__/.venv/bin/python",
        "-LogDir",
        "C:\\trading-codex\\logs",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "# mode=Background" in stdout
    assert "# schedule=Mon-Fri 16:10" in stdout
    assert "powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File" in stdout
    assert "trading_codex_stage2_ibkr_paper_daily_ops.ps1" in stdout
    assert "-IbkrAccountId DUP652353" in stdout
    assert "-WslRepoPath /__trading_codex_ibkr_stage2__" in stdout
    assert "-WslPython /__trading_codex_ibkr_stage2__/.venv/bin/python" in stdout
    assert "-LogDir C:\\trading-codex\\logs" in stdout
    assert "schtasks.exe /Create /TN \"TradingCodex\\stage2_ibkr_paper_daily_ops\" /XML" in stdout


def test_print_only_includes_optional_overrides_and_run_now_command() -> None:
    proc = _run_print_only(
        "-RunNow",
        "-PresetsFile",
        "C:\\stage2\\presets.json",
        "-ArchiveRoot",
        "C:\\stage2\\archive",
        "-IbkrBaseDir",
        "C:\\stage2\\ibkr",
        "-VerifyIbkrSsl",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "-PresetsFile C:\\stage2\\presets.json" in stdout
    assert "-ArchiveRoot C:\\stage2\\archive" in stdout
    assert "-IbkrBaseDir C:\\stage2\\ibkr" in stdout
    assert "-VerifyIbkrSsl" in stdout
    assert "schtasks.exe /Run /TN \"TradingCodex\\stage2_ibkr_paper_daily_ops\"" in stdout


def test_print_only_defaults_to_runtime_checkout_path() -> None:
    proc = _run_print_only("-IbkrAccountId", "DUP652353")

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "-WslRepoPath ~/trading_codex" in stdout
    assert "-WslPython ~/trading_codex/.venv/bin/python" in stdout
    assert "schtasks.exe /Create /TN \"TradingCodex\\stage2_ibkr_paper_daily_ops\" /XML" in stdout
