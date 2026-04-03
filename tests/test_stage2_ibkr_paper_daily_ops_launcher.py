from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


POWERSHELL_EXE = shutil.which("powershell.exe")


def _run_wrapper(*extra_args: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL_EXE is None:
        pytest.skip("powershell.exe is required for Stage 2 IBKR paper daily ops wrapper tests")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "scripts" / "windows" / "trading_codex_stage2_ibkr_paper_daily_ops.ps1"),
        "-PrintOnly",
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def test_print_only_renders_preflight_and_daily_ops_commands() -> None:
    proc = _run_wrapper(
        "-WslRepoPath",
        "/__trading_codex_ibkr_stage2__",
        "-WslPython",
        "/__trading_codex_ibkr_stage2__/.venv/bin/python",
        "-IbkrAccountId",
        "DUP652353",
        "-LogDir",
        "C:\\trading-codex\\logs",
        "-ArchiveRoot",
        "/tmp/trading_codex_archive",
        "-IbkrBaseDir",
        "/tmp/trading_codex_ibkr",
        "-Timestamp",
        "2026-04-03T16:10:00-05:00",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "repo_path=/__trading_codex_ibkr_stage2__" in stdout
    assert "python_path=/__trading_codex_ibkr_stage2__/.venv/bin/python" in stdout
    assert "presets_file=/__trading_codex_ibkr_stage2__/configs/presets.example.json" in stdout
    assert "ibkr_account_id_source=parameter" in stdout
    assert "preflight_command=cd '/__trading_codex_ibkr_stage2__' && '/__trading_codex_ibkr_stage2__/.venv/bin/python' scripts/ibkr_paper_lane_daily_ops_preflight.py" in stdout
    assert "command=cd '/__trading_codex_ibkr_stage2__' && '/__trading_codex_ibkr_stage2__/.venv/bin/python' scripts/ibkr_paper_lane_daily_ops.py" in stdout
    assert "'--ibkr-account-id' 'DUP652353'" in stdout
    assert "'--ibkr-base-url' 'https://127.0.0.1:5000/v1/api'" in stdout
    assert "'--no-ibkr-verify-ssl'" in stdout
    assert "'--archive-root' '/tmp/trading_codex_archive'" in stdout
    assert "'--ibkr-base-dir' '/tmp/trading_codex_ibkr'" in stdout
    assert "'--timestamp' '2026-04-03T16:10:00-05:00'" in stdout
    assert "log_path=C:\\trading-codex\\logs\\stage2_ibkr_paper_daily_ops-" in stdout


def test_print_only_converts_windows_style_paths_for_explicit_overrides() -> None:
    proc = _run_wrapper(
        "-WslRepoPath",
        "/__trading_codex_ibkr_stage2__",
        "-WslPython",
        "/__trading_codex_ibkr_stage2__/.venv/bin/python",
        "-IbkrAccountId",
        "DUP652353",
        "-PresetsFile",
        "C:\\stage2\\presets.json",
        "-ArchiveRoot",
        "C:\\stage2\\archive",
        "-IbkrBaseDir",
        "C:\\stage2\\ibkr",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "presets_file=/mnt/c/stage2/presets.json" in stdout
    assert "'--presets-file' '/mnt/c/stage2/presets.json'" in stdout
    assert "'--archive-root' '/mnt/c/stage2/archive'" in stdout
    assert "'--ibkr-base-dir' '/mnt/c/stage2/ibkr'" in stdout
    assert "C:\\stage2\\presets.json" not in stdout
    assert "C:\\stage2\\archive" not in stdout
    assert "C:\\stage2\\ibkr" not in stdout


def test_print_only_defaults_to_runtime_checkout_path() -> None:
    proc = _run_wrapper("-IbkrAccountId", "DUP652353")

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "repo_path=~/trading_codex" in stdout
    assert "python_path=~/trading_codex/.venv/bin/python" in stdout
    assert "presets_file=/home/aarondaugherty/trading_codex/configs/presets.example.json" in stdout
    assert "preflight_command=cd ~/trading_codex && ~/trading_codex/.venv/bin/python scripts/ibkr_paper_lane_daily_ops_preflight.py" in stdout
    assert "command=cd ~/trading_codex && ~/trading_codex/.venv/bin/python scripts/ibkr_paper_lane_daily_ops.py" in stdout
