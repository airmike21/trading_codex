from __future__ import annotations

import shutil
import socketserver
import subprocess
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler
from pathlib import Path

import pytest


POWERSHELL_EXE = shutil.which("powershell.exe")


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _run_launcher(*extra_args: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL_EXE is None:
        pytest.skip("powershell.exe is required for review dashboard launcher tests")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "scripts" / "windows" / "trading_codex_review_dashboard.ps1"),
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def _run_installer(*extra_args: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL_EXE is None:
        pytest.skip("powershell.exe is required for review dashboard launcher tests")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(repo_root / "scripts" / "windows" / "install_review_dashboard_shortcut.ps1"),
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))


def _commit_fake_review_repo(repo_path: Path) -> None:
    (repo_path / "scripts").mkdir(parents=True, exist_ok=True)
    (repo_path / "src" / "trading_codex").mkdir(parents=True, exist_ok=True)
    (repo_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)

    (repo_path / "pyproject.toml").write_text("[project]\nname = 'fake-review'\nversion = '0.0.0'\n", encoding="utf-8")
    (repo_path / "scripts" / "review_dashboard.py").write_text("print('placeholder')\n", encoding="utf-8")
    (repo_path / "src" / "trading_codex" / "review_dashboard_data.py").write_text("VALUE = 1\n", encoding="utf-8")
    python_stub = repo_path / ".venv" / "bin" / "python"
    python_stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    python_stub.chmod(0o755)

    subprocess.run(["git", "init", "-q"], cwd=repo_path, check=True)
    subprocess.run(["git", "config", "user.email", "tests@example.com"], cwd=repo_path, check=True)
    subprocess.run(["git", "config", "user.name", "Tests"], cwd=repo_path, check=True)
    subprocess.run(["git", "add", "."], cwd=repo_path, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "fake review repo"], cwd=repo_path, check=True)


@contextmanager
def _http_server(routes: dict[str, tuple[int, bytes]]):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            status, body = routes.get(self.path, (404, b"missing"))
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    class TestServer(socketserver.TCPServer):
        allow_reuse_address = True

    with TestServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server.server_address[1]
        finally:
            server.shutdown()
            thread.join(timeout=5)


def _powershell_can_reach(url: str) -> bool:
    if POWERSHELL_EXE is None:
        return False

    proc = subprocess.run(
        [
            POWERSHELL_EXE,
            "-NoProfile",
            "-Command",
            f"try {{ (Invoke-WebRequest -UseBasicParsing -Uri '{url}' -TimeoutSec 2).Content | Out-String | Write-Output }} catch {{ exit 9 }}",
        ],
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def test_validate_only_accepts_clean_review_workspace(tmp_path: Path) -> None:
    fake_repo = tmp_path / "review-repo"
    fake_repo.mkdir()
    _commit_fake_review_repo(fake_repo)

    proc = _run_launcher(
        "-ValidateOnly",
        "-WslRepoPath",
        str(fake_repo),
        "-WslPython",
        str(fake_repo / ".venv" / "bin" / "python"),
    )

    assert proc.returncode == 0, proc.stderr
    assert "Review dashboard workspace OK" in proc.stdout


def test_validate_only_refuses_normal_checkout_path() -> None:
    proc = _run_launcher("-ValidateOnly", "-WslRepoPath", "~/trading_codex")

    combined = _normalize_whitespace(proc.stdout + proc.stderr)
    assert proc.returncode != 0
    assert "resolves to ~/trading_codex" in combined
    assert "dedicated clean review workspace" in combined
    assert "~/.codex-workspaces/trading-review" in combined


def test_launcher_reuses_existing_healthy_dashboard(tmp_path: Path) -> None:
    fake_repo = tmp_path / "review-repo"
    fake_repo.mkdir()
    _commit_fake_review_repo(fake_repo)

    with _http_server({"/_stcore/health": (200, b"ok")}) as port:
        url = f"http://127.0.0.1:{port}/_stcore/health"
        if not _powershell_can_reach(url):
            pytest.skip("powershell.exe cannot reach the local WSL test server on localhost")

        proc = _run_launcher(
            "-NoBrowser",
            "-WslRepoPath",
            str(fake_repo),
            "-WslPython",
            str(fake_repo / ".venv" / "bin" / "python"),
            "-Port",
            str(port),
        )

    assert proc.returncode == 0, proc.stderr
    assert "already running" in proc.stdout


def test_launcher_refuses_non_dashboard_process_on_port(tmp_path: Path) -> None:
    fake_repo = tmp_path / "review-repo"
    fake_repo.mkdir()
    _commit_fake_review_repo(fake_repo)

    with _http_server({"/": (200, b"not streamlit")}) as port:
        url = f"http://127.0.0.1:{port}/"
        if not _powershell_can_reach(url):
            pytest.skip("powershell.exe cannot reach the local WSL test server on localhost")

        proc = _run_launcher(
            "-NoBrowser",
            "-WslRepoPath",
            str(fake_repo),
            "-WslPython",
            str(fake_repo / ".venv" / "bin" / "python"),
            "-Port",
            str(port),
        )

    combined = _normalize_whitespace(proc.stdout + proc.stderr)
    assert proc.returncode != 0
    assert "already in use" in combined
    assert "non-dashboard process" in combined


def test_shortcut_installer_prints_hidden_shortcut_plan() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    proc = _run_installer(
        "-PrintOnly",
        "-WslRepoPath",
        str(repo_root),
        "-Port",
        "8501",
    )

    assert proc.returncode == 0, proc.stderr
    stdout = proc.stdout
    assert "# review_dashboard_shortcut" in stdout
    assert "Trading Codex Review Hub.lnk" in stdout
    assert "trading_codex_review_dashboard.ps1" in stdout
    assert "-WindowStyle Hidden" in stdout
    assert "-ShowErrorDialog" in stdout
