#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch the Trading Codex review dashboard as a detached Streamlit process."
    )
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--python-path", required=True)
    parser.add_argument("--dashboard-script", required=True)
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--port", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_directory = os.path.dirname(args.log_path)
    if log_directory:
        os.makedirs(log_directory, exist_ok=True)

    with open(args.log_path, "ab", buffering=0) as log_handle:
        process = subprocess.Popen(
            [
                args.python_path,
                "-m",
                "streamlit",
                "run",
                args.dashboard_script,
                "--server.address",
                "127.0.0.1",
                "--server.port",
                args.port,
                "--server.headless",
                "true",
                "--browser.gatherUsageStats",
                "false",
            ],
            cwd=args.repo_path,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    sys.stdout.write(str(process.pid))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
