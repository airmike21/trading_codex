from __future__ import annotations

import os
import re
import shlex
from pathlib import Path
from typing import MutableMapping


DEFAULT_TASTYTRADE_SECRETS_PATH = Path.home() / ".config" / "trading_codex" / "tastytrade.env"
TASTYTRADE_SECRET_KEYS = frozenset(
    {
        "TASTYTRADE_ACCOUNT",
        "TASTYTRADE_USERNAME",
        "TASTYTRADE_PASSWORD",
        "TASTYTRADE_SESSION_TOKEN",
        "TASTYTRADE_ACCESS_TOKEN",
        "TASTYTRADE_API_TOKEN",
        "TASTYTRADE_CHALLENGE_CODE",
        "TASTYTRADE_CHALLENGE_TOKEN",
        "TASTYTRADE_API_BASE_URL",
        "TASTYTRADE_TIMEOUT_SECONDS",
    }
)

_ASSIGNMENT_RE = re.compile(r"^(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)$")


def _parse_assignment(line: str, *, path: Path, line_number: int) -> tuple[str, str] | None:
    stripped = line.strip()
    if stripped == "" or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[7:].lstrip()

    match = _ASSIGNMENT_RE.match(stripped)
    if match is None:
        raise ValueError(f"Invalid env assignment in {path} at line {line_number}.")

    key = match.group("key")
    if key not in TASTYTRADE_SECRET_KEYS:
        raise ValueError(f"Unsupported env key {key!r} in {path} at line {line_number}.")

    raw_value = match.group("value")
    if raw_value == "":
        return key, ""

    lexer = shlex.shlex(raw_value, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    tokens = list(lexer)
    if len(tokens) != 1:
        raise ValueError(f"Invalid env value in {path} at line {line_number}.")
    return key, tokens[0]


def parse_tastytrade_secrets_file(path: Path) -> dict[str, str]:
    resolved_path = Path(os.path.expanduser(str(path)))
    values: dict[str, str] = {}
    for line_number, line in enumerate(resolved_path.read_text(encoding="utf-8").splitlines(), start=1):
        parsed = _parse_assignment(line, path=resolved_path, line_number=line_number)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = value
    return values


def load_tastytrade_secrets(
    *,
    secrets_file: Path | None = None,
    environ: MutableMapping[str, str] | None = None,
) -> Path | None:
    target_env = os.environ if environ is None else environ
    if secrets_file is None:
        candidate = DEFAULT_TASTYTRADE_SECRETS_PATH
        if not candidate.exists():
            return None
    else:
        candidate = Path(os.path.expanduser(str(secrets_file)))
        if not candidate.exists():
            raise FileNotFoundError(f"Tastytrade secrets file not found: {candidate}")

    values = parse_tastytrade_secrets_file(candidate)
    for key, value in values.items():
        target_env.setdefault(key, value)
    return candidate
