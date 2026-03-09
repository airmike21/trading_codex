from __future__ import annotations

from pathlib import Path

import pytest

from trading_codex.execution import secrets


def test_load_tastytrade_secrets_reads_account_username_and_password(tmp_path: Path) -> None:
    secrets_path = tmp_path / "tastytrade.env"
    secrets_path.write_text(
        "\n".join(
            [
                "export TASTYTRADE_ACCOUNT='5WZ59227'",
                "export TASTYTRADE_USERNAME='user@example.com'",
                "export TASTYTRADE_PASSWORD='pa$$:word'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env: dict[str, str] = {}
    loaded_path = secrets.load_tastytrade_secrets(secrets_file=secrets_path, environ=env)

    assert loaded_path == secrets_path
    assert env == {
        "TASTYTRADE_ACCOUNT": "5WZ59227",
        "TASTYTRADE_USERNAME": "user@example.com",
        "TASTYTRADE_PASSWORD": "pa$$:word",
    }


def test_shell_env_overrides_file_values(tmp_path: Path) -> None:
    secrets_path = tmp_path / "tastytrade.env"
    secrets_path.write_text(
        "\n".join(
            [
                "export TASTYTRADE_ACCOUNT='file-account'",
                "export TASTYTRADE_USERNAME='file-user@example.com'",
                "export TASTYTRADE_PASSWORD='file-password'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = {
        "TASTYTRADE_USERNAME": "shell-user@example.com",
        "TASTYTRADE_PASSWORD": "shell-password",
    }
    secrets.load_tastytrade_secrets(secrets_file=secrets_path, environ=env)

    assert env["TASTYTRADE_ACCOUNT"] == "file-account"
    assert env["TASTYTRADE_USERNAME"] == "shell-user@example.com"
    assert env["TASTYTRADE_PASSWORD"] == "shell-password"


def test_missing_default_secrets_file_is_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(secrets, "DEFAULT_TASTYTRADE_SECRETS_PATH", tmp_path / "missing.env")
    env: dict[str, str] = {}

    loaded_path = secrets.load_tastytrade_secrets(environ=env)

    assert loaded_path is None
    assert env == {}


def test_secret_values_do_not_appear_in_error_messages(tmp_path: Path) -> None:
    secrets_path = tmp_path / "tastytrade.env"
    secrets_path.write_text(
        "\n".join(
            [
                "export TASTYTRADE_PASSWORD='super-secret-password'",
                "not valid shell env",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as excinfo:
        secrets.parse_tastytrade_secrets_file(secrets_path)

    message = str(excinfo.value)
    assert "super-secret-password" not in message
    assert "Invalid env assignment" in message
