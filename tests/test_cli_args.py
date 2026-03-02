import sys

import pytest

from scripts.run_backtest import parse_args


def test_next_action_flags_are_mutually_exclusive(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_backtest.py", "--next-action", "--next-action-json"],
    )

    with pytest.raises(SystemExit) as excinfo:
        parse_args()

    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "not allowed with argument" in err
    assert "--next-action" in err
    assert "--next-action-json" in err
