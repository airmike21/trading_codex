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

    assert excinfo.value.code != 0
    out = capsys.readouterr()
    msg = out.err + out.out
    assert (
        "not allowed with argument" in msg
        or "mutually exclusive" in msg
    )
    assert "--next-action" in msg
    assert "--next-action-json" in msg
