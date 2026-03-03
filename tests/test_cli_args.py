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


def test_vol_leverage_flags_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--min-leverage",
            "0.2",
            "--max-leverage",
            "1.5",
            "--vol-lookback",
            "63",
        ],
    )

    args = parse_args()
    assert args.min_leverage == 0.2
    assert args.max_leverage == 1.5
    assert args.vol_lookback == 63


def test_legacy_vol_min_max_flags_still_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--vol-min",
            "0.1",
            "--vol-max",
            "0.9",
        ],
    )

    args = parse_args()
    assert args.min_leverage == 0.1
    assert args.max_leverage == 0.9


def test_risk_parity_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "risk_parity_erc",
            "--symbols",
            "SPY",
            "TLT",
            "GLD",
            "--rp-lookback",
            "80",
            "--rp-rebalance",
            "W",
            "--rp-max-iter",
            "300",
            "--rp-tol",
            "1e-7",
        ],
    )

    args = parse_args()
    assert args.strategy == "risk_parity_erc"
    assert args.symbols == ["SPY", "TLT", "GLD"]
    assert args.rp_lookback == 80
    assert args.rp_rebalance == "W"
    assert args.rp_max_iter == 300
    assert args.rp_tol == 1e-7


def test_tsmom_v1_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "tsmom_v1",
            "--symbols",
            "SPY",
            "QQQ",
            "--defensive",
            "TLT",
            "--ts-lookback",
            "180",
            "--ts-rebalance",
            "W",
        ],
    )

    args = parse_args()
    assert args.strategy == "tsmom_v1"
    assert args.symbols == ["SPY", "QQQ"]
    assert args.defensive == "TLT"
    assert args.ts_lookback == 180
    assert args.ts_rebalance == "W"

def test_xsmom_v1_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "xsmom_v1",
            "--symbols",
            "SPY",
            "QQQ",
            "--defensive",
            "TLT",
            "--xs-lookback",
            "200",
            "--xs-top-n",
            "1",
            "--xs-rebalance",
            "W",
        ],
    )

    args = parse_args()
    assert args.strategy == "xsmom_v1"
    assert args.symbols == ["SPY", "QQQ"]
    assert args.defensive == "TLT"
    assert args.xs_lookback == 200
    assert args.xs_top_n == 1
    assert args.xs_rebalance == "W"
