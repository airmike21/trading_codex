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


def test_vol_target_flag_defaults_to_10pct_when_enabled_without_value(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--vol-target",
            "--vol-lookback",
            "63",
        ],
    )

    args = parse_args()
    assert args.vol_target == 0.10
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


def test_cost_model_flags_parse_and_keep_legacy_commission_bps(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--slippage-bps",
            "7.5",
            "--commission-per-trade",
            "1.25",
            "--commission-bps",
            "0.4",
        ],
    )

    args = parse_args()
    assert args.slippage_bps == 7.5
    assert args.commission_per_trade == 1.25
    assert args.commission_bps == 0.4


def test_shadow_artifacts_dir_flag_parses(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--shadow-artifacts-dir",
            "/tmp/shadow-review",
        ],
    )

    args = parse_args()
    assert args.shadow_artifacts_dir == "/tmp/shadow-review"


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


def test_dual_mom_v1_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "dual_mom_v1",
            "--symbols",
            "SPY",
            "QQQ",
            "--dm-lookback",
            "180",
            "--dm-top-n",
            "2",
            "--dm-rebalance",
            "15",
            "--dm-defensive-symbol",
            "SHY",
        ],
    )

    args = parse_args()
    assert args.strategy == "dual_mom_v1"
    assert args.symbols == ["SPY", "QQQ"]
    assert args.dm_lookback == 180
    assert args.dm_top_n == 2
    assert args.dm_rebalance == 15
    assert args.dm_defensive_symbol == "SHY"


def test_dual_mom_vol10_cash_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "dual_mom_vol10_cash",
            "--symbols",
            "SPY",
            "QQQ",
            "IWM",
            "EFA",
            "--dmv-mom-lookback",
            "84",
            "--dmv-rebalance",
            "15",
            "--dmv-defensive-symbol",
            "BIL",
            "--dmv-vol-lookback",
            "30",
            "--dmv-target-vol",
            "0.12",
        ],
    )

    args = parse_args()
    assert args.strategy == "dual_mom_vol10_cash"
    assert args.symbols == ["SPY", "QQQ", "IWM", "EFA"]
    assert args.dmv_mom_lookback == 84
    assert args.dmv_rebalance == 15
    assert args.dmv_defensive_symbol == "BIL"
    assert args.dmv_vol_lookback == 30
    assert args.dmv_target_vol == 0.12


def test_dual_mom_vol10_cash_rejects_generic_vol_target(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "dual_mom_vol10_cash",
            "--vol-target",
            "0.12",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        parse_args()

    assert excinfo.value.code != 0
    msg = capsys.readouterr().err
    assert "dual_mom_vol10_cash" in msg
    assert "--vol-target" in msg


def test_dual_mom_vol10_cash_rejects_ivol(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "dual_mom_vol10_cash",
            "--ivol",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        parse_args()

    assert excinfo.value.code != 0
    msg = capsys.readouterr().err
    assert "dual_mom_vol10_cash" in msg
    assert "--ivol" in msg


def test_valmom_v1_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "valmom_v1",
            "--symbols",
            "SPY",
            "QQQ",
            "--vm-mom-lookback",
            "180",
            "--vm-val-lookback",
            "900",
            "--vm-top-n",
            "2",
            "--vm-rebalance",
            "15",
            "--vm-defensive-symbol",
            "SHY",
            "--vm-mom-weight",
            "1.25",
            "--vm-val-weight",
            "0.75",
        ],
    )

    args = parse_args()
    assert args.strategy == "valmom_v1"
    assert args.symbols == ["SPY", "QQQ"]
    assert args.vm_mom_lookback == 180
    assert args.vm_val_lookback == 900
    assert args.vm_top_n == 2
    assert args.vm_rebalance == 15
    assert args.vm_defensive_symbol == "SHY"
    assert args.vm_mom_weight == 1.25
    assert args.vm_val_weight == 0.75


def test_ivol_cli_args_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "valmom_v1",
            "--symbols",
            "SPY",
            "QQQ",
            "--ivol",
            "--ivol-lookback",
            "84",
            "--ivol-eps",
            "1e-6",
        ],
    )

    args = parse_args()
    assert args.strategy == "valmom_v1"
    assert args.ivol is True
    assert args.ivol_lookback == 84
    assert args.ivol_eps == 1e-6


def test_rebalance_anchor_date_cli_arg_parse(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_backtest.py",
            "--strategy",
            "valmom_v1",
            "--symbols",
            "SPY",
            "QQQ",
            "--rebalance-anchor-date",
            "2021-01-01",
        ],
    )

    args = parse_args()
    assert args.rebalance_anchor_date == "2021-01-01"
