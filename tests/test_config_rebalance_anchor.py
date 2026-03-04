from __future__ import annotations

from scripts.run_backtest import load_run_backtest_config


def test_load_run_backtest_config_reads_rebalance_anchor_date(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text('rebalance_anchor_date = "2021-01-01"\n', encoding="utf-8")

    cfg = load_run_backtest_config(config_path)

    assert cfg.rebalance_anchor_date == "2021-01-01"


def test_load_run_backtest_config_missing_key_defaults_to_none(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text('strategy = "valmom_v1"\n', encoding="utf-8")

    cfg = load_run_backtest_config(config_path)

    assert cfg.rebalance_anchor_date is None
