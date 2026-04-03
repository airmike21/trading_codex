from __future__ import annotations

import json
from pathlib import Path

from scripts import ibkr_paper_lane_daily_ops_preflight
from tests.test_ibkr_paper_lane import FakeIbkrClient, _documented_account_prep


def _write_presets(path: Path) -> None:
    payload = {
        "presets": {
            "dual_mom_vol10_cash_core": {
                "description": "test preset",
                "run_backtest_args": [
                    "--strategy",
                    "dual_mom_vol10_cash",
                    "--symbols",
                    "SPY",
                    "QQQ",
                    "IWM",
                    "EFA",
                    "--dmv-defensive-symbol",
                    "BIL",
                    "--data-dir",
                    str(path.parent / "data"),
                    "--no-plot",
                ],
            }
        }
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_preflight_main_prints_resolved_preset_and_gateway_account(tmp_path: Path, monkeypatch, capsys) -> None:
    presets_path = tmp_path / "presets.json"
    _write_presets(presets_path)

    client = FakeIbkrClient(
        account_prep=_documented_account_prep(account_id="DUP652353", is_paper=True),
        expected_account_id="DUP652353",
    )
    monkeypatch.setattr(ibkr_paper_lane_daily_ops_preflight, "build_ibkr_paper_client", lambda *, config: client)

    rc = ibkr_paper_lane_daily_ops_preflight.main(
        [
            "--preset",
            "dual_mom_vol10_cash_core",
            "--presets-file",
            str(presets_path),
            "--ibkr-account-id",
            "DUP652353",
            "--ibkr-base-url",
            "https://127.0.0.1:5000/v1/api",
            "--no-ibkr-verify-ssl",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert "Stage 2 IBKR paper daily ops preflight OK" in captured.out
    assert f"Presets file: {presets_path}" in captured.out
    assert "IBKR account: DUP652353" in captured.out
    assert "Gateway selected account: DUP652353" in captured.out
