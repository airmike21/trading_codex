#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import paper_lane_daily_ops
except ImportError:  # pragma: no cover - direct script execution path
    import paper_lane_daily_ops  # type: ignore[no-redef]

from trading_codex.execution.ibkr_paper_lane import (
    build_ibkr_paper_client,
    build_ibkr_paper_preflight,
    load_ibkr_paper_client_config,
)


def _repo_root() -> Path:
    return REPO_ROOT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the narrow Stage 2 IBKR PaperTrader daily ops prerequisites: "
            "the expected preset source, the configured paper account id, and a reachable "
            "Client Portal Gateway session that is explicitly verified as paper."
        )
    )
    parser.add_argument("--preset", default=paper_lane_daily_ops.DEFAULT_PRESET, help="IBKR paper preset name.")
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Expected presets path. Defaults to configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format.")
    parser.add_argument(
        "--ibkr-account-id",
        type=str,
        default=None,
        help="IBKR PaperTrader account id. Defaults to IBKR_PAPER_ACCOUNT_ID.",
    )
    parser.add_argument(
        "--ibkr-base-url",
        type=str,
        default=None,
        help="Optional IBKR Web API base URL override.",
    )
    parser.add_argument(
        "--ibkr-timeout-seconds",
        type=float,
        default=None,
        help="Optional IBKR Web API timeout override.",
    )
    parser.add_argument(
        "--ibkr-verify-ssl",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Optional IBKR Web API TLS verification override.",
    )
    return parser


def _build_payload(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = _repo_root()
    resolved_presets_path, preset = paper_lane_daily_ops._resolve_preset(
        repo_root=repo_root,
        preset_name=args.preset,
        presets_path=args.presets_file,
    )
    config = load_ibkr_paper_client_config(
        account_id=args.ibkr_account_id,
        base_url=args.ibkr_base_url,
        verify_ssl=args.ibkr_verify_ssl,
        timeout_seconds=args.ibkr_timeout_seconds,
    )
    client = build_ibkr_paper_client(config=config)
    payload = build_ibkr_paper_preflight(client=client, config=config)
    payload["preset"] = preset.name
    payload["presets_file"] = str(resolved_presets_path)
    return payload


def _render_text(payload: dict[str, Any]) -> str:
    brokerage_accounts = payload.get("broker_account_prep", {}).get("brokerage_accounts", {})
    selected_account = ""
    if isinstance(brokerage_accounts, dict):
        selected_account = str(brokerage_accounts.get("selectedAccount", "")).strip()

    lines = [
        "Stage 2 IBKR paper daily ops preflight OK",
        f"Preset: {payload['preset']}",
        f"Presets file: {payload['presets_file']}",
        f"IBKR account: {payload['account_id']}",
        f"IBKR base URL: {payload['base_url']}",
        f"IBKR verify SSL: {payload['verify_ssl']}",
        f"IBKR timeout seconds: {payload['timeout_seconds']}",
    ]
    if selected_account:
        lines.append(f"Gateway selected account: {selected_account}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = _build_payload(args)
    except Exception as exc:
        print(f"[ibkr_paper_lane_daily_ops_preflight] ERROR: {exc}", file=sys.stderr)
        return 2

    if args.emit == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
