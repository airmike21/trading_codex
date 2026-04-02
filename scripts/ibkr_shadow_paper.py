#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import ibkr_paper_lane
except ImportError:  # pragma: no cover - direct script execution path
    import ibkr_paper_lane  # type: ignore[no-redef]

from trading_codex.execution.ibkr_shadow_paper import (
    DEFAULT_IBKR_SHADOW_CLIENT_ID,
    DEFAULT_IBKR_SHADOW_HOST,
    DEFAULT_IBKR_SHADOW_PORT,
    build_ibkr_shadow_client,
    build_ibkr_shadow_report,
    load_ibkr_shadow_config,
    render_ibkr_shadow_text,
)
from trading_codex.run_archive import write_run_archive


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Connect read-only to IBKR Paper TWS and print the proposed no-submit shadow action."
    )
    parser.add_argument("--emit", choices=["json", "text"], default="text", help="Stdout format.")
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--archive-root", type=Path, default=None, help="Optional run-archive root override.")
    parser.add_argument(
        "--ibkr-account-id",
        type=str,
        default=None,
        help="Optional paper DU account id. Defaults to IBKR_PAPER_ACCOUNT_ID when present.",
    )
    parser.add_argument(
        "--ibkr-host",
        type=str,
        default=None,
        help=f"IBKR Paper TWS host. Defaults to {DEFAULT_IBKR_SHADOW_HOST}.",
    )
    parser.add_argument(
        "--ibkr-port",
        type=int,
        default=None,
        help=f"IBKR Paper TWS port. Hard-guarded to {DEFAULT_IBKR_SHADOW_PORT}.",
    )
    parser.add_argument(
        "--ibkr-client-id",
        type=int,
        default=None,
        help=f"IBKR TWS client id. Defaults to {DEFAULT_IBKR_SHADOW_CLIENT_ID}.",
    )
    parser.add_argument(
        "--ibkr-connect-timeout-seconds",
        type=float,
        default=None,
        help="IBKR TWS connect timeout. Defaults to IBKR_TWS_CONNECT_TIMEOUT_SECONDS or 10.",
    )
    parser.add_argument(
        "--ibkr-read-timeout-seconds",
        type=float,
        default=None,
        help="IBKR TWS read timeout. Defaults to IBKR_TWS_READ_TIMEOUT_SECONDS or 10.",
    )
    ibkr_paper_lane._add_signal_source_args(parser)
    return parser


def _archive_report(
    *,
    payload: dict[str, Any],
    archive_root: Path | None,
) -> dict[str, Any]:
    summary_text = render_ibkr_shadow_text(payload)
    try:
        archived = write_run_archive(
            timestamp=payload["generated_at_chicago"],
            run_kind="ibkr_paper_shadow_execution",
            mode="no_submit",
            label=payload["source"]["label"],
            identity_parts=[
                payload["signal"].get("event_id"),
                payload["broker_account"].get("account_id"),
                payload["paper_endpoint_used"],
            ],
            manifest_fields={
                "account_id": payload["broker_account"].get("account_id"),
                "strategy": payload["signal"].get("strategy"),
                "symbol": payload["signal"].get("symbol"),
                "action": payload["signal"].get("action"),
                "target_shares": payload["signal"].get("target_shares"),
                "resize_new_shares": payload["signal"].get("resize_new_shares"),
                "next_rebalance": payload["signal"].get("next_rebalance"),
                "event_id": payload["signal"].get("event_id"),
                "endpoint_used": payload.get("endpoint_used"),
                "decision_summary": payload.get("decision_summary"),
                "action_state": payload.get("action_state"),
                "has_drift": payload.get("has_drift"),
                "is_noop": payload.get("is_noop"),
                "proposed_order_count": payload.get("proposed_order_count"),
                "managed_symbol_count": payload.get("managed_symbol_count"),
                "broker_position_symbol_count": payload.get("broker_position_symbol_count"),
                "reconciliation_summary": payload.get("reconciliation_summary"),
                "shadow_action_fingerprint": payload.get("shadow_action_fingerprint"),
                "simulation_only": True,
                "no_submit": True,
                "paper_endpoint_used": payload.get("paper_endpoint_used"),
                "blockers": payload.get("blockers"),
                "warnings": payload.get("warnings"),
                "source": {
                    "script": "scripts/ibkr_shadow_paper.py",
                    "kind": payload["source"]["kind"],
                    "label": payload["source"]["label"],
                    "ref": payload["source"]["ref"],
                },
            },
            json_artifacts={
                "shadow_execution_report": payload,
            },
            text_artifacts={
                "summary_text": summary_text,
            },
            preferred_root=archive_root,
        )
    except Exception as exc:
        payload = dict(payload)
        payload["archive_warning"] = str(exc)
        return payload

    payload = dict(payload)
    payload["archive_manifest_path"] = str(archived.paths.manifest_path)
    return payload


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def main(
    argv: list[str] | None = None,
    *,
    client_factory=build_ibkr_shadow_client,
) -> int:
    args = build_parser().parse_args(argv)

    try:
        signal_raw, source_kind, source_label, source_ref, data_dir, preset = ibkr_paper_lane._resolve_signal_source(
            args=args,
            repo_root=REPO_ROOT,
        )
        allowed_symbols = ibkr_paper_lane._resolve_allowed_symbols(raw_value=args.allowed_symbols, preset=preset)
        config = load_ibkr_shadow_config(
            host=args.ibkr_host,
            port=args.ibkr_port,
            client_id=args.ibkr_client_id,
            account_id=args.ibkr_account_id,
            connect_timeout_seconds=args.ibkr_connect_timeout_seconds,
            read_timeout_seconds=args.ibkr_read_timeout_seconds,
        )
        client = client_factory(config=config)
        payload = build_ibkr_shadow_report(
            client=client,
            config=config,
            allowed_symbols=allowed_symbols,
            signal_raw=signal_raw,
            source_kind=source_kind,
            source_label=source_label,
            source_ref=source_ref,
            data_dir=data_dir,
            timestamp=args.timestamp,
        )
        payload = _archive_report(payload=payload, archive_root=args.archive_root)

        if args.emit == "json":
            _print_json(payload)
        else:
            ibkr_paper_lane._print_text(
                render_ibkr_shadow_text(payload),
                archive_manifest_path=payload.get("archive_manifest_path"),
            )
        return 0
    except Exception as exc:
        print(f"[ibkr_shadow_paper] ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
