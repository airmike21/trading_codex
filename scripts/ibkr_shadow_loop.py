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
    from scripts import ibkr_shadow_paper
except ImportError:  # pragma: no cover - direct script execution path
    import ibkr_paper_lane  # type: ignore[no-redef]
    import ibkr_shadow_paper  # type: ignore[no-redef]

from trading_codex.execution.ibkr_shadow_loop import (
    apply_ibkr_shadow_loop_change_detection,
    derive_ibkr_shadow_loop_state_key,
    resolve_ibkr_shadow_loop_state_path,
)
from trading_codex.execution.ibkr_shadow_paper import (
    build_ibkr_shadow_client,
    build_ibkr_shadow_report,
    load_ibkr_shadow_config,
)


def build_parser() -> argparse.ArgumentParser:
    parser = ibkr_shadow_paper.build_parser()
    parser.description = "Run the IBKR paper shadow path once and report whether the shadow action changed."
    parser.add_argument(
        "--state-key",
        type=str,
        default=None,
        help="Optional local state key. Defaults to the preset name or signal file stem.",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help="Optional shadow-loop state directory override. Defaults to ~/.trading_codex, then ~/.cache/trading_codex, then /tmp/trading_codex.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="Optional explicit shadow-loop state file path.",
    )
    return parser


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def _print_text(payload: dict[str, Any]) -> None:
    print(
        f"state={payload['run_state']} "
        f"change={payload['change_status']} "
        f"fp={payload['shadow_action_fingerprint_short']} "
        f"summary={payload['decision_summary']}"
    )


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
        payload = ibkr_shadow_paper._archive_report(payload=payload, archive_root=args.archive_root)

        state_key = derive_ibkr_shadow_loop_state_key(
            requested_state_key=args.state_key,
            source_label=source_label,
        )
        state_path = resolve_ibkr_shadow_loop_state_path(
            state_key=state_key,
            base_dir=args.state_dir or args.archive_root,
            state_file=args.state_file,
            create=True,
        )
        payload = apply_ibkr_shadow_loop_change_detection(
            payload=payload,
            state_key=state_key,
            state_path=state_path,
        )

        if args.emit == "json":
            _print_json(payload)
        else:
            _print_text(payload)
        return 0
    except Exception as exc:
        print(f"[ibkr_shadow_loop] ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
