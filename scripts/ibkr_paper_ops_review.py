#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from trading_codex.execution.ibkr_paper_lane import DEFAULT_IBKR_PAPER_STATE_KEY
from trading_codex.execution.ibkr_paper_ops_review import (
    DEFAULT_REVIEW_LIMIT,
    build_ibkr_paper_ops_review,
    render_ibkr_paper_ops_review_text,
)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--limit must be >= 1.")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Review the retained Stage 2 IBKR PaperTrader forward-evidence lane without mutating broker "
            "state, lane state, or archive artifacts."
        )
    )
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format.")
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Defaults to ~/.trading_codex, then ~/.cache/trading_codex, then /tmp/trading_codex.",
    )
    parser.add_argument(
        "--state-key",
        type=str,
        default=DEFAULT_IBKR_PAPER_STATE_KEY,
        help="IBKR paper ops state key.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_REVIEW_LIMIT,
        help=f"Inspect at most this many most-recent retained runs (default: {DEFAULT_REVIEW_LIMIT}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        review = build_ibkr_paper_ops_review(
            archive_root=args.archive_root,
            state_key=args.state_key,
            limit=args.limit,
        )
    except Exception as exc:
        print(f"[ibkr_paper_ops_review] ERROR: {exc}", file=sys.stderr)
        return 2

    if args.emit == "json":
        print(json.dumps(review, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        print(render_ibkr_paper_ops_review_text(review))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
