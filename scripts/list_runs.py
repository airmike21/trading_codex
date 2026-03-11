#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from trading_codex.run_archive import recent_runs, resolve_archive_root, resolve_manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List recent archived Trading Codex runs.")
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Default prefers ~/.trading_codex, then ~/.cache/trading_codex, then /tmp/trading_codex.",
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of recent runs to show (default: 10).")
    parser.add_argument(
        "--latest-manifest-path",
        action="store_true",
        help="Print only the latest archived manifest path.",
    )
    return parser


def _display_value(record: dict[str, object], key: str, default: str = "-") -> str:
    value = record.get(key)
    if value is None or value == "":
        return default
    return str(value)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runs = recent_runs(limit=args.limit, root_dir=args.archive_root)

    if args.latest_manifest_path:
        if not runs:
            return 0
        print(resolve_manifest_path(runs[0], root_dir=args.archive_root))
        return 0

    if not runs:
        root_dir = resolve_archive_root(preferred_root=args.archive_root, create=False)
        print(f"No archived runs found under {root_dir}")
        return 0

    for record in runs:
        manifest_path = resolve_manifest_path(record, root_dir=args.archive_root)
        print(
            " | ".join(
                [
                    _display_value(record, "timestamp"),
                    _display_value(record, "run_kind"),
                    f"mode={_display_value(record, 'mode')}",
                    f"strategy={_display_value(record, 'strategy')}",
                    f"action={_display_value(record, 'action')}",
                    f"symbol={_display_value(record, 'symbol')}",
                    f"run_id={_display_value(record, 'run_id')}",
                    f"manifest={manifest_path}",
                ]
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
