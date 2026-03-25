#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import daily_signal
except ImportError:  # pragma: no cover - direct script execution path
    import daily_signal  # type: ignore[no-redef]

from trading_codex.execution.secrets import DEFAULT_TASTYTRADE_SANDBOX_SECRETS_PATH
from trading_codex.execution.tastytrade_sandbox import (
    render_tastytrade_sandbox_capability_report,
    run_tastytrade_sandbox_capability,
)
from trading_codex.run_archive import write_run_archive


DEFAULT_TASTYTRADE_SANDBOX_PRESET = "dual_mom_vol10_cash_core"


def _extract_option_values(args: list[str], flag: str) -> list[str]:
    values: list[str] = []
    index = 0
    while index < len(args):
        if args[index] != flag:
            index += 1
            continue
        index += 1
        while index < len(args) and not args[index].startswith("--"):
            value = args[index].strip()
            if value:
                values.append(value)
            index += 1
    return values


def _derive_symbols_from_preset(preset: daily_signal.Preset) -> list[str]:
    expanded = daily_signal._expand_known_path_args(preset.run_backtest_args)
    symbols = [
        *_extract_option_values(expanded, "--symbols"),
        *_extract_option_values(expanded, "--defensive"),
        *_extract_option_values(expanded, "--dm-defensive-symbol"),
        *_extract_option_values(expanded, "--dmv-defensive-symbol"),
        *_extract_option_values(expanded, "--vm-defensive-symbol"),
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if normalized == "" or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _resolve_symbols(
    *,
    preset_name: str | None,
    presets_path: Path | None,
    explicit_symbols: list[str] | None,
) -> tuple[list[str], str | None]:
    if explicit_symbols:
        return [symbol.strip().upper() for symbol in explicit_symbols if symbol.strip()], preset_name

    if preset_name is None:
        raise ValueError("Either --symbols or --preset is required.")

    resolved_presets_path = presets_path or daily_signal._default_presets_path(REPO_ROOT)
    presets = daily_signal._load_presets_json(resolved_presets_path)
    if preset_name not in presets:
        known = ", ".join(sorted(presets))
        raise ValueError(f"Unknown preset {preset_name!r}. Known: {known}")

    symbols = _derive_symbols_from_preset(presets[preset_name])
    if not symbols:
        raise ValueError(
            f"Could not derive sandbox capability symbols from preset {preset_name!r}; pass --symbols explicitly."
        )
    return symbols, preset_name


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bounded tastytrade sandbox capability probe and archive the evidence bundle."
    )
    parser.add_argument(
        "--preset",
        type=str,
        default=DEFAULT_TASTYTRADE_SANDBOX_PRESET,
        help="Preset used to derive the primary ETF universe (default: dual_mom_vol10_cash_core).",
    )
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Defaults to configs/presets.json, then configs/presets.example.json.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Optional explicit symbol list. Overrides preset-derived symbols.",
    )
    parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help=(
            "Optional tastytrade sandbox env file. "
            f"If omitted, auto-loads {DEFAULT_TASTYTRADE_SANDBOX_SECRETS_PATH} when present."
        ),
    )
    parser.add_argument(
        "--account-id",
        type=str,
        default=None,
        help="Optional explicit sandbox account override. If account discovery succeeds, it must match.",
    )
    parser.add_argument("--probe-order-symbol", type=str, default=None, help="Optional explicit preview/submit probe symbol.")
    parser.add_argument(
        "--probe-order-qty",
        type=int,
        default=1,
        help="Whole-share quantity for the synthetic sandbox probe order (default: 1).",
    )
    parser.add_argument(
        "--enable-sandbox-submit",
        action="store_true",
        help="Attempt a real sandbox order submit. Disabled by default.",
    )
    parser.add_argument(
        "--sandbox-submit-account",
        type=str,
        default=None,
        help="Required for sandbox submit. Must exactly match the selected sandbox account.",
    )
    parser.add_argument(
        "--cancel-after-submit",
        action="store_true",
        help="Attempt sandbox cancel after a successful sandbox submit.",
    )
    parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional sandbox challenge code override. Env fallback: TASTYTRADE_SANDBOX_CHALLENGE_CODE.",
    )
    parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional sandbox challenge token override. Env fallback: TASTYTRADE_SANDBOX_CHALLENGE_TOKEN.",
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Default follows the Trading Codex archive-root fallback chain.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["json", "text"], default="text", help="Stdout format.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        symbols, resolved_preset = _resolve_symbols(
            preset_name=args.preset,
            presets_path=args.presets_file,
            explicit_symbols=args.symbols,
        )
        report = run_tastytrade_sandbox_capability(
            symbols=symbols,
            preset_name=resolved_preset,
            secrets_file=args.secrets_file,
            explicit_account_id=args.account_id,
            probe_order_symbol=args.probe_order_symbol,
            probe_order_qty=args.probe_order_qty,
            enable_submit=bool(args.enable_sandbox_submit),
            sandbox_submit_account=args.sandbox_submit_account,
            cancel_after_submit=bool(args.cancel_after_submit),
            timestamp=_parse_timestamp(args.timestamp),
            challenge_code=args.tastytrade_challenge_code,
            challenge_token=args.tastytrade_challenge_token,
        )
    except Exception as exc:
        print(f"[tastytrade_sandbox_capability] ERROR: {exc}", file=sys.stderr)
        return 2

    archived = write_run_archive(
        timestamp=report["generated_at"],
        run_kind="tastytrade_sandbox_capability",
        mode="sandbox",
        label=resolved_preset or "explicit_symbols",
        identity_parts=[
            report["config"].get("base_url"),
            report["capability_matrix"]["account_discovery_selection"]["details"].get("selected_account_id"),
            ",".join(report["symbols"]),
            report["controls"]["probe_order_symbol"],
            report["controls"]["probe_order_qty"],
        ],
        manifest_fields={
            "preset": resolved_preset,
            "symbols": report["symbols"],
            "selected_account_id": report["capability_matrix"]["account_discovery_selection"]["details"].get(
                "selected_account_id"
            ),
            "overall_status": report["summary"]["overall_status"],
            "pre_submit_status": report["summary"]["pre_submit_status"],
            "mutation_status": report["summary"]["mutation_status"],
            "source": {
                "script": "scripts/tastytrade_sandbox_capability.py",
            },
        },
        json_artifacts={"capability_report": report},
        text_artifacts={"capability_summary": render_tastytrade_sandbox_capability_report(report)},
        preferred_root=args.archive_root,
    )

    capability_report_path = archived.paths.run_dir / archived.manifest["artifact_paths"]["capability_report"]
    capability_summary_path = archived.paths.run_dir / archived.manifest["artifact_paths"]["capability_summary"]
    output = dict(report)
    output["archive"] = {
        "root_dir": str(archived.paths.root_dir),
        "run_dir": str(archived.paths.run_dir),
        "manifest_path": str(archived.paths.manifest_path),
        "capability_report_path": str(capability_report_path),
        "capability_summary_path": str(capability_summary_path),
    }

    if args.emit == "json":
        print(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False))
        return 0

    lines = [
        render_tastytrade_sandbox_capability_report(report),
        "",
        f"Manifest: {archived.paths.manifest_path}",
        f"Capability report: {capability_report_path}",
        f"Capability summary: {capability_summary_path}",
    ]
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
