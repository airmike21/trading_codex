#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from scripts import daily_signal
except ImportError:  # pragma: no cover - direct script execution path
    import daily_signal  # type: ignore[no-redef]

from trading_codex.execution import (
    FileBrokerPositionAdapter,
    RequestsTastytradeHttpClient,
    TastytradeBrokerExecutionAdapter,
    build_artifact_paths,
    build_live_submission_artifact_path,
    build_live_submission_ledger_path,
    build_live_submission_preview,
    build_live_submission_refusal_from_plan,
    build_manual_order_checklist_path,
    build_simulated_submission_artifact_path,
    build_manual_ticket_csv_path,
    build_execution_plan,
    build_order_intent_artifact_path,
    build_order_intent_export,
    build_simulated_submission_export,
    parse_signal_payload,
    render_markdown,
    resolve_timestamp,
    plan_sha256_for_preview,
    write_artifacts,
    write_live_submission_artifact,
    write_manual_order_checklist,
    write_manual_ticket_csv,
    write_order_intent_artifact,
    write_simulated_submission_artifact,
)
from trading_codex.execution.secrets import DEFAULT_TASTYTRADE_SECRETS_PATH, load_tastytrade_secrets


def _repo_root() -> Path:
    return REPO_ROOT


def _env_with_src(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return env


def _extract_flag_value(args: list[str], flag: str) -> str | None:
    for index, item in enumerate(args):
        if item == flag and index + 1 < len(args):
            return args[index + 1]
    return None


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


def _data_dir_for_preset(*, repo_root: Path, preset: daily_signal.Preset) -> Path | None:
    expanded = daily_signal._expand_known_path_args(preset.run_backtest_args)
    data_dir = _extract_flag_value(expanded, "--data-dir")
    if data_dir:
        return Path(data_dir)
    candidate = repo_root / "data"
    return candidate if candidate.exists() else None


def _load_signal_from_preset(
    *,
    repo_root: Path,
    preset_name: str,
    presets_path: Path | None,
) -> tuple[dict[str, Any], daily_signal.Preset, Path]:
    resolved_presets_path = presets_path or daily_signal._default_presets_path(repo_root)
    presets = daily_signal._load_presets_json(resolved_presets_path)
    if preset_name not in presets:
        known = ", ".join(sorted(presets))
        raise ValueError(f"Unknown preset {preset_name!r}. Known: {known}")

    preset = presets[preset_name]
    expanded_args = daily_signal._expand_known_path_args(preset.run_backtest_args)
    cmd = [sys.executable, str(repo_root / "scripts" / "run_backtest.py"), *expanded_args, "--next-action-json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root), env=_env_with_src(repo_root))
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"run_backtest failed for preset {preset_name!r} ({proc.returncode}): {detail}")

    lines = proc.stdout.splitlines()
    if len(lines) != 1:
        raise RuntimeError(f"run_backtest --next-action-json must emit exactly one line. Got: {len(lines)}")
    payload = json.loads(lines[0])
    if not isinstance(payload, dict):
        raise RuntimeError("run_backtest --next-action-json did not return a JSON object.")
    return payload, preset, resolved_presets_path


def _load_signal_from_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Signal JSON file must contain a JSON object.")
    return payload


def _parse_allowed_symbols_csv(value: str) -> set[str]:
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    if not symbols:
        raise ValueError("--allowed-symbols must contain at least one symbol.")
    return symbols


def _derive_allowed_symbols_from_preset(preset: daily_signal.Preset) -> set[str]:
    expanded = daily_signal._expand_known_path_args(preset.run_backtest_args)
    symbols = {
        item.strip().upper()
        for item in (
            _extract_option_values(expanded, "--symbols")
            + _extract_option_values(expanded, "--defensive")
            + _extract_option_values(expanded, "--vm-defensive-symbol")
        )
        if item.strip()
    }
    return symbols


def _resolve_allowed_symbols(*, raw_value: str | None, preset: daily_signal.Preset | None) -> set[str]:
    if raw_value:
        return _parse_allowed_symbols_csv(raw_value)
    if preset is None:
        raise ValueError(
            "--allowed-symbols is required with --broker tastytrade unless it can be derived from --preset."
        )
    derived = _derive_allowed_symbols_from_preset(preset)
    if not derived:
        raise ValueError(
            f"Could not derive an allowed symbol universe from preset {preset.name!r}; pass --allowed-symbols explicitly."
        )
    return derived


def _should_resolve_managed_symbols(*, args: argparse.Namespace, preset: daily_signal.Preset | None) -> bool:
    if args.allowed_symbols:
        return True
    if args.broker == "tastytrade":
        return True
    if args.account_scope != "full_account" or args.ack_unmanaged_holdings:
        return True
    return preset is not None


def _blocked_summary(plan: Any) -> str:
    parts: list[str] = []
    blocker_set = set(plan.blockers)
    if "capital_sizing_yields_zero_shares" in blocker_set:
        parts.append("capital sizing yields zero affordable shares")
    if "capital_sizing_missing_reference_price" in blocker_set:
        parts.append("capital sizing missing reference price")
    if "managed_unsupported_positions_present" in blocker_set and plan.managed_unsupported_positions:
        joined = ", ".join(position.symbol for position in plan.managed_unsupported_positions)
        parts.append(f"managed unsupported positions: {joined}")
    if "unmanaged_positions_present" in blocker_set and plan.unmanaged_positions:
        joined = ", ".join(position.symbol for position in plan.unmanaged_positions)
        if "ack_unmanaged_holdings_required" in blocker_set:
            parts.append(f"unmanaged positions require --ack-unmanaged-holdings: {joined}")
        else:
            parts.append(f"unmanaged positions: {joined}")
    if "buy_notional_exceeds_buying_power" in blocker_set:
        parts.append("buy notional exceeds buying power")
    if not parts:
        parts = list(plan.blockers)
    return "; ".join(parts)


def _load_local_tastytrade_secrets(secrets_file: Path | None) -> Path | None:
    return load_tastytrade_secrets(secrets_file=secrets_file)


def _export_refusal_prefix(args: argparse.Namespace) -> str:
    requested: list[str] = []
    if args.export_order_intents or args.export_manual_ticket_csv or args.export_simulated_orders:
        requested.append("ORDER INTENT")
    if args.export_manual_ticket_csv:
        requested.append("MANUAL TICKET CSV")
    if args.export_simulated_orders:
        requested.append("SIMULATED ORDER")
    if not requested:
        return "REFUSED ORDER INTENT EXPORT"
    return "REFUSED " + " / ".join(requested) + " EXPORT"


def _resolve_sizing_args(args: argparse.Namespace) -> tuple[str, float | None]:
    if args.sleeve_capital is not None:
        return "sleeve_capital", float(args.sleeve_capital)
    if args.account_capital is not None:
        return "account_capital", float(args.account_capital)
    return "signal_target_shares", None


def _live_submit_summary(export: Any) -> str:
    if export.refusal_reasons:
        return "; ".join(export.refusal_reasons)
    failed_orders = [order for order in export.orders if not order.succeeded]
    if failed_orders:
        return "; ".join(
            f"{order.symbol} {order.side} {order.quantity}: {order.error or 'submission failed'}"
            for order in failed_orders
        )
    return f"submitted {len(export.orders)} live orders"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a dry-run execution plan only. No live orders, broker writes, or auto-trading."
    )
    signal_group = parser.add_mutually_exclusive_group(required=True)
    signal_group.add_argument("--preset", help="Load the latest signal by running run_backtest for this preset.")
    signal_group.add_argument("--signal-json-file", type=Path, help="Plan from a precomputed next_action JSON file.")
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path when using --preset. Default: configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument(
        "--broker",
        choices=["file", "tastytrade"],
        default="file",
        help="Broker snapshot source. 'tastytrade' is read-only and still dry-run only.",
    )
    parser.add_argument("--positions-file", type=Path, default=None, help="Mock/file broker positions JSON.")
    parser.add_argument(
        "--account-id",
        type=str,
        default=None,
        help="Broker account id. Required with --broker tastytrade unless TASTYTRADE_ACCOUNT is available via env or secrets file.",
    )
    parser.add_argument(
        "--allowed-symbols",
        type=str,
        default=None,
        help="Comma-separated managed symbol universe. Required for scoped real broker reads unless derivable from --preset.",
    )
    parser.add_argument(
        "--account-scope",
        choices=["full_account", "managed_sleeve"],
        default="full_account",
        help="Planning scope. full_account remains fail-closed; managed_sleeve computes math on managed holdings only.",
    )
    parser.add_argument(
        "--ack-unmanaged-holdings",
        action="store_true",
        help="Required to proceed in managed_sleeve mode when unmanaged holdings are present. Dry-run only.",
    )
    parser.add_argument(
        "--tastytrade-challenge-code",
        type=str,
        default=None,
        help="Optional device-challenge code for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_CODE.",
    )
    parser.add_argument(
        "--tastytrade-challenge-token",
        type=str,
        default=None,
        help="Optional device-challenge token override for tastytrade auth. Env fallback: TASTYTRADE_CHALLENGE_TOKEN.",
    )
    parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help=f"Optional tastytrade secrets env file. If omitted, auto-loads {DEFAULT_TASTYTRADE_SECRETS_PATH} when present.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.home() / ".trading_codex" / "execution_plans",
        help="Durable dry-run execution plan artifact directory.",
    )
    capital_group = parser.add_mutually_exclusive_group()
    capital_group.add_argument(
        "--sleeve-capital",
        type=float,
        default=None,
        help="Optional sleeve capital for account-sized target sizing. Uses whole shares and rounds down conservatively.",
    )
    capital_group.add_argument(
        "--account-capital",
        type=float,
        default=None,
        help="Optional account capital for account-sized target sizing. Uses whole shares and rounds down conservatively.",
    )
    parser.add_argument(
        "--reserve-cash-pct",
        type=float,
        default=0.0,
        help="Optional reserve cash percent applied before capital sizing (default: 0.0).",
    )
    parser.add_argument(
        "--max-allocation-pct",
        type=float,
        default=1.0,
        help="Optional cap on allocation percent when capital sizing is enabled (default: 1.0).",
    )
    parser.add_argument(
        "--cap-to-buying-power",
        action="store_true",
        help="For capital-based sizing only, cap configured capital to the current broker buying power when available.",
    )
    parser.add_argument(
        "--export-order-intents",
        action="store_true",
        help="Write a dry-run order-intent export JSON artifact from a clean execution plan. Refused by default when plan blockers exist.",
    )
    parser.add_argument(
        "--export-manual-ticket-csv",
        action="store_true",
        help="Write a manual-entry CSV artifact derived from the clean order-intent export. Implies --export-order-intents.",
    )
    parser.add_argument(
        "--export-simulated-orders",
        action="store_true",
        help="Write a dry-run simulated broker-order request artifact derived from the clean order-intent export. No orders are submitted.",
    )
    parser.add_argument(
        "--live-submit",
        action="store_true",
        help="Attempt real tastytrade submission for clean managed-sleeve ETF orders only. Requires --confirm-live-submit.",
    )
    parser.add_argument(
        "--confirm-live-submit",
        type=str,
        default=None,
        help="Required live-submit confirmation. Must exactly match the tastytrade account id being submitted.",
    )
    parser.add_argument(
        "--live-allowed-account",
        type=str,
        default=None,
        help="Required live-submit account binding. Must exactly match the tastytrade account id being submitted.",
    )
    parser.add_argument(
        "--confirm-plan-sha256",
        type=str,
        default=None,
        help="Required live-submit plan confirmation. Must exactly match the dry-run plan_sha256 being submitted.",
    )
    parser.add_argument(
        "--live-max-order-notional",
        type=float,
        default=None,
        help="Required live-submit-only per-order notional safety cap.",
    )
    parser.add_argument(
        "--live-max-order-qty",
        type=int,
        default=None,
        help="Required live-submit-only per-order quantity safety cap.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format after writing artifacts.")
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    try:
        signal_raw: dict[str, Any]
        source_kind: str
        source_label: str
        source_ref: str | None
        data_dir: Path | None
        preset: daily_signal.Preset | None = None
        if args.confirm_live_submit and not args.live_submit:
            raise ValueError("--confirm-live-submit requires --live-submit.")
        if args.live_allowed_account and not args.live_submit:
            raise ValueError("--live-allowed-account requires --live-submit.")
        if args.confirm_plan_sha256 and not args.live_submit:
            raise ValueError("--confirm-plan-sha256 requires --live-submit.")
        if args.live_max_order_notional is not None and not args.live_submit:
            raise ValueError("--live-max-order-notional requires --live-submit.")
        if args.live_max_order_qty is not None and not args.live_submit:
            raise ValueError("--live-max-order-qty requires --live-submit.")
        if args.live_max_order_notional is not None and args.live_max_order_notional <= 0:
            raise ValueError("--live-max-order-notional must be > 0.")
        if args.live_max_order_qty is not None and args.live_max_order_qty <= 0:
            raise ValueError("--live-max-order-qty must be > 0.")
        if args.account_scope == "full_account" and args.ack_unmanaged_holdings:
            raise ValueError("--ack-unmanaged-holdings can only be used with --account-scope managed_sleeve.")

        if args.preset:
            signal_raw, preset, resolved_presets_path = _load_signal_from_preset(
                repo_root=repo_root,
                preset_name=args.preset,
                presets_path=args.presets_file,
            )
            source_kind = "preset"
            source_label = preset.name
            source_ref = str(resolved_presets_path)
            data_dir = _data_dir_for_preset(repo_root=repo_root, preset=preset)
        else:
            signal_raw = _load_signal_from_file(args.signal_json_file)
            source_kind = "signal_json_file"
            source_label = args.signal_json_file.stem
            source_ref = str(args.signal_json_file)
            data_dir = None

        signal = parse_signal_payload(signal_raw)
        managed_symbols: set[str] | None = None
        if _should_resolve_managed_symbols(args=args, preset=preset):
            managed_symbols = _resolve_allowed_symbols(raw_value=args.allowed_symbols, preset=preset)
        if args.account_scope == "managed_sleeve" and not managed_symbols:
            raise ValueError(
                "--account-scope managed_sleeve requires --allowed-symbols or a --preset that derives the managed symbol universe."
            )
        broker_source_ref: str | None
        if args.broker == "file":
            if args.positions_file is None:
                raise ValueError("--positions-file is required when --broker file.")
            broker_adapter = FileBrokerPositionAdapter(args.positions_file)
            broker_source_ref = str(args.positions_file)
        else:
            _load_local_tastytrade_secrets(args.secrets_file)
            if args.positions_file is not None:
                raise ValueError("--positions-file cannot be used with --broker tastytrade.")
            resolved_account_id = args.account_id.strip() if args.account_id and args.account_id.strip() else os.getenv("TASTYTRADE_ACCOUNT")
            if not resolved_account_id:
                raise ValueError(
                    "--account-id is required when --broker tastytrade unless TASTYTRADE_ACCOUNT is available via env or secrets file."
                )
            broker_adapter = TastytradeBrokerExecutionAdapter(
                account_id=resolved_account_id,
                client=RequestsTastytradeHttpClient(
                    challenge_code=args.tastytrade_challenge_code,
                    challenge_token=args.tastytrade_challenge_token,
                ),
            )
            broker_source_ref = f"tastytrade:{resolved_account_id}"
        broker_snapshot = broker_adapter.load_snapshot()
        timestamp = resolve_timestamp(args.timestamp)
        sizing_mode, capital_input = _resolve_sizing_args(args)
        plan = build_execution_plan(
            signal=signal,
            broker_snapshot=broker_snapshot,
            account_scope=args.account_scope,
            managed_symbols=managed_symbols,
            ack_unmanaged_holdings=args.ack_unmanaged_holdings,
            source_kind=source_kind,
            source_label=source_label,
            source_ref=source_ref,
            broker_source_ref=broker_source_ref,
            data_dir=data_dir,
            generated_at=timestamp,
            sizing_mode=sizing_mode,
            capital_input=capital_input,
            cap_to_buying_power=args.cap_to_buying_power,
            reserve_cash_pct=float(args.reserve_cash_pct),
            max_allocation_pct=float(args.max_allocation_pct),
        )
        plan_preview = build_live_submission_preview(plan)
        plan_sha256 = plan_sha256_for_preview(plan_preview)

        base_dir = Path(daily_signal._expand_user(str(args.base_dir)))
        artifact_paths = build_artifact_paths(base_dir, timestamp=timestamp, source_label=source_label)
        export_order_intents_requested = (
            args.export_order_intents
            or args.export_manual_ticket_csv
            or args.export_simulated_orders
        )
        order_intent_export = None
        simulated_export = None
        extra_artifacts: dict[str, str] | None = None
        if (export_order_intents_requested or args.live_submit) and not plan.blockers:
            order_intent_artifact_path = build_order_intent_artifact_path(artifact_paths)
            manual_order_checklist_path = build_manual_order_checklist_path(artifact_paths)
            manual_ticket_csv_path = build_manual_ticket_csv_path(artifact_paths)
            simulated_submission_path = build_simulated_submission_artifact_path(artifact_paths)
            order_intent_export = build_order_intent_export(plan)
            if export_order_intents_requested or args.live_submit:
                export_artifacts = {
                    "json_path": str(order_intent_artifact_path),
                    "manual_order_checklist_path": str(manual_order_checklist_path),
                }
                if args.export_manual_ticket_csv:
                    export_artifacts["manual_ticket_csv_path"] = str(manual_ticket_csv_path)
                if args.export_simulated_orders or args.live_submit:
                    export_artifacts["simulated_order_requests_path"] = str(simulated_submission_path)
                write_order_intent_artifact(
                    order_intent_export,
                    path=order_intent_artifact_path,
                    artifacts=export_artifacts,
                )
                write_manual_order_checklist(order_intent_export, path=manual_order_checklist_path)
                extra_artifacts = {
                    "order_intents_json_path": str(order_intent_artifact_path),
                    "manual_order_checklist_path": str(manual_order_checklist_path),
                }
            if args.export_manual_ticket_csv:
                write_manual_ticket_csv(order_intent_export, path=manual_ticket_csv_path)
                extra_artifacts["manual_ticket_csv_path"] = str(manual_ticket_csv_path)
            if args.export_simulated_orders or args.live_submit:
                simulated_export = build_simulated_submission_export(order_intent_export)
                write_simulated_submission_artifact(
                    simulated_export,
                    path=simulated_submission_path,
                    artifacts={"json_path": str(simulated_submission_path)},
                )
                extra_artifacts["simulated_order_requests_path"] = str(simulated_submission_path)
        live_submission_export = None
        if args.live_submit:
            live_submission_path = build_live_submission_artifact_path(artifact_paths)
            live_submission_ledger_path = build_live_submission_ledger_path(artifact_paths)
            if plan.blockers:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_refused_for_blocked_plan"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif args.confirm_live_submit is None:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_confirmation"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif args.live_allowed_account is None:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_live_allowed_account"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif args.confirm_plan_sha256 is None:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_confirm_plan_sha256"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif args.live_max_order_notional is None:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_live_max_order_notional"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif args.live_max_order_qty is None:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_live_max_order_qty"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif args.broker != "tastytrade":
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_tastytrade_broker"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif simulated_export is None:
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["live_submit_requires_simulated_orders"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            elif not hasattr(broker_adapter, "submit_live_orders"):
                live_submission_export = build_live_submission_refusal_from_plan(
                    plan=plan,
                    refusal_reasons=["broker_adapter_not_live_submit_capable"],
                    plan_preview=plan_preview,
                    plan_sha256=plan_sha256,
                    live_allowed_account=args.live_allowed_account,
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                )
            else:
                live_submission_export = broker_adapter.submit_live_orders(
                    export=simulated_export,
                    confirm_account_id=args.confirm_live_submit,
                    live_allowed_account=args.live_allowed_account,
                    confirm_plan_sha256=args.confirm_plan_sha256,
                    allowed_symbols=managed_symbols or set(),
                    live_max_order_notional=args.live_max_order_notional,
                    live_max_order_qty=args.live_max_order_qty,
                    ledger_path=live_submission_ledger_path,
                    live_submission_artifact_path=live_submission_path,
                )
            write_live_submission_artifact(
                live_submission_export,
                path=live_submission_path,
                artifacts={"json_path": str(live_submission_path)},
            )
            if extra_artifacts is None:
                extra_artifacts = {}
            extra_artifacts["live_submission_json_path"] = str(live_submission_path)
        json_payload = write_artifacts(plan, artifacts=artifact_paths, extra_artifacts=extra_artifacts)

        if args.emit == "json":
            print(json.dumps(json_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        else:
            print(render_markdown(plan, artifacts=artifact_paths), end="")
        if args.live_submit and live_submission_export is not None:
            if not live_submission_export.live_submit_attempted or not live_submission_export.submission_succeeded:
                print(f"[plan_execution] LIVE SUBMIT REFUSED: {_live_submit_summary(live_submission_export)}", file=sys.stderr)
                return 2
        if export_order_intents_requested and plan.blockers:
            print(f"[plan_execution] {_export_refusal_prefix(args)}: {_blocked_summary(plan)}", file=sys.stderr)
            return 2
        if plan.blockers:
            print(f"[plan_execution] BLOCKED: {_blocked_summary(plan)}", file=sys.stderr)
            return 2
        return 0
    except Exception as exc:
        print(f"[plan_execution] ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
