#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape as xml_escape

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from trading_codex.execution.paper_lane import DEFAULT_PAPER_STATE_KEY
from trading_codex.run_archive import resolve_archive_root, write_run_archive

try:
    from scripts import daily_signal, update_data_eod
except ImportError:  # pragma: no cover - direct script execution path
    import daily_signal  # type: ignore[no-redef]
    import update_data_eod  # type: ignore[no-redef]

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


DEFAULT_PRESET = "dual_mom_vol10_cash_core"
DEFAULT_PROVIDER = "stooq"
STEP_SCHEMA_NAME = "paper_lane_daily_ops_step"
STEP_SCHEMA_VERSION = 1
RUN_SCHEMA_NAME = "paper_lane_daily_ops_run"
RUN_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_NAME = "paper_lane_daily_ops_log_entry"
SUMMARY_SCHEMA_VERSION = 1
RUN_LOG_COLUMNS = (
    "schema_name",
    "schema_version",
    "run_id",
    "timestamp_chicago",
    "ops_date",
    "overall_result",
    "failed_step",
    "preset",
    "state_key",
    "provider",
    "presets_file",
    "data_dir",
    "paper_base_dir",
    "update_exit_code",
    "update_updated_symbols",
    "status_exit_code",
    "status_signal_date",
    "status_signal_action",
    "status_signal_symbol",
    "status_target_shares",
    "status_next_rebalance",
    "status_event_id",
    "status_drift_present",
    "status_event_already_applied",
    "status_archive_manifest_path",
    "apply_exit_code",
    "apply_result",
    "apply_duplicate_event_blocked",
    "apply_event_receipt_path",
    "apply_archive_manifest_path",
    "paper_state_path",
    "paper_ledger_path",
    "daily_ops_manifest_path",
    "daily_ops_jsonl_path",
    "daily_ops_csv_path",
    "daily_ops_xlsx_path",
    "successful_signal_days_recorded",
)


def _repo_root() -> Path:
    return REPO_ROOT


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _resolve_timestamp(value: str | None) -> datetime:
    if value is None:
        return _chicago_now()

    parsed = datetime.fromisoformat(value)
    if ZoneInfo is not None:
        chicago = ZoneInfo("America/Chicago")
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=chicago)
        return parsed.astimezone(chicago)
    return parsed


def _safe_slug(value: str, *, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned.strip("._-") or fallback


def _fsync_directory(path: Path) -> None:
    try:
        dir_fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with tmp_path.open("w", encoding="utf-8", newline="") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with tmp_path.open("wb") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _append_jsonl_record(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_directory(path.parent)


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_csv(path: Path, *, rows: list[dict[str, Any]]) -> None:
    from io import StringIO

    sio = StringIO()
    writer = csv.DictWriter(sio, fieldnames=list(RUN_LOG_COLUMNS), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in RUN_LOG_COLUMNS})
    _atomic_write_text(path, sio.getvalue())


def _excel_column_name(index: int) -> str:
    value = index
    chars: list[str] = []
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        chars.append(chr(65 + remainder))
    return "".join(reversed(chars))


def _excel_cell(row_index: int, column_index: int, value: object) -> str:
    ref = f"{_excel_column_name(column_index)}{row_index}"
    if value is None or value == "":
        return f'<c r="{ref}"/>'

    if isinstance(value, bool):
        return f'<c r="{ref}" t="n"><v>{1 if value else 0}</v></c>'

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if math.isfinite(number):
            text = str(int(number)) if float(number).is_integer() else repr(number)
            return f'<c r="{ref}" t="n"><v>{text}</v></c>'

    text = xml_escape(str(value))
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'


def _build_sheet_xml(*, headers: list[str], rows: list[dict[str, Any]]) -> str:
    xml_rows: list[str] = []
    header_cells = "".join(_excel_cell(1, index, name) for index, name in enumerate(headers, start=1))
    xml_rows.append(f'<row r="1">{header_cells}</row>')
    for row_index, row in enumerate(rows, start=2):
        cells = "".join(
            _excel_cell(row_index, column_index, row.get(column_name, ""))
            for column_index, column_name in enumerate(headers, start=1)
        )
        xml_rows.append(f'<row r="{row_index}">{cells}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(xml_rows)}</sheetData>'
        "</worksheet>"
    )


def _core_props_xml(timestamp: datetime) -> str:
    created = timestamp.astimezone().isoformat()
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<dc:creator>Trading Codex</dc:creator>"
        "<cp:lastModifiedBy>Trading Codex</cp:lastModifiedBy>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{xml_escape(created)}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{xml_escape(created)}</dcterms:modified>'
        "</cp:coreProperties>"
    )


def _build_xlsx_bytes(*, headers: list[str], rows: list[dict[str, Any]], timestamp: datetime) -> bytes:
    from io import BytesIO

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="daily_runs" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )
    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" '
        'Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" '
        'Target="docProps/app.xml"/>'
        "</Relationships>"
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '<Override PartName="/docProps/core.xml" '
        'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        '<Override PartName="/docProps/app.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
        "</Types>"
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )
    app_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>Trading Codex</Application>"
        "<HeadingPairs><vt:vector size=\"2\" baseType=\"variant\">"
        "<vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant>"
        "<vt:variant><vt:i4>1</vt:i4></vt:variant>"
        "</vt:vector></HeadingPairs>"
        "<TitlesOfParts><vt:vector size=\"1\" baseType=\"lpstr\">"
        "<vt:lpstr>daily_runs</vt:lpstr>"
        "</vt:vector></TitlesOfParts>"
        "</Properties>"
    )

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", root_rels_xml)
        zf.writestr("docProps/app.xml", app_xml)
        zf.writestr("docProps/core.xml", _core_props_xml(timestamp))
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/styles.xml", styles_xml)
        zf.writestr("xl/worksheets/sheet1.xml", _build_sheet_xml(headers=headers, rows=rows))
    return buffer.getvalue()


def _write_xlsx(path: Path, *, rows: list[dict[str, Any]], timestamp: datetime) -> None:
    payload = _build_xlsx_bytes(headers=list(RUN_LOG_COLUMNS), rows=rows, timestamp=timestamp)
    _atomic_write_bytes(path, payload)


def _expand_path(value: Path | None) -> Path | None:
    if value is None:
        return None
    return Path(os.path.expanduser(os.path.expandvars(str(value)))).resolve()


def _extract_flag_value(args: list[str], flag: str) -> str | None:
    for index, item in enumerate(args):
        if item == flag and index + 1 < len(args):
            return args[index + 1]
    return None


def _resolve_data_dir(*, repo_root: Path, preset: daily_signal.Preset, explicit: Path | None) -> Path:
    if explicit is not None:
        return _expand_path(explicit) or explicit

    expanded = daily_signal._expand_known_path_args(preset.run_backtest_args)
    from_preset = _extract_flag_value(expanded, "--data-dir")
    if from_preset:
        return Path(from_preset)

    repo_data = repo_root / "data"
    if repo_data.exists():
        return repo_data

    return Path.home() / "trading_codex" / "data"


def _resolve_preset(
    *,
    repo_root: Path,
    preset_name: str,
    presets_path: Path | None,
) -> tuple[Path, daily_signal.Preset]:
    resolved_presets_path = _expand_path(presets_path) if presets_path is not None else daily_signal._default_presets_path(repo_root)
    if resolved_presets_path is None or not resolved_presets_path.exists():
        raise FileNotFoundError(f"Presets file not found: {resolved_presets_path}")
    presets = daily_signal._load_presets_json(resolved_presets_path)
    if preset_name not in presets:
        known = ", ".join(sorted(presets))
        raise ValueError(f"Unknown preset {preset_name!r}. Known: {known}")
    return resolved_presets_path, presets[preset_name]


def _resolve_symbols_for_preset(preset: daily_signal.Preset) -> list[str]:
    expanded = daily_signal._expand_known_path_args(preset.run_backtest_args)
    symbols = update_data_eod._extract_symbols_from_args(expanded)
    seen: set[str] = set()
    deduped: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    if not deduped:
        raise ValueError(f"Preset {preset.name!r} does not resolve to any update_data_eod symbols.")
    return deduped


def resolve_ops_paths(
    *,
    state_key: str,
    archive_root: Path | None = None,
    create: bool,
) -> dict[str, Path]:
    resolved_archive_root = resolve_archive_root(preferred_root=_expand_path(archive_root), create=create)
    ops_root = resolved_archive_root / "stage2_paper_ops" / _safe_slug(state_key, fallback=DEFAULT_PAPER_STATE_KEY)
    if create:
        ops_root.mkdir(parents=True, exist_ok=True)
    return {
        "archive_root": resolved_archive_root,
        "ops_root": ops_root,
        "jsonl_path": ops_root / "paper_lane_daily_ops_log.jsonl",
        "csv_path": ops_root / "paper_lane_daily_ops_runs.csv",
        "xlsx_path": ops_root / "paper_lane_daily_ops_runs.xlsx",
    }


def build_update_data_eod_cmd(
    *,
    repo_root: Path,
    provider: str,
    data_dir: Path,
    symbols: list[str],
) -> list[str]:
    return [
        sys.executable,
        str(repo_root / "scripts" / "update_data_eod.py"),
        "--provider",
        provider,
        "--data-dir",
        str(data_dir),
        "--verbose",
        "--symbols",
        *symbols,
    ]


def build_paper_lane_cmd(
    *,
    repo_root: Path,
    command: str,
    preset_name: str,
    presets_path: Path,
    state_key: str,
    data_dir: Path,
    paper_base_dir: Path | None,
    timestamp: str | None,
) -> list[str]:
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "paper_lane.py"),
        "--emit",
        "json",
        "--state-key",
        state_key,
    ]
    if paper_base_dir is not None:
        cmd.extend(["--base-dir", str(paper_base_dir)])
    if timestamp is not None:
        cmd.extend(["--timestamp", timestamp])
    cmd.extend(
        [
            command,
            "--preset",
            preset_name,
            "--presets-file",
            str(presets_path),
            "--data-dir",
            str(data_dir),
        ]
    )
    return cmd


def _run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root), env=env)


def _parse_update_metrics(stderr: str) -> dict[str, Any]:
    match = re.search(r"updated_symbols=(\d+)", stderr)
    return {
        "updated_symbols": None if match is None else int(match.group(1)),
    }


def _run_step(
    *,
    repo_root: Path,
    step_name: str,
    cmd: list[str],
    expect_json_stdout: bool,
    timestamp: datetime,
) -> dict[str, Any]:
    started = _chicago_now()
    proc = _run_process(cmd, repo_root=repo_root)
    completed = _chicago_now()
    parse_error: str | None = None
    stdout_json: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None

    if expect_json_stdout:
        if proc.returncode == 0:
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError as exc:
                parse_error = f"stdout JSON decode failed: {exc}"
            else:
                if isinstance(parsed, dict):
                    stdout_json = parsed
                else:
                    parse_error = "stdout JSON payload must be an object."
    else:
        metrics = _parse_update_metrics(proc.stderr)

    success = proc.returncode == 0 and parse_error is None
    return {
        "schema_name": STEP_SCHEMA_NAME,
        "schema_version": STEP_SCHEMA_VERSION,
        "step": step_name,
        "timestamp_chicago": timestamp.isoformat(),
        "started_at_chicago": started.isoformat(),
        "completed_at_chicago": completed.isoformat(),
        "duration_seconds": round((completed - started).total_seconds(), 6),
        "command": cmd,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "stdout_json": stdout_json,
        "metrics": metrics,
        "parse_error": parse_error,
        "success": success,
    }


def _successful_signal_days(rows: list[dict[str, Any]]) -> int:
    dates = {
        str(row.get("status_signal_date"))
        for row in rows
        if row.get("overall_result") == "ok" and row.get("status_signal_date")
    }
    return len(dates)


def _build_summary_row(
    *,
    run_id: str,
    timestamp: datetime,
    preset_name: str,
    state_key: str,
    provider: str,
    presets_path: Path,
    data_dir: Path,
    paper_base_dir: Path | None,
    ops_paths: dict[str, Path],
    manifest_path: Path,
    step_results: dict[str, dict[str, Any]],
    overall_result: str,
    failed_step: str | None,
    successful_signal_days_recorded: int,
) -> dict[str, Any]:
    status_json = (step_results.get("paper_lane_status") or {}).get("stdout_json") or {}
    apply_json = (step_results.get("paper_lane_apply") or {}).get("stdout_json") or {}
    update_metrics = (step_results.get("update_data_eod") or {}).get("metrics") or {}
    signal = status_json.get("signal") if isinstance(status_json.get("signal"), dict) else {}
    paths = status_json.get("paths") if isinstance(status_json.get("paths"), dict) else {}

    return {
        "schema_name": SUMMARY_SCHEMA_NAME,
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "run_id": run_id,
        "timestamp_chicago": timestamp.isoformat(),
        "ops_date": timestamp.date().isoformat(),
        "overall_result": overall_result,
        "failed_step": failed_step,
        "preset": preset_name,
        "state_key": state_key,
        "provider": provider,
        "presets_file": str(presets_path),
        "data_dir": str(data_dir),
        "paper_base_dir": "" if paper_base_dir is None else str(paper_base_dir),
        "update_exit_code": (step_results.get("update_data_eod") or {}).get("exit_code", ""),
        "update_updated_symbols": update_metrics.get("updated_symbols", ""),
        "status_exit_code": (step_results.get("paper_lane_status") or {}).get("exit_code", ""),
        "status_signal_date": signal.get("date", ""),
        "status_signal_action": signal.get("action", ""),
        "status_signal_symbol": signal.get("symbol", ""),
        "status_target_shares": signal.get("target_shares", ""),
        "status_next_rebalance": signal.get("next_rebalance", ""),
        "status_event_id": signal.get("event_id", ""),
        "status_drift_present": status_json.get("drift_present", ""),
        "status_event_already_applied": status_json.get("event_already_applied", ""),
        "status_archive_manifest_path": status_json.get("archive_manifest_path", ""),
        "apply_exit_code": (step_results.get("paper_lane_apply") or {}).get("exit_code", ""),
        "apply_result": apply_json.get("result", ""),
        "apply_duplicate_event_blocked": apply_json.get("duplicate_event_blocked", ""),
        "apply_event_receipt_path": apply_json.get("event_receipt_path", ""),
        "apply_archive_manifest_path": apply_json.get("archive_manifest_path", ""),
        "paper_state_path": paths.get("state_path", ""),
        "paper_ledger_path": paths.get("ledger_path", ""),
        "daily_ops_manifest_path": str(manifest_path),
        "daily_ops_jsonl_path": str(ops_paths["jsonl_path"]),
        "daily_ops_csv_path": str(ops_paths["csv_path"]),
        "daily_ops_xlsx_path": str(ops_paths["xlsx_path"]),
        "successful_signal_days_recorded": successful_signal_days_recorded,
    }


def _render_summary_text(
    *,
    run_id: str,
    summary_row: dict[str, Any],
) -> str:
    lines = [
        f"Stage 2 daily ops run {run_id}",
        f"Result: {summary_row['overall_result']}",
        f"Preset: {summary_row['preset']}",
        f"Signal: {summary_row['status_signal_date']} {summary_row['status_signal_action']} {summary_row['status_signal_symbol']}",
        f"Event ID: {summary_row['status_event_id']}",
        f"Update exit: {summary_row['update_exit_code']} (updated_symbols={summary_row['update_updated_symbols']})",
        f"Status exit: {summary_row['status_exit_code']} drift_present={summary_row['status_drift_present']}",
        f"Apply exit: {summary_row['apply_exit_code']} result={summary_row['apply_result']}",
        f"Daily ops manifest: {summary_row['daily_ops_manifest_path']}",
        f"JSONL log: {summary_row['daily_ops_jsonl_path']}",
        f"CSV log: {summary_row['daily_ops_csv_path']}",
        f"XLSX workbook: {summary_row['daily_ops_xlsx_path']}",
        f"Successful signal days recorded: {summary_row['successful_signal_days_recorded']}",
    ]
    if summary_row["failed_step"]:
        lines.insert(2, f"Failed step: {summary_row['failed_step']}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the narrow Stage 2 paper-lane daily ops routine: update data, check paper status, "
            "apply paper action, and retain review artifacts outside the repo tree."
        )
    )
    parser.add_argument("--preset", default=DEFAULT_PRESET, help=f"Paper-lane preset name. Default: {DEFAULT_PRESET}")
    parser.add_argument(
        "--provider",
        choices=["stooq", "tiingo"],
        default=DEFAULT_PROVIDER,
        help=f"Data provider for update_data_eod. Default: {DEFAULT_PROVIDER}",
    )
    parser.add_argument(
        "--presets-file",
        type=Path,
        default=None,
        help="Optional presets path. Defaults to configs/presets.json then configs/presets.example.json.",
    )
    parser.add_argument("--state-key", default=DEFAULT_PAPER_STATE_KEY, help="Paper lane state key.")
    parser.add_argument("--data-dir", type=Path, default=None, help="Optional data dir override.")
    parser.add_argument("--paper-base-dir", type=Path, default=None, help="Optional paper lane state dir override.")
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="Optional archive root override. Defaults to ~/.trading_codex, then ~/.cache/trading_codex, then /tmp/trading_codex.",
    )
    parser.add_argument("--timestamp", type=str, default=None, help="Optional ISO timestamp override for deterministic tests.")
    parser.add_argument("--emit", choices=["text", "json"], default="text", help="Stdout format.")
    return parser


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()
    args = build_parser().parse_args(argv)

    try:
        timestamp = _resolve_timestamp(args.timestamp)
        resolved_paper_base_dir = _expand_path(args.paper_base_dir)
        resolved_presets_path, preset = _resolve_preset(
            repo_root=repo_root,
            preset_name=args.preset,
            presets_path=args.presets_file,
        )
        data_dir = _resolve_data_dir(repo_root=repo_root, preset=preset, explicit=args.data_dir)
        symbols = _resolve_symbols_for_preset(preset)
        ops_paths = resolve_ops_paths(
            state_key=args.state_key,
            archive_root=args.archive_root,
            create=True,
        )
    except Exception as exc:
        print(f"[paper_lane_daily_ops] ERROR: {exc}", file=sys.stderr)
        return 2

    step_specs = [
        (
            "update_data_eod",
            build_update_data_eod_cmd(
                repo_root=repo_root,
                provider=args.provider,
                data_dir=data_dir,
                symbols=symbols,
            ),
            False,
        ),
        (
            "paper_lane_status",
            build_paper_lane_cmd(
                repo_root=repo_root,
                command="status",
                preset_name=args.preset,
                presets_path=resolved_presets_path,
                state_key=args.state_key,
                data_dir=data_dir,
                paper_base_dir=resolved_paper_base_dir,
                timestamp=timestamp.isoformat(),
            ),
            True,
        ),
        (
            "paper_lane_apply",
            build_paper_lane_cmd(
                repo_root=repo_root,
                command="apply",
                preset_name=args.preset,
                presets_path=resolved_presets_path,
                state_key=args.state_key,
                data_dir=data_dir,
                paper_base_dir=resolved_paper_base_dir,
                timestamp=timestamp.isoformat(),
            ),
            True,
        ),
    ]

    step_results: dict[str, dict[str, Any]] = {}
    failed_step: str | None = None
    failed_exit_code = 0

    for step_name, cmd, expect_json_stdout in step_specs:
        result = _run_step(
            repo_root=repo_root,
            step_name=step_name,
            cmd=cmd,
            expect_json_stdout=expect_json_stdout,
            timestamp=timestamp,
        )
        step_results[step_name] = result
        if not result["success"]:
            failed_step = step_name
            failed_exit_code = int(result["exit_code"]) or 2
            break

    overall_result = "failed" if failed_step else "ok"
    prior_rows = _load_jsonl_records(ops_paths["jsonl_path"])
    provisional_summary = {
        "overall_result": overall_result,
        "status_signal_date": (
            (((step_results.get("paper_lane_status") or {}).get("stdout_json") or {}).get("signal") or {}).get("date")
        ),
    }
    successful_signal_days_recorded = _successful_signal_days(prior_rows + [provisional_summary])

    archive = write_run_archive(
        timestamp=timestamp,
        run_kind="paper_lane_daily_ops",
        mode=overall_result,
        label=args.state_key,
        identity_parts=[args.state_key, args.preset, timestamp.date().isoformat()],
        manifest_fields={
            "failed_step": failed_step,
            "preset": args.preset,
            "provider": args.provider,
            "state_key": args.state_key,
        },
        json_artifacts={
            "daily_ops_run": {
                "schema_name": RUN_SCHEMA_NAME,
                "schema_version": RUN_SCHEMA_VERSION,
                "timestamp_chicago": timestamp.isoformat(),
                "preset": args.preset,
                "provider": args.provider,
                "presets_file": str(resolved_presets_path),
                "state_key": args.state_key,
                "data_dir": str(data_dir),
                "paper_base_dir": None if resolved_paper_base_dir is None else str(resolved_paper_base_dir),
                "symbols": symbols,
                "overall_result": overall_result,
                "failed_step": failed_step,
                "step_results": step_results,
            },
            **{step_name: payload for step_name, payload in step_results.items()},
        },
        text_artifacts={
            "summary_text": "\n".join(
                [
                    f"preset={args.preset}",
                    f"provider={args.provider}",
                    f"state_key={args.state_key}",
                    f"overall_result={overall_result}",
                    f"failed_step={failed_step or ''}",
                ]
            )
        },
        preferred_root=ops_paths["archive_root"],
    )

    summary_row = _build_summary_row(
        run_id=archive.manifest["run_id"],
        timestamp=timestamp,
        preset_name=args.preset,
        state_key=args.state_key,
        provider=args.provider,
        presets_path=resolved_presets_path,
        data_dir=data_dir,
        paper_base_dir=resolved_paper_base_dir,
        ops_paths=ops_paths,
        manifest_path=archive.paths.manifest_path,
        step_results=step_results,
        overall_result=overall_result,
        failed_step=failed_step,
        successful_signal_days_recorded=successful_signal_days_recorded,
    )

    _append_jsonl_record(ops_paths["jsonl_path"], summary_row)
    all_rows = _load_jsonl_records(ops_paths["jsonl_path"])
    _write_csv(ops_paths["csv_path"], rows=all_rows)
    _write_xlsx(ops_paths["xlsx_path"], rows=all_rows, timestamp=timestamp)

    text_summary = _render_summary_text(run_id=archive.manifest["run_id"], summary_row=summary_row)
    if args.emit == "json":
        print(
            json.dumps(
                {
                    "schema_name": RUN_SCHEMA_NAME,
                    "schema_version": RUN_SCHEMA_VERSION,
                    "archive_manifest_path": str(archive.paths.manifest_path),
                    "summary": summary_row,
                    "step_results": step_results,
                },
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
        )
    else:
        print(text_summary)

    if failed_step is not None:
        print(
            f"[paper_lane_daily_ops] ERROR: step {failed_step} failed; see {archive.paths.manifest_path}",
            file=sys.stderr,
        )
        return failed_exit_code

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
