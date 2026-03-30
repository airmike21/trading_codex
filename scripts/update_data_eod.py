#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from trading_codex.data import LocalStore
from trading_codex.data.providers import StooqDataSource


@dataclass(frozen=True)
class TiingoCandle:
    d: date
    o: float
    h: float
    l: float
    c: float
    v: float


def _today_chicago() -> date:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Chicago")).date()
    except Exception:
        return datetime.now().date()


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_presets_path(repo_root: Path) -> Path:
    preferred = repo_root / "configs" / "presets.json"
    fallback = repo_root / "configs" / "presets.example.json"
    if preferred.exists():
        return preferred
    return fallback


def _extract_symbols_from_args(rb_args: list[str]) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(rb_args):
        token = rb_args[i]
        if token == "--symbols":
            j = i + 1
            while j < len(rb_args) and not rb_args[j].startswith("--"):
                out.append(rb_args[j])
                j += 1
            i = j
            continue
        if token in ("--vm-defensive-symbol", "--defensive", "--dm-defensive-symbol", "--dmv-defensive-symbol"):
            if i + 1 < len(rb_args) and not rb_args[i + 1].startswith("--"):
                out.append(rb_args[i + 1])
            i += 2
            continue
        i += 1
    return out


def _load_presets_symbols(presets_path: Path) -> list[str]:
    raw = json.loads(presets_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return []
    source = raw.get("presets") if isinstance(raw.get("presets"), dict) else raw
    if not isinstance(source, dict):
        return []

    symbols: list[str] = []
    for preset in source.values():
        if not isinstance(preset, dict):
            continue
        rb_args = preset.get("run_backtest_args")
        if not isinstance(rb_args, list) or not all(isinstance(x, str) for x in rb_args):
            continue
        symbols.extend(_extract_symbols_from_args(rb_args))

    seen: set[str] = set()
    deduped: list[str] = []
    for sym in symbols:
        s = sym.strip().upper()
        if s and s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def _ensure_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]
    required = ["open", "high", "low", "close"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if "volume" not in out.columns:
        out["volume"] = 0.0
    out = out[["open", "high", "low", "close", "volume"]]
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index)
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out


def _fetch_stooq_bars(
    symbol: str,
    start: date,
    end: date,
    stooq_suffix: str,
    timeout: float,
) -> pd.DataFrame:
    provider = StooqDataSource(timeout=timeout, symbol_suffix=stooq_suffix)
    panel = provider.get_daily_bars([symbol], pd.Timestamp(start), pd.Timestamp(end))
    if panel.empty or symbol not in panel.columns.get_level_values(0):
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    return _ensure_ohlcv(panel[symbol])


def _fetch_tiingo_bars(
    symbol: str,
    start: date,
    end: date,
    api_key: str,
    timeout: float,
) -> pd.DataFrame:
    endpoint = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "resampleFreq": "daily",
        "format": "json",
    }
    headers = {"Authorization": f"Token {api_key}"}
    resp = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list) or not payload:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    rows: list[TiingoCandle] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        ds = str(row.get("date", ""))[:10]
        try:
            d = _parse_date(ds)
            rows.append(
                TiingoCandle(
                    d=d,
                    o=float(row["open"]),
                    h=float(row["high"]),
                    l=float(row["low"]),
                    c=float(row["close"]),
                    v=float(row.get("volume", 0.0)),
                )
            )
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    idx = pd.DatetimeIndex([pd.Timestamp(r.d) for r in rows], name="date")
    df = pd.DataFrame(
        {
            "open": [r.o for r in rows],
            "high": [r.h for r in rows],
            "low": [r.l for r in rows],
            "close": [r.c for r in rows],
            "volume": [r.v for r in rows],
        },
        index=idx,
    )
    return _ensure_ohlcv(df)


def _merge_existing(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return _ensure_ohlcv(new)
    merged = pd.concat([_ensure_ohlcv(existing), _ensure_ohlcv(new)], axis=0)
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    return merged


def _read_existing_bars(store: LocalStore, symbol: str) -> pd.DataFrame | None:
    try:
        return store.read_bars(symbol)
    except FileNotFoundError:
        return None


def _resolve_symbols(args: argparse.Namespace, presets_path: Path) -> list[str]:
    if args.symbols:
        return [s.upper() for s in args.symbols]
    return _load_presets_symbols(presets_path)


def main(argv: list[str] | None = None) -> int:
    repo_root = _repo_root()

    parser = argparse.ArgumentParser(
        description=(
            "Update local daily OHLCV bars in LocalStore format. "
            "Tiingo is recommended; Stooq is the free fallback."
        )
    )
    parser.add_argument("--provider", choices=["tiingo", "stooq"], default="tiingo")
    parser.add_argument("--data-dir", type=Path, default=Path.home() / "trading_codex" / "data")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--presets-file", type=Path, default=None)
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--stooq-suffix", default=".us")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    start = _parse_date(args.start)
    end = _parse_date(args.end) if args.end else _today_chicago()
    if end < start:
        print("[update_data_eod] ERROR: --end must be >= --start", file=sys.stderr)
        return 2

    presets_path = args.presets_file or _default_presets_path(repo_root)
    symbols = _resolve_symbols(args, presets_path)
    if not symbols:
        print(
            f"[update_data_eod] ERROR: no symbols found (presets: {presets_path}). Pass --symbols.",
            file=sys.stderr,
        )
        return 2

    tiingo_key = os.environ.get("TIINGO_API_KEY", "")
    if args.provider == "tiingo" and not tiingo_key:
        print("[update_data_eod] ERROR: TIINGO_API_KEY is required for --provider tiingo.", file=sys.stderr)
        return 2

    store = LocalStore(base_dir=args.data_dir)
    updated = 0

    for symbol in symbols:
        existing = _read_existing_bars(store, symbol)
        fetch_start = start
        if existing is not None and not existing.empty:
            last_dt = existing.index.max().date()
            fetch_start = max(start, last_dt - timedelta(days=7))

        if args.verbose:
            print(
                f"[update_data_eod] {symbol}: provider={args.provider} fetch={fetch_start}..{end}",
                file=sys.stderr,
            )

        if args.provider == "tiingo":
            try:
                new_df = _fetch_tiingo_bars(symbol, fetch_start, end, tiingo_key, args.timeout)
            except Exception as exc:
                print(
                    f"[update_data_eod] ERROR: {symbol}: provider=tiingo fetch failed: {exc}",
                    file=sys.stderr,
                )
                return 2
        else:
            try:
                new_df = _fetch_stooq_bars(symbol, fetch_start, end, args.stooq_suffix, args.timeout)
            except Exception as exc:
                print(
                    f"[update_data_eod] ERROR: {symbol}: provider=stooq fetch failed: {exc}",
                    file=sys.stderr,
                )
                return 2

        if new_df.empty:
            if args.verbose:
                print(f"[update_data_eod] {symbol}: no rows returned", file=sys.stderr)
            continue

        merged = _merge_existing(existing, new_df)
        if args.dry_run:
            print(
                f"[update_data_eod] {symbol}: would write rows={len(merged)} (fetched={len(new_df)})",
                file=sys.stderr,
            )
            continue

        store.write_bars(symbol, merged)
        updated += 1
        if args.verbose:
            print(f"[update_data_eod] {symbol}: wrote rows={len(merged)}", file=sys.stderr)

    print(f"[update_data_eod] updated_symbols={updated}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
