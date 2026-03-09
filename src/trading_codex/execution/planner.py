from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.data import LocalStore
from trading_codex.execution.models import BrokerSnapshot, ExecutionPlan, PlanItem, SignalPayload
from trading_codex.execution.signals import desired_positions_from_signal

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


def _classify_item(*, signal: SignalPayload, symbol: str, desired: int, current: int) -> str:
    if desired == current:
        return "HOLD"
    if desired <= 0 and current > 0:
        if signal.action == "EXIT" or signal.symbol.upper() == "CASH":
            return "EXIT"
        return "SELL"
    if desired > 0 and current <= 0:
        return "BUY"
    if desired > current:
        return "RESIZE_BUY"
    return "RESIZE_SELL"


def _resolve_price_from_store(symbol: str, *, data_dir: Path | None) -> float | None:
    if data_dir is None:
        return None
    try:
        store = LocalStore(base_dir=data_dir)
        bars = store.read_bars(symbol)
    except (FileNotFoundError, ValueError):
        return None
    if bars.empty or "close" not in bars.columns:
        return None
    return float(bars["close"].iloc[-1])


def resolve_reference_price(
    *,
    signal: SignalPayload,
    broker_snapshot: BrokerSnapshot,
    symbol: str,
    data_dir: Path | None,
) -> float | None:
    if signal.symbol == symbol and signal.price is not None:
        return float(signal.price)
    broker_position = broker_snapshot.positions.get(symbol)
    if broker_position is not None and broker_position.price is not None:
        return float(broker_position.price)
    return _resolve_price_from_store(symbol, data_dir=data_dir)


def build_execution_plan(
    *,
    signal: SignalPayload,
    broker_snapshot: BrokerSnapshot,
    source_kind: str,
    source_label: str,
    source_ref: str | None,
    broker_source_ref: str | None,
    data_dir: Path | None,
    generated_at: datetime | None = None,
) -> ExecutionPlan:
    generated_dt = generated_at or _chicago_now()
    desired_positions = desired_positions_from_signal(signal)
    broker_symbols = {symbol for symbol, position in broker_snapshot.positions.items() if position.shares != 0}
    symbols = sorted(set(desired_positions) | broker_symbols)

    items: list[PlanItem] = []
    total_buy_notional = 0.0
    total_sell_notional = 0.0
    warnings: list[str] = []
    blockers: list[str] = []

    for symbol in symbols:
        desired = desired_positions.get(symbol, 0)
        current = broker_snapshot.positions.get(symbol).shares if symbol in broker_snapshot.positions else 0
        classification = _classify_item(signal=signal, symbol=symbol, desired=desired, current=current)
        delta = desired - current

        item_warnings: list[str] = []
        item_blockers: list[str] = []
        if desired < 0 or current < 0:
            item_blockers.append("negative_shares_not_supported")

        reference_price = resolve_reference_price(
            signal=signal,
            broker_snapshot=broker_snapshot,
            symbol=symbol,
            data_dir=data_dir,
        )
        estimated_notional: float | None = None
        if delta != 0 and reference_price is None:
            item_warnings.append("missing_reference_price")
        elif reference_price is not None:
            estimated_notional = round(abs(delta) * reference_price, 2)
            if delta > 0:
                total_buy_notional += estimated_notional
            elif delta < 0:
                total_sell_notional += estimated_notional

        items.append(
            PlanItem(
                symbol=symbol,
                desired_target_shares=desired,
                current_broker_shares=current,
                delta_shares=delta,
                classification=classification,
                reference_price=None if reference_price is None else round(reference_price, 6),
                estimated_notional=estimated_notional,
                warnings=item_warnings,
                blockers=item_blockers,
            )
        )
        warnings.extend(item_warnings)
        blockers.extend(item_blockers)

    total_buy_notional = round(total_buy_notional, 2)
    total_sell_notional = round(total_sell_notional, 2)
    net_notional = round(total_buy_notional - total_sell_notional, 2)

    if broker_snapshot.buying_power is not None and total_buy_notional > round(broker_snapshot.buying_power, 2):
        blockers.append("buy_notional_exceeds_buying_power")
    elif broker_snapshot.cash is not None and total_buy_notional > round(broker_snapshot.cash, 2):
        warnings.append("buy_notional_exceeds_cash")

    return ExecutionPlan(
        generated_at_chicago=generated_dt.isoformat(),
        dry_run=True,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        broker_source_ref=broker_source_ref,
        signal=signal,
        broker_snapshot=broker_snapshot,
        items=items,
        total_buy_notional=total_buy_notional,
        total_sell_notional=total_sell_notional,
        net_notional=net_notional,
        warnings=sorted(set(warnings)),
        blockers=sorted(set(blockers)),
    )


def execution_plan_to_dict(plan: ExecutionPlan, *, artifacts: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "artifacts": artifacts or {},
        "blockers": list(plan.blockers),
        "broker_snapshot": {
            "account_id": plan.broker_snapshot.account_id,
            "as_of": plan.broker_snapshot.as_of,
            "broker_name": plan.broker_snapshot.broker_name,
            "buying_power": plan.broker_snapshot.buying_power,
            "cash": plan.broker_snapshot.cash,
            "positions": [
                {
                    "price": position.price,
                    "shares": position.shares,
                    "symbol": position.symbol,
                }
                for symbol, position in sorted(plan.broker_snapshot.positions.items())
            ],
        },
        "dry_run": plan.dry_run,
        "generated_at_chicago": plan.generated_at_chicago,
        "items": [
            {
                "blockers": list(item.blockers),
                "classification": item.classification,
                "current_broker_shares": item.current_broker_shares,
                "delta_shares": item.delta_shares,
                "desired_target_shares": item.desired_target_shares,
                "estimated_notional": item.estimated_notional,
                "reference_price": item.reference_price,
                "symbol": item.symbol,
                "warnings": list(item.warnings),
            }
            for item in plan.items
        ],
        "schema_name": "execution_plan",
        "schema_version": 1,
        "signal": {
            "action": plan.signal.action,
            "date": plan.signal.date,
            "event_id": plan.signal.event_id,
            "next_rebalance": plan.signal.next_rebalance,
            "price": plan.signal.price,
            "resize_new_shares": plan.signal.resize_new_shares,
            "resize_prev_shares": plan.signal.resize_prev_shares,
            "symbol": plan.signal.symbol,
            "target_shares": plan.signal.target_shares,
            "strategy": plan.signal.strategy,
        },
        "source": {
            "broker_source_ref": plan.broker_source_ref,
            "kind": plan.source_kind,
            "label": plan.source_label,
            "ref": plan.source_ref,
        },
        "totals": {
            "buy_notional": plan.total_buy_notional,
            "net_notional": plan.net_notional,
            "sell_notional": plan.total_sell_notional,
        },
        "warnings": list(plan.warnings),
    }
