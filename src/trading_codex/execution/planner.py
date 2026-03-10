from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from trading_codex.data import LocalStore
from trading_codex.execution.models import (
    ACCOUNT_SCOPES,
    SIZING_MODES,
    BrokerPosition,
    BrokerSnapshot,
    ExecutionPlan,
    OrderIntent,
    OrderIntentExport,
    PlanItem,
    ScopedBrokerPosition,
    SignalPayload,
    SimulatedOrderRequest,
    SimulatedSubmissionExport,
    SizingContext,
)
from trading_codex.execution.signals import desired_positions_from_signal

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


def _chicago_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)


SUPPORTED_INSTRUMENT_TYPES = {"equity"}
DERIVATIVE_INSTRUMENT_MARKERS = ("option", "future", "derivative")
DEFAULT_SIGNAL_CAPITAL_BASE = 10_000.0
ORDER_INTENT_SIDE_BY_CLASSIFICATION = {
    "BUY": "BUY",
    "RESIZE_BUY": "BUY",
    "SELL": "SELL",
    "RESIZE_SELL": "SELL",
    "EXIT": "SELL",
}
SIMULATED_ORDER_TYPE = "MARKET"
SIMULATED_TIME_IN_FORCE = "DAY"


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


def _normalized_instrument_type(position: BrokerPosition) -> str | None:
    if position.instrument_type is None:
        return None
    normalized = position.instrument_type.strip().lower()
    return normalized or None


def _scope_symbol(position: BrokerPosition) -> str:
    return (position.underlying_symbol or position.symbol).upper()


def _unsupported_position_reason(position: BrokerPosition) -> str | None:
    instrument_type = _normalized_instrument_type(position)
    if instrument_type is not None and any(marker in instrument_type for marker in DERIVATIVE_INSTRUMENT_MARKERS):
        return "derivative_position"
    if position.shares < 0:
        return "short_equity_position"
    if instrument_type is not None and instrument_type not in SUPPORTED_INSTRUMENT_TYPES:
        return "unsupported_instrument_type"
    return None


def _scoped_position(position: BrokerPosition, *, classification_reason: str) -> ScopedBrokerPosition:
    return ScopedBrokerPosition(
        symbol=position.symbol,
        scope_symbol=_scope_symbol(position),
        shares=position.shares,
        price=position.price,
        instrument_type=position.instrument_type,
        underlying_symbol=position.underlying_symbol,
        classification_reason=classification_reason,
    )


def _nonzero_positions(broker_snapshot: BrokerSnapshot) -> list[BrokerPosition]:
    return [
        broker_snapshot.positions[symbol]
        for symbol in sorted(broker_snapshot.positions)
        if broker_snapshot.positions[symbol].shares != 0
    ]


def _classify_broker_positions(
    *,
    broker_snapshot: BrokerSnapshot,
    managed_symbols: set[str],
) -> tuple[list[ScopedBrokerPosition], list[ScopedBrokerPosition], list[ScopedBrokerPosition], dict[str, BrokerPosition]]:
    managed_supported_positions: list[ScopedBrokerPosition] = []
    managed_unsupported_positions: list[ScopedBrokerPosition] = []
    unmanaged_positions: list[ScopedBrokerPosition] = []
    full_account_supported_positions: dict[str, BrokerPosition] = {}

    for position in _nonzero_positions(broker_snapshot):
        support_reason = _unsupported_position_reason(position)
        if support_reason is None:
            full_account_supported_positions[position.symbol] = position

        if _scope_symbol(position) in managed_symbols:
            if support_reason is None:
                managed_supported_positions.append(
                    _scoped_position(position, classification_reason="supported_equity_position")
                )
            else:
                managed_unsupported_positions.append(_scoped_position(position, classification_reason=support_reason))
            continue

        unmanaged_positions.append(
            _scoped_position(position, classification_reason=support_reason or "outside_managed_universe")
        )

    return (
        managed_supported_positions,
        managed_unsupported_positions,
        unmanaged_positions,
        full_account_supported_positions,
    )


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


def _resolve_desired_positions(
    *,
    signal: SignalPayload,
    broker_snapshot: BrokerSnapshot,
    data_dir: Path | None,
    sizing_mode: str,
    capital_input: float | None,
    cap_to_buying_power: bool,
    reserve_cash_pct: float,
    max_allocation_pct: float,
    baseline_signal_capital: float,
) -> tuple[dict[str, int], SizingContext, list[str], list[str]]:
    if sizing_mode not in SIZING_MODES:
        raise ValueError(f"Unsupported sizing_mode {sizing_mode!r}. Expected one of: {', '.join(SIZING_MODES)}")
    if sizing_mode == "signal_target_shares":
        if capital_input is not None:
            raise ValueError("capital_input must be omitted when sizing_mode='signal_target_shares'.")
        return (
            desired_positions_from_signal(signal),
            SizingContext(
                mode=sizing_mode,
                baseline_signal_capital=None,
                capital_input=None,
                effective_capital_used=None,
                buying_power_cap_applied=False,
                reserve_cash_pct=float(reserve_cash_pct),
                max_allocation_pct=float(max_allocation_pct),
                usable_capital=None,
                inferred_signal_allocation_pct=None,
                applied_allocation_pct=None,
            ),
            [],
            [],
        )

    if capital_input is None or capital_input <= 0:
        raise ValueError("capital_input must be > 0 when using capital-based sizing.")
    if reserve_cash_pct < 0.0 or reserve_cash_pct >= 1.0:
        raise ValueError("reserve_cash_pct must be >= 0 and < 1.")
    if max_allocation_pct <= 0.0 or max_allocation_pct > 1.0:
        raise ValueError("max_allocation_pct must be > 0 and <= 1.")
    if baseline_signal_capital <= 0.0:
        raise ValueError("baseline_signal_capital must be > 0.")

    desired_positions = desired_positions_from_signal(signal)
    sizing_warnings: list[str] = []
    effective_capital = float(capital_input)
    buying_power_cap_applied = False
    if cap_to_buying_power:
        if broker_snapshot.buying_power is None:
            sizing_warnings.append("buying_power_missing_for_cap_to_buying_power")
        else:
            buying_power = float(broker_snapshot.buying_power)
            effective_capital = min(effective_capital, buying_power)
            buying_power_cap_applied = effective_capital < float(capital_input)
    effective_capital = round(effective_capital, 2)
    usable_capital = round(effective_capital * (1.0 - float(reserve_cash_pct)), 2)
    base_context = {
        "mode": sizing_mode,
        "baseline_signal_capital": float(baseline_signal_capital),
        "capital_input": float(capital_input),
        "effective_capital_used": effective_capital,
        "buying_power_cap_applied": buying_power_cap_applied,
        "reserve_cash_pct": float(reserve_cash_pct),
        "max_allocation_pct": float(max_allocation_pct),
        "usable_capital": usable_capital,
    }
    if not desired_positions:
        return (
            desired_positions,
            SizingContext(
                inferred_signal_allocation_pct=None,
                applied_allocation_pct=None,
                **base_context,
            ),
            sizing_warnings,
            [],
        )
    if len(desired_positions) != 1:
        raise ValueError("Capital sizing currently supports a single desired symbol.")

    symbol, signal_target_shares = next(iter(desired_positions.items()))
    reference_price = resolve_reference_price(
        signal=signal,
        broker_snapshot=broker_snapshot,
        symbol=symbol,
        data_dir=data_dir,
    )
    if reference_price is None or reference_price <= 0.0:
        return (
            desired_positions,
            SizingContext(
                inferred_signal_allocation_pct=None,
                applied_allocation_pct=None,
                **base_context,
            ),
            sizing_warnings,
            ["capital_sizing_missing_reference_price"],
        )

    inferred_signal_allocation_pct = min(
        1.0,
        max(0.0, round((signal_target_shares * reference_price) / float(baseline_signal_capital), 6)),
    )
    applied_allocation_pct = min(inferred_signal_allocation_pct, float(max_allocation_pct))
    computed_shares = int((usable_capital * applied_allocation_pct) // reference_price)
    computed_positions = {symbol: computed_shares} if computed_shares > 0 else {}
    blockers: list[str] = []
    if signal_target_shares > 0 and computed_shares <= 0:
        blockers.append("capital_sizing_yields_zero_shares")

    return (
        computed_positions,
        SizingContext(
            inferred_signal_allocation_pct=inferred_signal_allocation_pct,
            applied_allocation_pct=applied_allocation_pct,
            **base_context,
        ),
        sizing_warnings,
        blockers,
    )


def build_execution_plan(
    *,
    signal: SignalPayload,
    broker_snapshot: BrokerSnapshot,
    account_scope: str = "full_account",
    managed_symbols: set[str] | None = None,
    ack_unmanaged_holdings: bool = False,
    source_kind: str,
    source_label: str,
    source_ref: str | None,
    broker_source_ref: str | None,
    data_dir: Path | None,
    generated_at: datetime | None = None,
    sizing_mode: str = "signal_target_shares",
    capital_input: float | None = None,
    cap_to_buying_power: bool = False,
    reserve_cash_pct: float = 0.0,
    max_allocation_pct: float = 1.0,
    baseline_signal_capital: float = DEFAULT_SIGNAL_CAPITAL_BASE,
) -> ExecutionPlan:
    if account_scope not in ACCOUNT_SCOPES:
        raise ValueError(f"Unsupported account_scope {account_scope!r}. Expected one of: {', '.join(ACCOUNT_SCOPES)}")
    generated_dt = generated_at or _chicago_now()
    desired_positions, sizing, sizing_warnings, sizing_blockers = _resolve_desired_positions(
        signal=signal,
        broker_snapshot=broker_snapshot,
        data_dir=data_dir,
        sizing_mode=sizing_mode,
        capital_input=capital_input,
        cap_to_buying_power=cap_to_buying_power,
        reserve_cash_pct=reserve_cash_pct,
        max_allocation_pct=max_allocation_pct,
        baseline_signal_capital=baseline_signal_capital,
    )
    managed_symbol_list = sorted({symbol.upper() for symbol in (managed_symbols or set())})
    managed_symbol_set = set(managed_symbol_list)
    managed_supported_positions: list[ScopedBrokerPosition] = []
    managed_unsupported_positions: list[ScopedBrokerPosition] = []
    unmanaged_positions: list[ScopedBrokerPosition] = []

    if managed_symbol_set:
        (
            managed_supported_positions,
            managed_unsupported_positions,
            unmanaged_positions,
            _full_account_supported_positions,
        ) = _classify_broker_positions(
            broker_snapshot=broker_snapshot,
            managed_symbols=managed_symbol_set,
        )
        planning_positions = {
            position.symbol: broker_snapshot.positions[position.symbol]
            for position in managed_supported_positions
        }
        if account_scope == "managed_sleeve":
            plan_math_scope = "managed_sleeve_only"
        else:
            plan_math_scope = "managed_supported_positions_with_full_account_blockers"
    else:
        planning_positions = dict(broker_snapshot.positions)
        plan_math_scope = "full_account_positions"

    broker_symbols = {symbol for symbol, position in planning_positions.items() if position.shares != 0}
    symbols = sorted(set(desired_positions) | broker_symbols)

    items: list[PlanItem] = []
    total_buy_notional = 0.0
    total_sell_notional = 0.0
    warnings: list[str] = list(sizing_warnings)
    blockers: list[str] = list(sizing_blockers)

    for symbol in symbols:
        desired = desired_positions.get(symbol, 0)
        current = planning_positions.get(symbol).shares if symbol in planning_positions else 0
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

    if managed_symbol_set:
        if managed_unsupported_positions:
            blockers.append("managed_unsupported_positions_present")
            blockers.append(
                "managed_unsupported_symbols:" + ",".join(position.symbol for position in managed_unsupported_positions)
            )
        if unmanaged_positions:
            if account_scope == "managed_sleeve":
                if ack_unmanaged_holdings:
                    warnings.append("unmanaged_positions_acknowledged_for_managed_sleeve")
                else:
                    blockers.append("unmanaged_positions_present")
                    blockers.append("unmanaged_symbols:" + ",".join(position.symbol for position in unmanaged_positions))
                    blockers.append("ack_unmanaged_holdings_required")
            else:
                blockers.append("unmanaged_positions_present")
                blockers.append("unmanaged_symbols:" + ",".join(position.symbol for position in unmanaged_positions))
                blockers.append("full_account_scope_blocked_by_unmanaged_positions")

    if broker_snapshot.buying_power is not None and total_buy_notional > round(broker_snapshot.buying_power, 2):
        blockers.append("buy_notional_exceeds_buying_power")
    elif broker_snapshot.cash is not None and total_buy_notional > round(broker_snapshot.cash, 2):
        warnings.append("buy_notional_exceeds_cash")

    return ExecutionPlan(
        generated_at_chicago=generated_dt.isoformat(),
        dry_run=True,
        account_scope=account_scope,
        plan_math_scope=plan_math_scope,
        managed_symbols_universe=managed_symbol_list,
        unmanaged_holdings_acknowledged=ack_unmanaged_holdings,
        source_kind=source_kind,
        source_label=source_label,
        source_ref=source_ref,
        broker_source_ref=broker_source_ref,
        signal=signal,
        sizing=sizing,
        broker_snapshot=broker_snapshot,
        managed_supported_positions=managed_supported_positions,
        managed_unsupported_positions=managed_unsupported_positions,
        unmanaged_positions=unmanaged_positions,
        items=items,
        total_buy_notional=total_buy_notional,
        total_sell_notional=total_sell_notional,
        net_notional=net_notional,
        warnings=sorted(set(warnings)),
        blockers=sorted(set(blockers)),
    )


def _sizing_payload(sizing: SizingContext) -> dict[str, Any]:
    return {
        "applied_allocation_pct": sizing.applied_allocation_pct,
        "baseline_signal_capital": sizing.baseline_signal_capital,
        "buying_power_cap_applied": sizing.buying_power_cap_applied,
        "capital_input": sizing.capital_input,
        "effective_capital_used": sizing.effective_capital_used,
        "inferred_signal_allocation_pct": sizing.inferred_signal_allocation_pct,
        "max_allocation_pct": sizing.max_allocation_pct,
        "mode": sizing.mode,
        "reserve_cash_pct": sizing.reserve_cash_pct,
        "usable_capital": sizing.usable_capital,
    }


def execution_plan_to_dict(plan: ExecutionPlan, *, artifacts: dict[str, str] | None = None) -> dict[str, Any]:
    def _scoped_positions_payload(items: list[ScopedBrokerPosition]) -> list[dict[str, Any]]:
        return [
            {
                "classification_reason": item.classification_reason,
                "instrument_type": item.instrument_type,
                "price": item.price,
                "scope_symbol": item.scope_symbol,
                "shares": item.shares,
                "symbol": item.symbol,
                "underlying_symbol": item.underlying_symbol,
            }
            for item in items
        ]

    return {
        "account_scope": plan.account_scope,
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
                    "instrument_type": position.instrument_type,
                    "price": position.price,
                    "shares": position.shares,
                    "symbol": position.symbol,
                    "underlying_symbol": position.underlying_symbol,
                }
                for symbol, position in sorted(plan.broker_snapshot.positions.items())
            ],
        },
        "dry_run": plan.dry_run,
        "generated_at_chicago": plan.generated_at_chicago,
        "sizing": _sizing_payload(plan.sizing),
        "managed_supported_positions": _scoped_positions_payload(plan.managed_supported_positions),
        "managed_symbols_universe": list(plan.managed_symbols_universe),
        "managed_unsupported_positions": _scoped_positions_payload(plan.managed_unsupported_positions),
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
        "plan_math_scope": plan.plan_math_scope,
        "schema_name": "execution_plan",
        "schema_version": 2,
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
        "unmanaged_holdings_acknowledged": plan.unmanaged_holdings_acknowledged,
        "unmanaged_positions": _scoped_positions_payload(plan.unmanaged_positions),
        "warnings": list(plan.warnings),
    }


def build_order_intent_export(plan: ExecutionPlan) -> OrderIntentExport:
    if plan.blockers:
        raise ValueError(
            "Order intent export refused because execution plan has blockers: " + ", ".join(plan.blockers)
        )

    intents: list[OrderIntent] = []
    for item in plan.items:
        side = ORDER_INTENT_SIDE_BY_CLASSIFICATION.get(item.classification)
        if side is None:
            continue
        quantity = abs(item.delta_shares)
        if quantity <= 0:
            continue
        intents.append(
            OrderIntent(
                event_id=plan.signal.event_id,
                strategy=plan.signal.strategy,
                symbol=item.symbol,
                side=side,
                quantity=quantity,
                reference_price=item.reference_price,
                estimated_notional=item.estimated_notional,
                classification=item.classification,
                current_broker_shares=item.current_broker_shares,
                desired_target_shares=item.desired_target_shares,
                blockers=list(item.blockers),
                warnings=list(item.warnings),
            )
        )

    return OrderIntentExport(
        generated_at_chicago=plan.generated_at_chicago,
        dry_run=plan.dry_run,
        source_kind=plan.source_kind,
        source_label=plan.source_label,
        source_ref=plan.source_ref,
        broker_name=plan.broker_snapshot.broker_name,
        account_id=plan.broker_snapshot.account_id,
        broker_source_ref=plan.broker_source_ref,
        account_scope=plan.account_scope,
        plan_math_scope=plan.plan_math_scope,
        sizing=plan.sizing,
        managed_symbols_universe=list(plan.managed_symbols_universe),
        blockers=list(plan.blockers),
        warnings=list(plan.warnings),
        unmanaged_holdings_acknowledged=plan.unmanaged_holdings_acknowledged,
        unmanaged_positions_count=len(plan.unmanaged_positions),
        unmanaged_positions_summary=list(plan.unmanaged_positions),
        intents=intents,
    )


def build_simulated_submission_export(export: OrderIntentExport) -> SimulatedSubmissionExport:
    orders = [
        SimulatedOrderRequest(
            account_id=export.account_id,
            broker_name=export.broker_name,
            symbol=intent.symbol,
            side=intent.side,
            quantity=intent.quantity,
            order_type=SIMULATED_ORDER_TYPE,
            time_in_force=SIMULATED_TIME_IN_FORCE,
            strategy=intent.strategy,
            event_id=intent.event_id,
            reference_price=intent.reference_price,
            estimated_notional=intent.estimated_notional,
            classification=intent.classification,
            blockers=list(intent.blockers),
            warnings=list(intent.warnings),
        )
        for intent in export.intents
    ]

    return SimulatedSubmissionExport(
        generated_at_chicago=export.generated_at_chicago,
        dry_run=export.dry_run,
        source_kind=export.source_kind,
        source_label=export.source_label,
        source_ref=export.source_ref,
        broker_name=export.broker_name,
        account_id=export.account_id,
        broker_source_ref=export.broker_source_ref,
        account_scope=export.account_scope,
        plan_math_scope=export.plan_math_scope,
        sizing=export.sizing,
        managed_symbols_universe=list(export.managed_symbols_universe),
        blockers=list(export.blockers),
        warnings=list(export.warnings),
        unmanaged_holdings_acknowledged=export.unmanaged_holdings_acknowledged,
        unmanaged_positions_count=export.unmanaged_positions_count,
        unmanaged_positions_summary=list(export.unmanaged_positions_summary),
        orders=orders,
    )


def order_intent_export_to_dict(
    export: OrderIntentExport,
    *,
    artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "account_scope": export.account_scope,
        "account_id": export.account_id,
        "artifacts": artifacts or {},
        "blockers": list(export.blockers),
        "broker_name": export.broker_name,
        "broker_source_ref": export.broker_source_ref,
        "dry_run": export.dry_run,
        "generated_at_chicago": export.generated_at_chicago,
        "intents": [
            {
                "blockers": list(intent.blockers),
                "classification": intent.classification,
                "current_broker_shares": intent.current_broker_shares,
                "desired_target_shares": intent.desired_target_shares,
                "estimated_notional": intent.estimated_notional,
                "event_id": intent.event_id,
                "quantity": intent.quantity,
                "reference_price": intent.reference_price,
                "side": intent.side,
                "strategy": intent.strategy,
                "symbol": intent.symbol,
                "warnings": list(intent.warnings),
            }
            for intent in export.intents
        ],
        "managed_symbols_universe": list(export.managed_symbols_universe),
        "plan_math_scope": export.plan_math_scope,
        "schema_name": "order_intent_export",
        "schema_version": 1,
        "sizing": _sizing_payload(export.sizing),
        "source": {
            "kind": export.source_kind,
            "label": export.source_label,
            "ref": export.source_ref,
        },
        "unmanaged_holdings_acknowledged": export.unmanaged_holdings_acknowledged,
        "unmanaged_positions_count": export.unmanaged_positions_count,
        "unmanaged_positions_summary": [
            {
                "classification_reason": position.classification_reason,
                "instrument_type": position.instrument_type,
                "price": position.price,
                "scope_symbol": position.scope_symbol,
                "shares": position.shares,
                "symbol": position.symbol,
                "underlying_symbol": position.underlying_symbol,
            }
            for position in export.unmanaged_positions_summary
        ],
        "warnings": list(export.warnings),
    }


def simulated_submission_export_to_dict(
    export: SimulatedSubmissionExport,
    *,
    artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "account_id": export.account_id,
        "account_scope": export.account_scope,
        "artifacts": artifacts or {},
        "blockers": list(export.blockers),
        "broker_name": export.broker_name,
        "broker_source_ref": export.broker_source_ref,
        "dry_run": export.dry_run,
        "generated_at_chicago": export.generated_at_chicago,
        "managed_symbols_universe": list(export.managed_symbols_universe),
        "orders": [
            {
                "account_id": order.account_id,
                "blockers": list(order.blockers),
                "broker_name": order.broker_name,
                "classification": order.classification,
                "estimated_notional": order.estimated_notional,
                "event_id": order.event_id,
                "order_type": order.order_type,
                "quantity": order.quantity,
                "reference_price": order.reference_price,
                "side": order.side,
                "strategy": order.strategy,
                "symbol": order.symbol,
                "time_in_force": order.time_in_force,
                "warnings": list(order.warnings),
            }
            for order in export.orders
        ],
        "plan_math_scope": export.plan_math_scope,
        "schema_name": "simulated_submission_export",
        "schema_version": 1,
        "sizing": _sizing_payload(export.sizing),
        "source": {
            "kind": export.source_kind,
            "label": export.source_label,
            "ref": export.source_ref,
        },
        "unmanaged_holdings_acknowledged": export.unmanaged_holdings_acknowledged,
        "unmanaged_positions_count": export.unmanaged_positions_count,
        "unmanaged_positions_summary": [
            {
                "classification_reason": position.classification_reason,
                "instrument_type": position.instrument_type,
                "price": position.price,
                "scope_symbol": position.scope_symbol,
                "shares": position.shares,
                "symbol": position.symbol,
                "underlying_symbol": position.underlying_symbol,
            }
            for position in export.unmanaged_positions_summary
        ],
        "warnings": list(export.warnings),
    }
