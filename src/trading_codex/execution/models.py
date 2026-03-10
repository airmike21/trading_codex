from __future__ import annotations

from dataclasses import dataclass
from typing import Any


PLAN_CLASSIFICATIONS = ("BUY", "SELL", "RESIZE_BUY", "RESIZE_SELL", "HOLD", "EXIT")
ACCOUNT_SCOPES = ("full_account", "managed_sleeve")
ORDER_INTENT_SIDES = ("BUY", "SELL")
SIZING_MODES = ("signal_target_shares", "sleeve_capital", "account_capital")


@dataclass(frozen=True)
class SignalPayload:
    date: str
    strategy: str
    action: str
    symbol: str
    price: float | None
    target_shares: int
    resize_prev_shares: int | None
    resize_new_shares: int | None
    next_rebalance: str | None
    event_id: str
    raw: dict[str, Any]

    @property
    def desired_target_shares(self) -> int:
        if self.resize_new_shares is not None:
            return self.resize_new_shares
        return self.target_shares


@dataclass(frozen=True)
class SizingContext:
    mode: str
    baseline_signal_capital: float | None
    capital_input: float | None
    reserve_cash_pct: float
    max_allocation_pct: float
    usable_capital: float | None
    inferred_signal_allocation_pct: float | None
    applied_allocation_pct: float | None


@dataclass(frozen=True)
class BrokerPosition:
    symbol: str
    shares: int
    price: float | None
    instrument_type: str | None
    underlying_symbol: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class BrokerSnapshot:
    broker_name: str
    account_id: str | None
    as_of: str | None
    cash: float | None
    buying_power: float | None
    positions: dict[str, BrokerPosition]
    raw: dict[str, Any]


@dataclass(frozen=True)
class PlanItem:
    symbol: str
    desired_target_shares: int
    current_broker_shares: int
    delta_shares: int
    classification: str
    reference_price: float | None
    estimated_notional: float | None
    warnings: list[str]
    blockers: list[str]


@dataclass(frozen=True)
class ScopedBrokerPosition:
    symbol: str
    scope_symbol: str
    shares: int
    price: float | None
    instrument_type: str | None
    underlying_symbol: str | None
    classification_reason: str


@dataclass(frozen=True)
class ExecutionPlan:
    generated_at_chicago: str
    dry_run: bool
    account_scope: str
    plan_math_scope: str
    managed_symbols_universe: list[str]
    unmanaged_holdings_acknowledged: bool
    source_kind: str
    source_label: str
    source_ref: str | None
    broker_source_ref: str | None
    signal: SignalPayload
    sizing: SizingContext
    broker_snapshot: BrokerSnapshot
    managed_supported_positions: list[ScopedBrokerPosition]
    managed_unsupported_positions: list[ScopedBrokerPosition]
    unmanaged_positions: list[ScopedBrokerPosition]
    items: list[PlanItem]
    total_buy_notional: float
    total_sell_notional: float
    net_notional: float
    warnings: list[str]
    blockers: list[str]


@dataclass(frozen=True)
class OrderIntent:
    event_id: str
    strategy: str
    symbol: str
    side: str
    quantity: int
    reference_price: float | None
    estimated_notional: float | None
    classification: str
    current_broker_shares: int
    desired_target_shares: int
    blockers: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class OrderIntentExport:
    generated_at_chicago: str
    dry_run: bool
    source_kind: str
    source_label: str
    source_ref: str | None
    broker_name: str
    account_id: str | None
    broker_source_ref: str | None
    account_scope: str
    plan_math_scope: str
    sizing: SizingContext
    managed_symbols_universe: list[str]
    blockers: list[str]
    warnings: list[str]
    unmanaged_holdings_acknowledged: bool
    unmanaged_positions_count: int
    unmanaged_positions_summary: list[ScopedBrokerPosition]
    intents: list[OrderIntent]


@dataclass(frozen=True)
class SimulatedOrderRequest:
    account_id: str | None
    broker_name: str
    symbol: str
    side: str
    quantity: int
    order_type: str
    time_in_force: str
    strategy: str
    event_id: str
    reference_price: float | None
    estimated_notional: float | None
    classification: str
    blockers: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class SimulatedSubmissionExport:
    generated_at_chicago: str
    dry_run: bool
    source_kind: str
    source_label: str
    source_ref: str | None
    broker_name: str
    account_id: str | None
    broker_source_ref: str | None
    account_scope: str
    plan_math_scope: str
    sizing: SizingContext
    managed_symbols_universe: list[str]
    blockers: list[str]
    warnings: list[str]
    unmanaged_holdings_acknowledged: bool
    unmanaged_positions_count: int
    unmanaged_positions_summary: list[ScopedBrokerPosition]
    orders: list[SimulatedOrderRequest]
