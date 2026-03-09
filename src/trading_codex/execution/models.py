from __future__ import annotations

from dataclasses import dataclass
from typing import Any


PLAN_CLASSIFICATIONS = ("BUY", "SELL", "RESIZE_BUY", "RESIZE_SELL", "HOLD", "EXIT")


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
class BrokerPosition:
    symbol: str
    shares: int
    price: float | None
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
class ExecutionPlan:
    generated_at_chicago: str
    dry_run: bool
    source_kind: str
    source_label: str
    source_ref: str | None
    broker_source_ref: str | None
    signal: SignalPayload
    broker_snapshot: BrokerSnapshot
    items: list[PlanItem]
    total_buy_notional: float
    total_sell_notional: float
    net_notional: float
    warnings: list[str]
    blockers: list[str]
