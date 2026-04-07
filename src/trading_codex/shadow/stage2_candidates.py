"""Stage 2 control-plane mappings for the primary live candidate and shadow bench."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from trading_codex.shadow.template import (
    ShadowStrategyTemplate,
    build_local_shadow_template,
    build_primary_live_candidate_v1_vol_managed_shadow_template,
)
from trading_codex.strategies.dual_mom_v1 import DualMomentumV1Strategy

PRIMARY_LIVE_CANDIDATE_V1_ID = "primary_live_candidate_v1"
PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY = "dual_mom_vol10_cash"
PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_PRESET = "dual_mom_vol10_cash_core"
PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STATE_KEY = PRIMARY_LIVE_CANDIDATE_V1_ID

PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID = "primary_live_candidate_v1_vol_managed"
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_STRATEGY = "dual_mom_v1"
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_LABEL = "dual_mom_v1_shadow_impl"
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS = ("SPY", "QQQ", "IWM", "EFA")
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL = "BIL"
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK = 63
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_TOP_N = 1
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_REBALANCE = 21
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_TARGET_VOL = 0.10
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_LOOKBACK = 20
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MIN = 0.0
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MAX = 1.0
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_UPDATE = "rebalance"


@dataclass(frozen=True)
class ControlPlaneStrategyMapping:
    strategy_id: str
    runtime_strategy: str
    default_preset: str | None = None
    default_state_key: str | None = None


@dataclass(frozen=True)
class ShadowStrategyRuntimeConfig:
    strategy_id: str
    primary_candidate_mapping: ControlPlaneStrategyMapping
    implementation_strategy: str
    implementation_label: str
    risk_symbols: tuple[str, ...]
    defensive_symbol: str
    momentum_lookback: int
    top_n: int
    rebalance: int
    vol_target: float
    vol_lookback: int
    vol_min: float = 0.0
    vol_max: float = 1.0
    vol_update: str = "rebalance"

    def build_strategy(self) -> DualMomentumV1Strategy:
        return DualMomentumV1Strategy(
            symbols=self.risk_symbols,
            lookback=self.momentum_lookback,
            top_n=self.top_n,
            rebalance=self.rebalance,
            defensive_symbol=self.defensive_symbol,
        )

    def build_shadow_template(self) -> ShadowStrategyTemplate:
        return build_primary_live_candidate_v1_vol_managed_shadow_template(
            defensive_symbols=(self.defensive_symbol, "CASH"),
        )


def primary_live_candidate_v1_runtime_mapping() -> ControlPlaneStrategyMapping:
    return ControlPlaneStrategyMapping(
        strategy_id=PRIMARY_LIVE_CANDIDATE_V1_ID,
        runtime_strategy=PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY,
        default_preset=PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_PRESET,
        default_state_key=PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STATE_KEY,
    )


def primary_live_candidate_v1_vol_managed_shadow_config(
    *,
    symbols: Iterable[str] = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS,
    defensive_symbol: str = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL,
    momentum_lookback: int = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK,
    top_n: int = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_TOP_N,
    rebalance: int = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_REBALANCE,
    vol_target: float = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_TARGET_VOL,
    vol_lookback: int = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_LOOKBACK,
    vol_min: float = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MIN,
    vol_max: float = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MAX,
    vol_update: str = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_UPDATE,
) -> ShadowStrategyRuntimeConfig:
    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        raise ValueError("primary_live_candidate_v1_vol_managed requires at least one risk symbol.")

    rendered_defensive_symbol = str(defensive_symbol).strip().upper()
    if not rendered_defensive_symbol:
        raise ValueError("primary_live_candidate_v1_vol_managed defensive_symbol must not be empty.")

    return ShadowStrategyRuntimeConfig(
        strategy_id=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
        primary_candidate_mapping=primary_live_candidate_v1_runtime_mapping(),
        implementation_strategy=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_STRATEGY,
        implementation_label=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_LABEL,
        risk_symbols=normalized_symbols,
        defensive_symbol=rendered_defensive_symbol,
        momentum_lookback=int(momentum_lookback),
        top_n=int(top_n),
        rebalance=int(rebalance),
        vol_target=float(vol_target),
        vol_lookback=int(vol_lookback),
        vol_min=float(vol_min),
        vol_max=float(vol_max),
        vol_update=str(vol_update),
    )


def build_shadow_template_for_strategy(
    strategy_id: str,
    *,
    defensive_symbol: str | None = None,
) -> ShadowStrategyTemplate:
    if str(strategy_id) == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID:
        shadow_config = primary_live_candidate_v1_vol_managed_shadow_config(
            defensive_symbol=defensive_symbol or PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL,
        )
        return shadow_config.build_shadow_template()
    return build_local_shadow_template(str(strategy_id))


def _normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        rendered = str(symbol).strip().upper()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return tuple(normalized)
