"""Stage 2 control-plane mappings for the primary live candidate and shadow bench."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from trading_codex.shadow.template import (
    ShadowStrategyTemplate,
    build_local_shadow_template,
    build_primary_live_candidate_v1_etf_rotation_shadow_template,
    build_primary_live_candidate_v1_vol_managed_shadow_template,
)
from trading_codex.strategies.xsmom_v1 import CrossSectionalMomentumV1Strategy
from trading_codex.strategies.dual_mom_v1 import DualMomentumV1Strategy

PRIMARY_LIVE_CANDIDATE_V1_ID = "primary_live_candidate_v1"
PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STRATEGY = "dual_mom_vol10_cash"
PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_PRESET = "dual_mom_vol10_cash_core"
PRIMARY_LIVE_CANDIDATE_V1_RUNTIME_STATE_KEY = PRIMARY_LIVE_CANDIDATE_V1_ID

PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_TOP_N = 1
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID = "primary_live_candidate_v1_vol_managed"
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_STRATEGY = "dual_mom_v1"
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_LABEL = "dual_mom_v1_shadow_impl"
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS = ("SPY", "QQQ", "IWM", "EFA")
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL = "BIL"
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK = 63
PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_REBALANCE = 21
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_TARGET_VOL = 0.10
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_LOOKBACK = 20
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MIN = 0.0
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MAX = 1.0
PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_UPDATE = "rebalance"
PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_ID = "primary_live_candidate_v1_etf_rotation"
PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_FAMILY_ID = PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_ID
PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_IMPLEMENTATION_STRATEGY = "xsmom_v1"
PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_IMPLEMENTATION_LABEL = "xsmom_v1_shadow_impl"
PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_DEFAULT_REBALANCE = "M"


@dataclass(frozen=True)
class ControlPlaneStrategyMapping:
    strategy_id: str
    runtime_strategy: str
    default_preset: str | None = None
    default_state_key: str | None = None


@dataclass(frozen=True)
class ShadowStrategyRuntimeConfig:
    strategy_id: str
    template_family_id: str
    primary_candidate_mapping: ControlPlaneStrategyMapping
    implementation_strategy: str
    implementation_label: str
    risk_symbols: tuple[str, ...]
    defensive_symbol: str
    momentum_lookback: int
    top_n: int
    rebalance: int | str
    vol_target: float | None
    vol_lookback: int
    vol_min: float = 0.0
    vol_max: float = 1.0
    vol_update: str = "rebalance"

    def build_strategy(self) -> DualMomentumV1Strategy | CrossSectionalMomentumV1Strategy:
        if self.implementation_strategy == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_IMPLEMENTATION_STRATEGY:
            return DualMomentumV1Strategy(
                symbols=self.risk_symbols,
                lookback=self.momentum_lookback,
                top_n=self.top_n,
                rebalance=int(self.rebalance),
                defensive_symbol=self.defensive_symbol,
            )
        if self.implementation_strategy == PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_IMPLEMENTATION_STRATEGY:
            return CrossSectionalMomentumV1Strategy(
                symbols=self.risk_symbols,
                lookback=self.momentum_lookback,
                top_n=self.top_n,
                rebalance=str(self.rebalance),
                defensive=self.defensive_symbol,
            )
        raise ValueError(
            f"Unsupported Stage 2 shadow implementation {self.implementation_strategy!r}."
        )

    def build_shadow_template(self) -> ShadowStrategyTemplate:
        return build_shadow_template_for_strategy(
            self.strategy_id,
            template_family_id=self.template_family_id,
            defensive_symbol=self.defensive_symbol,
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
    strategy_id: str = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
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
        strategy_id=_normalize_strategy_id(strategy_id, field_name="strategy_id"),
        template_family_id=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID,
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


def primary_live_candidate_v1_etf_rotation_shadow_config(
    *,
    strategy_id: str = PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_ID,
    symbols: Iterable[str] = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS,
    defensive_symbol: str = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL,
    momentum_lookback: int = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK,
    top_n: int = PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_TOP_N,
    rebalance: str = PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_DEFAULT_REBALANCE,
    vol_target: float | None = None,
    vol_lookback: int = 63,
    vol_min: float = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MIN,
    vol_max: float = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MAX,
    vol_update: str = PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_UPDATE,
) -> ShadowStrategyRuntimeConfig:
    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        raise ValueError("primary_live_candidate_v1_etf_rotation requires at least one risk symbol.")

    rendered_defensive_symbol = str(defensive_symbol).strip().upper()
    if not rendered_defensive_symbol:
        raise ValueError("primary_live_candidate_v1_etf_rotation defensive_symbol must not be empty.")

    rendered_rebalance = str(rebalance).strip().upper()
    if rendered_rebalance not in {"M", "W"}:
        raise ValueError("primary_live_candidate_v1_etf_rotation rebalance must be 'M' or 'W'.")

    return ShadowStrategyRuntimeConfig(
        strategy_id=_normalize_strategy_id(strategy_id, field_name="strategy_id"),
        template_family_id=PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_FAMILY_ID,
        primary_candidate_mapping=primary_live_candidate_v1_runtime_mapping(),
        implementation_strategy=PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_IMPLEMENTATION_STRATEGY,
        implementation_label=PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_IMPLEMENTATION_LABEL,
        risk_symbols=normalized_symbols,
        defensive_symbol=rendered_defensive_symbol,
        momentum_lookback=int(momentum_lookback),
        top_n=int(top_n),
        rebalance=rendered_rebalance,
        vol_target=None if vol_target is None else float(vol_target),
        vol_lookback=int(vol_lookback),
        vol_min=float(vol_min),
        vol_max=float(vol_max),
        vol_update=str(vol_update),
    )


def resolve_shadow_runtime_config(
    family_id: str,
    *,
    strategy_id: str | None = None,
    symbols: Iterable[str] | None = None,
    defensive_symbol: str | None = None,
    momentum_lookback: int | None = None,
    top_n: int | None = None,
    rebalance: int | None = None,
    vol_target: float | None = None,
    vol_lookback: int | None = None,
    vol_min: float | None = None,
    vol_max: float | None = None,
    vol_update: str | None = None,
) -> ShadowStrategyRuntimeConfig:
    normalized_family_id = _normalize_strategy_id(family_id, field_name="family_id")
    if normalized_family_id == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID:
        return primary_live_candidate_v1_vol_managed_shadow_config(
            strategy_id=(
                strategy_id
                if strategy_id is not None
                else PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
            ),
            symbols=(
                PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS
                if symbols is None
                else symbols
            ),
            defensive_symbol=(
                PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL
                if defensive_symbol is None
                else defensive_symbol
            ),
            momentum_lookback=(
                PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK
                if momentum_lookback is None
                else momentum_lookback
            ),
            top_n=PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_TOP_N if top_n is None else top_n,
            rebalance=PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_REBALANCE if rebalance is None else rebalance,
            vol_target=(
                PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_TARGET_VOL
                if vol_target is None
                else vol_target
            ),
            vol_lookback=(
                PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_LOOKBACK
                if vol_lookback is None
                else vol_lookback
            ),
            vol_min=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MIN if vol_min is None else vol_min,
            vol_max=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MAX if vol_max is None else vol_max,
            vol_update=(
                PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_UPDATE
                if vol_update is None
                else vol_update
            ),
        )
    if normalized_family_id == PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_FAMILY_ID:
        return primary_live_candidate_v1_etf_rotation_shadow_config(
            strategy_id=(
                strategy_id
                if strategy_id is not None
                else PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_ID
            ),
            symbols=(
                PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_RISK_SYMBOLS
                if symbols is None
                else symbols
            ),
            defensive_symbol=(
                PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL
                if defensive_symbol is None
                else defensive_symbol
            ),
            momentum_lookback=(
                PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_MOMENTUM_LOOKBACK
                if momentum_lookback is None
                else momentum_lookback
            ),
            top_n=PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_TOP_N if top_n is None else top_n,
            rebalance=(
                PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_DEFAULT_REBALANCE
                if rebalance is None
                else str(rebalance)
            ),
            vol_target=vol_target,
            vol_lookback=63 if vol_lookback is None else vol_lookback,
            vol_min=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MIN if vol_min is None else vol_min,
            vol_max=PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_MAX if vol_max is None else vol_max,
            vol_update=(
                PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_DEFAULT_VOL_UPDATE
                if vol_update is None
                else vol_update
            ),
        )

    raise ValueError(f"Unsupported Stage 2 shadow family {normalized_family_id!r}.")


def build_shadow_template_for_strategy(
    strategy_id: str,
    *,
    template_family_id: str | None = None,
    defensive_symbol: str | None = None,
) -> ShadowStrategyTemplate:
    normalized_strategy_id = _normalize_strategy_id(strategy_id, field_name="strategy_id")
    family_id = normalized_strategy_id if template_family_id is None else _normalize_strategy_id(
        template_family_id,
        field_name="template_family_id",
    )
    if family_id == PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_FAMILY_ID:
        return build_primary_live_candidate_v1_vol_managed_shadow_template(
            strategy_id=normalized_strategy_id,
            defensive_symbols=((defensive_symbol or PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL), "CASH"),
        )
    if family_id == PRIMARY_LIVE_CANDIDATE_V1_ETF_ROTATION_FAMILY_ID:
        return build_primary_live_candidate_v1_etf_rotation_shadow_template(
            strategy_id=normalized_strategy_id,
            defensive_symbols=((defensive_symbol or PRIMARY_LIVE_CANDIDATE_V1_DEFAULT_DEFENSIVE_SYMBOL), "CASH"),
        )
    return build_local_shadow_template(normalized_strategy_id)


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


def _normalize_strategy_id(strategy_id: str, *, field_name: str) -> str:
    rendered = str(strategy_id).strip()
    if not rendered:
        raise ValueError(f"{field_name} must not be empty.")
    return rendered
