"""Standard shadow-strategy packaging for signal, weights, diagnostics, and reports."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from trading_codex.backtest.shadow_artifacts import (
    build_shadow_review_bundle,
    render_shadow_review_markdown,
)
from trading_codex.shadow.risk_invariants import (
    DrawdownKillSwitchConfig,
    LiquidityCheckConfig,
    PositionCapConfig,
    RegimeGuardrailConfig,
    RiskInvariantConfig,
    TurnoverCapConfig,
    evaluate_risk_invariants,
)

_WEIGHT_EPSILON = 1e-12


@dataclass(frozen=True)
class ShadowStrategyOutputs:
    signal: dict[str, Any]
    target_weights: dict[str, Any]
    diagnostics: dict[str, Any]
    reports: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal": dict(self.signal),
            "target_weights": dict(self.target_weights),
            "diagnostics": dict(self.diagnostics),
            "reports": dict(self.reports),
        }


@dataclass(frozen=True)
class ShadowStrategyTemplate:
    strategy_id: str
    invariant_config: RiskInvariantConfig

    def build_outputs(
        self,
        *,
        bars: pd.DataFrame,
        weights: pd.Series | pd.DataFrame,
        turnover: pd.Series,
        equity: pd.Series,
        next_action_payload: Mapping[str, Any],
        metrics_summary: Mapping[str, float],
        cost_assumptions: Mapping[str, float],
        actions: Sequence[Mapping[str, Any]] | None = None,
        expected_symbol_count: int | None = None,
        actual_symbol_count: int | None = None,
        leverage: float | None = None,
        vol_target: float | None = None,
        realized_vol: float | None = None,
    ) -> ShadowStrategyOutputs:
        signal_payload = dict(next_action_payload)
        signal_payload["shadow_strategy_id"] = self.strategy_id

        weight_frame = _weight_frame(
            weights,
            symbol_hint=str(signal_payload.get("symbol") or ""),
        )
        invariant_report = evaluate_risk_invariants(
            bars=bars,
            weights=weight_frame,
            turnover=turnover,
            equity=equity,
            config=self.invariant_config,
            symbol_hint=str(signal_payload.get("symbol") or ""),
        )
        invariant_payload = invariant_report.as_dict()
        target_weight_payload = _target_weight_payload(weight_frame, turnover)

        rendered_actions = [dict(item) for item in actions] if actions is not None else [signal_payload]
        bundle = build_shadow_review_bundle(
            strategy=self.strategy_id,
            as_of_date=str(signal_payload.get("date")),
            next_rebalance=(
                None
                if signal_payload.get("next_rebalance") is None
                else str(signal_payload.get("next_rebalance"))
            ),
            actions=rendered_actions,
            cost_assumptions=dict(cost_assumptions),
            metrics=dict(metrics_summary),
            leverage=leverage,
            vol_target=vol_target,
            realized_vol=realized_vol,
            warnings=[],
            blockers=[],
            expected_symbol_count=expected_symbol_count,
            actual_symbol_count=actual_symbol_count,
            extra_warning_reasons=invariant_payload["warning_reasons"],
            extra_blocking_reasons=invariant_payload["blocking_reasons"],
            risk_invariants=invariant_payload,
            shadow_strategy_id=self.strategy_id,
        )
        markdown = render_shadow_review_markdown(bundle)

        diagnostics = {
            "shadow_strategy_id": self.strategy_id,
            "as_of_date": target_weight_payload["as_of_date"],
            "metrics": dict(metrics_summary),
            "risk_invariants": invariant_payload,
            "expected_symbol_count": expected_symbol_count,
            "actual_symbol_count": actual_symbol_count,
        }
        reports = {
            "shadow_review_bundle": bundle,
            "shadow_review_markdown": markdown,
            "review_summary": dict(bundle.get("review_summary", {})),
        }
        return ShadowStrategyOutputs(
            signal=signal_payload,
            target_weights=target_weight_payload,
            diagnostics=diagnostics,
            reports=reports,
        )


def build_local_shadow_template(
    strategy_id: str,
) -> ShadowStrategyTemplate:
    return ShadowStrategyTemplate(
        strategy_id=strategy_id,
        invariant_config=RiskInvariantConfig(
            position_caps=PositionCapConfig(max_abs_weight=1.0),
            turnover_caps=TurnoverCapConfig(max_turnover=2.0),
            liquidity_checks=LiquidityCheckConfig(
                lookback=20,
                min_avg_dollar_volume=50_000.0,
                max_target_adv_fraction=0.20,
            ),
        ),
    )


def build_primary_live_candidate_v1_vol_managed_shadow_template(
    *,
    strategy_id: str = "primary_live_candidate_v1_vol_managed",
    defensive_symbols: Iterable[str] = ("SHY", "BIL", "CASH"),
    gate_symbol: str = "SPY",
    gate_sma_window: int = 200,
    max_drawdown: float = -0.20,
) -> ShadowStrategyTemplate:
    normalized_defensives = _normalize_symbols(defensive_symbols)
    if "CASH" not in normalized_defensives:
        normalized_defensives = (*normalized_defensives, "CASH")
    return ShadowStrategyTemplate(
        strategy_id=strategy_id,
        invariant_config=RiskInvariantConfig(
            position_caps=PositionCapConfig(max_abs_weight=1.0),
            turnover_caps=TurnoverCapConfig(max_turnover=2.0),
            liquidity_checks=LiquidityCheckConfig(
                lookback=20,
                min_avg_dollar_volume=5_000_000.0,
                max_target_adv_fraction=0.05,
            ),
            drawdown_kill_switch=DrawdownKillSwitchConfig(
                max_drawdown=float(max_drawdown),
                allowed_defensive_symbols=normalized_defensives,
            ),
            regime_guardrails=RegimeGuardrailConfig(
                gate_symbol=str(gate_symbol).strip().upper(),
                sma_window=int(gate_sma_window),
                allowed_defensive_symbols=normalized_defensives,
            ),
        ),
    )


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


def _weight_frame(
    weights: pd.Series | pd.DataFrame,
    *,
    symbol_hint: str | None = None,
) -> pd.DataFrame:
    if isinstance(weights, pd.DataFrame):
        return weights.copy().astype(float)

    column_name = symbol_hint if symbol_hint and symbol_hint.upper() != "CASH" else None
    if column_name is None:
        column_name = str(weights.name) if weights.name else "asset"
    return weights.astype(float).to_frame(name=column_name)


def _target_weight_payload(
    weights: pd.DataFrame,
    turnover: pd.Series,
) -> dict[str, Any]:
    if weights.empty:
        return {
            "as_of_date": None,
            "current": {},
            "previous": {},
            "active_symbols": ["CASH"],
            "gross_exposure": 0.0,
            "net_exposure": 0.0,
            "latest_turnover": 0.0,
        }

    current = weights.iloc[-1].fillna(0.0)
    previous = weights.iloc[-2].fillna(0.0) if len(weights) >= 2 else pd.Series(0.0, index=current.index)
    active_symbols = [
        str(symbol)
        for symbol, weight in current.items()
        if abs(float(weight)) > _WEIGHT_EPSILON
    ]
    if not active_symbols:
        active_symbols = ["CASH"]

    return {
        "as_of_date": weights.index[-1].date().isoformat(),
        "current": {str(symbol): float(weight) for symbol, weight in current.items()},
        "previous": {str(symbol): float(weight) for symbol, weight in previous.items()},
        "active_symbols": active_symbols,
        "gross_exposure": float(current.abs().sum()),
        "net_exposure": float(current.sum()),
        "latest_turnover": float(turnover.iloc[-1]) if len(turnover) else 0.0,
    }
