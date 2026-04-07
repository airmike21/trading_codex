"""Reusable risk-invariant evaluation for shadow-only strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from trading_codex.backtest.costs import MODEL_PORTFOLIO_VALUE

_WEIGHT_EPSILON = 1e-12


@dataclass(frozen=True)
class PositionCapConfig:
    max_abs_weight: float = 1.0


@dataclass(frozen=True)
class TurnoverCapConfig:
    max_turnover: float = 2.0


@dataclass(frozen=True)
class LiquidityCheckConfig:
    lookback: int = 20
    min_avg_dollar_volume: float = 1_000_000.0
    max_target_adv_fraction: float = 0.05
    model_portfolio_value: float = MODEL_PORTFOLIO_VALUE


@dataclass(frozen=True)
class DrawdownKillSwitchConfig:
    max_drawdown: float = -0.20
    allowed_defensive_symbols: tuple[str, ...] = ("CASH",)


@dataclass(frozen=True)
class RegimeGuardrailConfig:
    gate_symbol: str = "SPY"
    sma_window: int = 200
    allowed_defensive_symbols: tuple[str, ...] = ("CASH",)


@dataclass(frozen=True)
class RiskInvariantConfig:
    position_caps: PositionCapConfig | None = None
    turnover_caps: TurnoverCapConfig | None = None
    liquidity_checks: LiquidityCheckConfig | None = None
    drawdown_kill_switch: DrawdownKillSwitchConfig | None = None
    regime_guardrails: RegimeGuardrailConfig | None = None


@dataclass(frozen=True)
class InvariantCheckResult:
    name: str
    status: str
    summary: str
    details: dict[str, Any] = field(default_factory=dict)
    warning_reasons: tuple[str, ...] = ()
    blocking_reasons: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "summary": self.summary,
            "details": dict(self.details),
            "warning_reasons": list(self.warning_reasons),
            "blocking_reasons": list(self.blocking_reasons),
        }


@dataclass(frozen=True)
class RiskInvariantReport:
    checks: dict[str, InvariantCheckResult]
    warning_reasons: tuple[str, ...]
    blocking_reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        rendered_checks = {
            name: check.as_dict()
            for name, check in self.checks.items()
        }
        statuses = [check.status for check in self.checks.values()]
        return {
            "checks": rendered_checks,
            "warning_reasons": list(self.warning_reasons),
            "blocking_reasons": list(self.blocking_reasons),
            "summary": {
                "check_count": len(rendered_checks),
                "pass_count": sum(status == "pass" for status in statuses),
                "warning_count": sum(status == "warning" for status in statuses),
                "block_count": sum(status == "block" for status in statuses),
                "disabled_count": sum(status == "disabled" for status in statuses),
                "warning_reason_count": len(self.warning_reasons),
                "blocking_reason_count": len(self.blocking_reasons),
            },
        }


def _normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def _normalize_symbols(symbols: tuple[str, ...] | list[str] | set[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        rendered = _normalize_symbol(symbol)
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

    column_name = symbol_hint if symbol_hint and _normalize_symbol(symbol_hint) != "CASH" else None
    if column_name is None:
        column_name = str(weights.name) if weights.name else "asset"
    return weights.astype(float).to_frame(name=column_name)


def _field_panel(
    bars: pd.DataFrame,
    *,
    field: str,
    symbols: list[str],
    symbol_hint: str | None = None,
) -> pd.DataFrame:
    if isinstance(bars.columns, pd.MultiIndex) and bars.columns.nlevels == 2:
        panel = bars.xs(field, axis=1, level=1).astype(float)
        missing = [symbol for symbol in symbols if symbol not in panel.columns]
        if missing:
            raise ValueError(f"Bars missing {field!r} values for symbols: {missing}")
        return panel.loc[:, symbols]

    if field not in bars.columns:
        raise ValueError(f"Bars missing {field!r} column for liquidity/regime checks.")

    column_name = symbol_hint if symbol_hint and _normalize_symbol(symbol_hint) != "CASH" else None
    if column_name is None:
        column_name = symbols[0] if symbols else "asset"
    return pd.DataFrame({column_name: bars[field].astype(float)}, index=bars.index)


def _active_symbols(latest_weights: pd.Series) -> tuple[str, ...]:
    active = [
        str(symbol)
        for symbol, weight in latest_weights.items()
        if abs(float(weight)) > _WEIGHT_EPSILON
    ]
    return tuple(active)


def _latest_turnover(turnover: pd.Series) -> float:
    if turnover.empty:
        return 0.0
    return float(turnover.iloc[-1])


def _current_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running_peak = equity.cummax()
    if running_peak.empty or float(running_peak.iloc[-1]) <= 0.0:
        return 0.0
    return float((equity.iloc[-1] / running_peak.iloc[-1]) - 1.0)


def _is_defensive_compliant(
    active_symbols: tuple[str, ...],
    *,
    allowed_defensive_symbols: tuple[str, ...],
) -> bool:
    if not active_symbols:
        return True
    allowed = set(_normalize_symbols(allowed_defensive_symbols))
    return all(_normalize_symbol(symbol) in allowed for symbol in active_symbols)


def _disabled_check(name: str, summary: str) -> InvariantCheckResult:
    return InvariantCheckResult(name=name, status="disabled", summary=summary)


def _position_cap_check(
    weight_frame: pd.DataFrame,
    config: PositionCapConfig | None,
) -> InvariantCheckResult:
    name = "position_caps"
    if config is None:
        return _disabled_check(name, "Position caps not configured.")

    latest_weights = weight_frame.iloc[-1].fillna(0.0)
    max_abs_weight = float(latest_weights.abs().max()) if len(latest_weights) else 0.0
    offenders = {
        str(symbol): float(weight)
        for symbol, weight in latest_weights.items()
        if abs(float(weight)) > float(config.max_abs_weight) + _WEIGHT_EPSILON
    }
    if offenders:
        return InvariantCheckResult(
            name=name,
            status="block",
            summary=(
                f"Latest absolute weight {max_abs_weight:.4f} breached cap "
                f"{float(config.max_abs_weight):.4f}."
            ),
            details={
                "cap": float(config.max_abs_weight),
                "latest_max_abs_weight": max_abs_weight,
                "offenders": offenders,
            },
            blocking_reasons=("position_cap_breach",),
        )

    return InvariantCheckResult(
        name=name,
        status="pass",
        summary=(
            f"Latest absolute weight {max_abs_weight:.4f} stayed within cap "
            f"{float(config.max_abs_weight):.4f}."
        ),
        details={
            "cap": float(config.max_abs_weight),
            "latest_max_abs_weight": max_abs_weight,
            "offenders": {},
        },
    )


def _turnover_cap_check(turnover: pd.Series, config: TurnoverCapConfig | None) -> InvariantCheckResult:
    name = "turnover_caps"
    if config is None:
        return _disabled_check(name, "Turnover caps not configured.")

    latest_turnover = _latest_turnover(turnover)
    if latest_turnover > float(config.max_turnover) + _WEIGHT_EPSILON:
        return InvariantCheckResult(
            name=name,
            status="block",
            summary=(
                f"Latest turnover {latest_turnover:.4f} breached cap "
                f"{float(config.max_turnover):.4f}."
            ),
            details={
                "cap": float(config.max_turnover),
                "latest_turnover": latest_turnover,
            },
            blocking_reasons=("turnover_cap_breach",),
        )

    return InvariantCheckResult(
        name=name,
        status="pass",
        summary=(
            f"Latest turnover {latest_turnover:.4f} stayed within cap "
            f"{float(config.max_turnover):.4f}."
        ),
        details={
            "cap": float(config.max_turnover),
            "latest_turnover": latest_turnover,
        },
    )


def _liquidity_check(
    bars: pd.DataFrame,
    weight_frame: pd.DataFrame,
    config: LiquidityCheckConfig | None,
    *,
    symbol_hint: str | None = None,
) -> InvariantCheckResult:
    name = "liquidity_checks"
    if config is None:
        return _disabled_check(name, "Liquidity checks not configured.")

    latest_weights = weight_frame.iloc[-1].fillna(0.0)
    active_symbols = [
        str(symbol)
        for symbol, weight in latest_weights.items()
        if abs(float(weight)) > _WEIGHT_EPSILON
    ]
    if not active_symbols:
        return InvariantCheckResult(
            name=name,
            status="pass",
            summary="No active positions to validate for liquidity.",
            details={
                "lookback": int(config.lookback),
                "active_symbols": [],
                "checked_symbols": [],
            },
        )

    close_panel = _field_panel(
        bars,
        field="close",
        symbols=active_symbols,
        symbol_hint=symbol_hint,
    )
    volume_panel = _field_panel(
        bars,
        field="volume",
        symbols=active_symbols,
        symbol_hint=symbol_hint,
    )
    average_dollar_volume = (
        close_panel.mul(volume_panel)
        .rolling(window=int(config.lookback), min_periods=int(config.lookback))
        .mean()
    )
    latest_adv = average_dollar_volume.iloc[-1]

    checked_symbols: list[dict[str, Any]] = []
    offenders: list[dict[str, Any]] = []
    for symbol in active_symbols:
        target_notional = float(config.model_portfolio_value) * abs(float(latest_weights.loc[symbol]))
        avg_dollar_volume = latest_adv.get(symbol)
        avg_dollar_volume_value = (
            None if pd.isna(avg_dollar_volume) else float(avg_dollar_volume)
        )
        adv_fraction = (
            None
            if avg_dollar_volume_value is None or avg_dollar_volume_value <= 0.0
            else float(target_notional / avg_dollar_volume_value)
        )
        record = {
            "symbol": symbol,
            "target_notional": target_notional,
            "avg_dollar_volume": avg_dollar_volume_value,
            "target_adv_fraction": adv_fraction,
        }
        checked_symbols.append(record)

        if (
            avg_dollar_volume_value is None
            or avg_dollar_volume_value < float(config.min_avg_dollar_volume)
            or adv_fraction is None
            or adv_fraction > float(config.max_target_adv_fraction)
        ):
            offenders.append(record)

    if offenders:
        return InvariantCheckResult(
            name=name,
            status="block",
            summary=(
                "One or more active symbols failed the liquidity checks for average dollar "
                "volume or target ADV fraction."
            ),
            details={
                "lookback": int(config.lookback),
                "min_avg_dollar_volume": float(config.min_avg_dollar_volume),
                "max_target_adv_fraction": float(config.max_target_adv_fraction),
                "checked_symbols": checked_symbols,
                "offenders": offenders,
            },
            blocking_reasons=("liquidity_guardrail_breach",),
        )

    return InvariantCheckResult(
        name=name,
        status="pass",
        summary="All active symbols passed the liquidity checks.",
        details={
            "lookback": int(config.lookback),
            "min_avg_dollar_volume": float(config.min_avg_dollar_volume),
            "max_target_adv_fraction": float(config.max_target_adv_fraction),
            "checked_symbols": checked_symbols,
            "offenders": [],
        },
    )


def _drawdown_kill_switch_check(
    equity: pd.Series,
    weight_frame: pd.DataFrame,
    config: DrawdownKillSwitchConfig | None,
) -> InvariantCheckResult:
    name = "drawdown_kill_switch"
    if config is None:
        return _disabled_check(name, "Drawdown kill-switch not configured.")

    latest_weights = weight_frame.iloc[-1].fillna(0.0)
    active_symbols = _active_symbols(latest_weights)
    current_drawdown = _current_drawdown(equity)
    triggered = current_drawdown <= float(config.max_drawdown)
    compliant = _is_defensive_compliant(
        active_symbols,
        allowed_defensive_symbols=config.allowed_defensive_symbols,
    )

    if triggered and not compliant:
        return InvariantCheckResult(
            name=name,
            status="block",
            summary=(
                f"Current drawdown {current_drawdown:.4f} breached kill-switch "
                f"{float(config.max_drawdown):.4f} while risk exposure remained active."
            ),
            details={
                "max_drawdown": float(config.max_drawdown),
                "current_drawdown": current_drawdown,
                "triggered": True,
                "active_symbols": list(active_symbols),
                "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
            },
            blocking_reasons=("drawdown_kill_switch_breach",),
        )

    if triggered:
        return InvariantCheckResult(
            name=name,
            status="pass",
            summary=(
                f"Current drawdown {current_drawdown:.4f} breached kill-switch "
                f"{float(config.max_drawdown):.4f}, and the allocation is defensive."
            ),
            details={
                "max_drawdown": float(config.max_drawdown),
                "current_drawdown": current_drawdown,
                "triggered": True,
                "active_symbols": list(active_symbols),
                "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
            },
        )

    return InvariantCheckResult(
        name=name,
        status="pass",
        summary=(
            f"Current drawdown {current_drawdown:.4f} stayed above kill-switch "
            f"{float(config.max_drawdown):.4f}."
        ),
        details={
            "max_drawdown": float(config.max_drawdown),
            "current_drawdown": current_drawdown,
            "triggered": False,
            "active_symbols": list(active_symbols),
            "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
        },
    )


def _regime_guardrail_check(
    bars: pd.DataFrame,
    weight_frame: pd.DataFrame,
    config: RegimeGuardrailConfig | None,
) -> InvariantCheckResult:
    name = "regime_guardrails"
    if config is None:
        return _disabled_check(name, "Regime guardrails not configured.")

    gate_symbol = _normalize_symbol(config.gate_symbol)
    close_panel = _field_panel(
        bars,
        field="close",
        symbols=[gate_symbol],
        symbol_hint=gate_symbol,
    )
    gate_close = close_panel[gate_symbol].astype(float)
    gate_sma = gate_close.rolling(window=int(config.sma_window), min_periods=int(config.sma_window)).mean()
    gate_state_series = gate_close.shift(1) > gate_sma.shift(1)
    gate_state_raw = gate_state_series.iloc[-1] if len(gate_state_series) else None
    gate_state = None if pd.isna(gate_state_raw) else bool(gate_state_raw)

    latest_weights = weight_frame.iloc[-1].fillna(0.0)
    active_symbols = _active_symbols(latest_weights)
    compliant = _is_defensive_compliant(
        active_symbols,
        allowed_defensive_symbols=config.allowed_defensive_symbols,
    )
    latest_close = float(gate_close.iloc[-1]) if len(gate_close) else None
    latest_sma = float(gate_sma.iloc[-1]) if len(gate_sma) and pd.notna(gate_sma.iloc[-1]) else None

    if gate_state is None:
        return InvariantCheckResult(
            name=name,
            status="block",
            summary=(
                f"Regime guardrail for {gate_symbol} could not be evaluated because the "
                f"{int(config.sma_window)}-day SMA was unavailable."
            ),
            details={
                "gate_symbol": gate_symbol,
                "sma_window": int(config.sma_window),
                "gate_state": None,
                "latest_close": latest_close,
                "latest_sma": latest_sma,
                "active_symbols": list(active_symbols),
                "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
            },
            blocking_reasons=("regime_guardrail_data_missing",),
        )

    if not gate_state and not compliant:
        return InvariantCheckResult(
            name=name,
            status="block",
            summary=(
                f"Regime guardrail is risk-off for {gate_symbol}, but risk exposure "
                "remained active."
            ),
            details={
                "gate_symbol": gate_symbol,
                "sma_window": int(config.sma_window),
                "gate_state": "risk_off",
                "latest_close": latest_close,
                "latest_sma": latest_sma,
                "active_symbols": list(active_symbols),
                "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
            },
            blocking_reasons=("regime_guardrail_breach",),
        )

    if not gate_state:
        return InvariantCheckResult(
            name=name,
            status="pass",
            summary=f"Regime guardrail is risk-off for {gate_symbol}, and the allocation is defensive.",
            details={
                "gate_symbol": gate_symbol,
                "sma_window": int(config.sma_window),
                "gate_state": "risk_off",
                "latest_close": latest_close,
                "latest_sma": latest_sma,
                "active_symbols": list(active_symbols),
                "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
            },
        )

    return InvariantCheckResult(
        name=name,
        status="pass",
        summary=f"Regime guardrail is risk-on for {gate_symbol}.",
        details={
            "gate_symbol": gate_symbol,
            "sma_window": int(config.sma_window),
            "gate_state": "risk_on",
            "latest_close": latest_close,
            "latest_sma": latest_sma,
            "active_symbols": list(active_symbols),
            "allowed_defensive_symbols": list(_normalize_symbols(config.allowed_defensive_symbols)),
        },
    )


def evaluate_risk_invariants(
    *,
    bars: pd.DataFrame,
    weights: pd.Series | pd.DataFrame,
    turnover: pd.Series,
    equity: pd.Series,
    config: RiskInvariantConfig,
    symbol_hint: str | None = None,
) -> RiskInvariantReport:
    weight_frame = _weight_frame(weights, symbol_hint=symbol_hint)
    checks = {
        "position_caps": _position_cap_check(weight_frame, config.position_caps),
        "turnover_caps": _turnover_cap_check(turnover, config.turnover_caps),
        "liquidity_checks": _liquidity_check(
            bars,
            weight_frame,
            config.liquidity_checks,
            symbol_hint=symbol_hint,
        ),
        "drawdown_kill_switch": _drawdown_kill_switch_check(
            equity,
            weight_frame,
            config.drawdown_kill_switch,
        ),
        "regime_guardrails": _regime_guardrail_check(
            bars,
            weight_frame,
            config.regime_guardrails,
        ),
    }

    warning_reasons: list[str] = []
    blocking_reasons: list[str] = []
    for check in checks.values():
        for reason in check.warning_reasons:
            if reason not in warning_reasons:
                warning_reasons.append(reason)
        for reason in check.blocking_reasons:
            if reason not in blocking_reasons:
                blocking_reasons.append(reason)

    return RiskInvariantReport(
        checks=checks,
        warning_reasons=tuple(warning_reasons),
        blocking_reasons=tuple(blocking_reasons),
    )
