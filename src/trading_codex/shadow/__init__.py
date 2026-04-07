"""Shadow-only reusable strategy packaging and risk invariants."""

from trading_codex.shadow.risk_invariants import (
    DrawdownKillSwitchConfig,
    LiquidityCheckConfig,
    PositionCapConfig,
    RegimeGuardrailConfig,
    RiskInvariantConfig,
    RiskInvariantReport,
    TurnoverCapConfig,
    evaluate_risk_invariants,
)
from trading_codex.shadow.template import (
    ShadowStrategyOutputs,
    ShadowStrategyTemplate,
    build_local_shadow_template,
    build_primary_live_candidate_v1_vol_managed_shadow_template,
)

__all__ = [
    "DrawdownKillSwitchConfig",
    "LiquidityCheckConfig",
    "PositionCapConfig",
    "RegimeGuardrailConfig",
    "RiskInvariantConfig",
    "RiskInvariantReport",
    "TurnoverCapConfig",
    "ShadowStrategyOutputs",
    "ShadowStrategyTemplate",
    "build_local_shadow_template",
    "build_primary_live_candidate_v1_vol_managed_shadow_template",
    "evaluate_risk_invariants",
]
