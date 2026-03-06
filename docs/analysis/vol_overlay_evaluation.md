# Volatility Overlay Evaluation Pack

- CSV: `docs/analysis/vol_overlay_evaluation.csv`
- Periods: full history and trailing 5 years from the full-history run.
- Grid: target_vol in [0.08, 0.1, 0.12], vol_lookback in [21, 63, 126], min_leverage=0.0, max_leverage=1.0.
- Baseline rows use a scalar leverage of 1.0 because the overlay is disabled.

## Best Configuration By Strategy

### dual_mom

| config label | target vol | vol lookback | mean rank | sharpe | calmar | cagr | max drawdown |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tv_0.12_lb_21 | 0.1200 | 21.0000 | 2.8000 | 0.6053 | 0.3729 | 0.0699 | -0.1876 |
| tv_0.10_lb_21 | 0.1000 | 21.0000 | 3.0000 | 0.6040 | 0.3750 | 0.0612 | -0.1631 |
| tv_0.08_lb_21 | 0.0800 | 21.0000 | 4.6000 | 0.5899 | 0.3609 | 0.0499 | -0.1384 |
| baseline |  |  | 4.6000 | 0.5731 | 0.3033 | 0.0923 | -0.3044 |
| tv_0.12_lb_63 | 0.1200 | 63.0000 | 5.0000 | 0.5573 | 0.3123 | 0.0640 | -0.2040 |

Baseline vs best overlay by period:

| config label | period | cagr | annualized vol | sharpe | max drawdown | calmar | total return | average leverage | trade count |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | full | 0.1115 | 0.1852 | 0.6639 | -0.3044 | 0.3663 | 4.5058 | 1.0000 | 42 |
| baseline | recent_5y | 0.0732 | 0.1800 | 0.4822 | -0.3044 | 0.2403 | 0.4218 | 1.0000 | 13 |
| tv_0.12_lb_21 | full | 0.0808 | 0.1262 | 0.6793 | -0.1876 | 0.4309 | 2.5047 | 0.7792 | 42 |
| tv_0.12_lb_21 | recent_5y | 0.0591 | 0.1220 | 0.5313 | -0.1876 | 0.3148 | 0.3311 | 0.7450 | 13 |

Recommendation for default overlay: **YES**. Best overlay candidate is `tv_0.12_lb_21` against baseline `baseline`.

### valmom_v1

| config label | target vol | vol lookback | mean rank | sharpe | calmar | cagr | max drawdown |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tv_0.12_lb_21 | 0.1200 | 21.0000 | 2.6000 | 0.5685 | 0.3339 | 0.0623 | -0.1865 |
| baseline |  |  | 2.8000 | 0.5846 | 0.3351 | 0.0787 | -0.2495 |
| tv_0.10_lb_21 | 0.1000 | 21.0000 | 3.8000 | 0.5205 | 0.2820 | 0.0512 | -0.1815 |
| tv_0.12_lb_63 | 0.1200 | 63.0000 | 4.6000 | 0.5078 | 0.2905 | 0.0553 | -0.1971 |
| tv_0.08_lb_63 | 0.0800 | 63.0000 | 5.6000 | 0.5086 | 0.2681 | 0.0433 | -0.1673 |

Baseline vs best overlay by period:

| config label | period | cagr | annualized vol | sharpe | max drawdown | calmar | total return | average leverage | trade count |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | full | 0.0644 | 0.1576 | 0.4754 | -0.2927 | 0.2200 | 0.9839 | 1.0000 | 75 |
| baseline | recent_5y | 0.0929 | 0.1428 | 0.6939 | -0.2064 | 0.4503 | 0.5571 | 1.0000 | 33 |
| tv_0.12_lb_21 | full | 0.0556 | 0.1194 | 0.5133 | -0.1865 | 0.2982 | 0.8113 | 0.8513 | 75 |
| tv_0.12_lb_21 | recent_5y | 0.0689 | 0.1181 | 0.6236 | -0.1865 | 0.3696 | 0.3940 | 0.8359 | 33 |

Recommendation for default overlay: **NO**. Best overlay candidate is `tv_0.12_lb_21` against baseline `baseline`.

## Overall Recommendation

- overlay default for dual_mom: **yes**
- overlay default for valmom_v1: **no**
- single recommended default parameter set: `target_vol=0.12`, `vol_lookback=21`

## Preset Naming Proposal

- Keep the existing preset name where the overlay becomes the default.
- Add an explicit raw/no-overlay opt-out preset if needed, for example `vm_core_raw` or `dual_mom_core_raw`.

## Notes

- `vm_core_due` was not evaluated separately because it shares the same run_backtest args as `vm_core`; due mode affects alerting, not backtest returns.
- Recommendations are conservative: the overlay is only a default candidate when the best overlay beats baseline on average rank and does not weaken the full-history drawdown/Calmar profile.
