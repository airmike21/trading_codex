"""Risk parity / Equal Risk Contribution (ERC) allocator utilities."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RiskParityConfig:
    lookback: int = 63
    max_iter: int = 200
    tol: float = 1e-8
    min_weight: float = 0.0
    jitter: float = 1e-10


def _clean_returns(returns: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(returns, pd.DataFrame):
        raise TypeError("returns must be a DataFrame.")
    return returns.astype(float).fillna(0.0)


def cov_matrix(returns: pd.DataFrame, lookback: int) -> pd.DataFrame:
    r = _clean_returns(returns)
    return r.tail(int(lookback)).cov(ddof=1)


def risk_contributions(weights: np.ndarray, cov: np.ndarray) -> np.ndarray:
    w = np.asarray(weights, dtype=float)
    sigma = np.asarray(cov, dtype=float)
    port_var = float(w.T @ sigma @ w)
    if port_var <= 0.0:
        return np.zeros_like(w)
    marginal = sigma @ w
    return (w * marginal) / port_var


def erc_weights_from_cov(cov: np.ndarray, cfg: RiskParityConfig) -> np.ndarray:
    sigma = np.asarray(cov, dtype=float).copy()
    n = sigma.shape[0]
    if sigma.shape != (n, n):
        raise ValueError("cov must be a square matrix.")
    sigma.flat[:: n + 1] += float(cfg.jitter)

    vols = np.sqrt(np.clip(np.diag(sigma), 0.0, np.inf))
    inv_vols = np.where(vols > 0.0, 1.0 / vols, 0.0)
    if float(inv_vols.sum()) <= 0.0:
        w = np.full(n, 1.0 / n, dtype=float)
    else:
        w = inv_vols / float(inv_vols.sum())

    target = np.full(n, 1.0 / n, dtype=float)
    for _ in range(int(cfg.max_iter)):
        rc = risk_contributions(w, sigma)
        if float(rc.sum()) <= 0.0:
            return np.full(n, 1.0 / n, dtype=float)

        ratio = np.divide(target, rc, out=np.zeros_like(target), where=rc > 0.0)
        w_new = w * ratio
        w_new = np.clip(w_new, float(cfg.min_weight), np.inf)
        total = float(w_new.sum())
        if total <= 0.0:
            w_new = np.full(n, 1.0 / n, dtype=float)
        else:
            w_new = w_new / total

        if float(np.max(np.abs(w_new - w))) < float(cfg.tol):
            return w_new
        w = w_new

    return w


def erc_weights(returns: pd.DataFrame, cfg: RiskParityConfig) -> pd.Series:
    r = _clean_returns(returns)
    cov = cov_matrix(r, lookback=int(cfg.lookback)).to_numpy()
    w = erc_weights_from_cov(cov, cfg)
    return pd.Series(w, index=r.columns, dtype=float)


def erc_weight_series(
    returns: pd.DataFrame,
    rebalance_mask: pd.Series,
    cfg: RiskParityConfig,
) -> pd.DataFrame:
    """Build rebalanced ERC weights and forward-fill between updates."""
    r = _clean_returns(returns)
    mask = rebalance_mask.reindex(r.index).fillna(False).astype(bool)
    weights = pd.DataFrame(index=r.index, columns=r.columns, dtype=float)

    n_assets = len(r.columns)
    if n_assets == 0:
        return weights
    equal_weight = pd.Series(1.0 / n_assets, index=r.columns, dtype=float)

    for dt in r.index:
        if not bool(mask.loc[dt]):
            continue
        hist = r.loc[:dt].iloc[:-1]
        if len(hist) < int(cfg.lookback):
            w = equal_weight
        else:
            w = erc_weights(hist, cfg)
        weights.loc[dt] = w

    weights = weights.ffill().fillna(1.0 / n_assets)
    return weights
