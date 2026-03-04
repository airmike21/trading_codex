import numpy as np
import pandas as pd
import pandas.testing as pdt

from trading_codex.overlays.ivol_overlay import apply_inverse_vol_overlay


def make_panel(close_map: dict[str, pd.Series]) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for symbol, close in close_map.items():
        frames[symbol] = pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=close.index,
        )
    return pd.concat(frames, axis=1)


def test_ivol_overlay_tilts_toward_lower_vol_and_ffills_between_updates():
    idx = pd.date_range("2020-01-01", periods=80, freq="B")
    ret_a = np.full(len(idx), 0.001)
    ret_b = np.where(np.arange(len(idx)) % 2 == 0, 0.03, -0.025)
    ret_shy = np.full(len(idx), 0.0002)
    bars = make_panel(
        {
            "A": pd.Series(100.0 * np.cumprod(1.0 + ret_a), index=idx),
            "B": pd.Series(100.0 * np.cumprod(1.0 + ret_b), index=idx),
            "SHY": pd.Series(100.0 * np.cumprod(1.0 + ret_shy), index=idx),
        }
    )

    weights = pd.DataFrame(0.0, index=idx, columns=["A", "B", "SHY"], dtype=float)
    first_active = idx[40]
    weights.loc[first_active:, "A"] = 0.5
    weights.loc[first_active:, "B"] = 0.5

    adjusted = apply_inverse_vol_overlay(bars, weights, lookback=20, eps=1e-8)

    assert float(adjusted.loc[first_active, "A"]) > float(adjusted.loc[first_active, "B"])
    active = weights.sum(axis=1) > 0.0
    assert bool(np.isclose(adjusted.loc[active].sum(axis=1), 1.0, atol=1e-12).all())

    changed = adjusted.ne(adjusted.shift(1)).any(axis=1)
    changed.iloc[0] = False
    changed_dates = adjusted.index[changed]
    assert list(changed_dates) == [first_active]

    expected_row = adjusted.loc[first_active]
    for dt in idx[idx.get_loc(first_active) + 1 :]:
        pdt.assert_series_equal(adjusted.loc[dt], expected_row, check_names=False)


def test_ivol_overlay_falls_back_to_original_when_vols_nan():
    idx = pd.date_range("2020-01-01", periods=10, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 101.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 102.0, len(idx)), index=idx),
            "SHY": pd.Series(np.linspace(100.0, 100.5, len(idx)), index=idx),
        }
    )

    weights = pd.DataFrame(0.0, index=idx, columns=["A", "B", "SHY"], dtype=float)
    first_active = idx[2]
    weights.loc[first_active:, "A"] = 0.5
    weights.loc[first_active:, "B"] = 0.5

    adjusted = apply_inverse_vol_overlay(bars, weights, lookback=20, eps=1e-8)

    pdt.assert_series_equal(adjusted.loc[first_active], weights.loc[first_active], check_names=False)
    active = weights.sum(axis=1) > 0.0
    assert bool(np.isclose(adjusted.loc[active].sum(axis=1), 1.0, atol=1e-12).all())
    for dt in idx[idx.get_loc(first_active) + 1 :]:
        pdt.assert_series_equal(adjusted.loc[dt], adjusted.loc[first_active], check_names=False)

