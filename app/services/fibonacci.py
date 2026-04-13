from __future__ import annotations

import pandas as pd


def build_retracement(low: float, high: float) -> dict:
    diff = high - low
    return {
        'anchor_low': low,
        'anchor_high': high,
        'fib_0': high,
        'fib_0618': high - diff * 0.618,
        'fib_0786': high - diff * 0.786,
        'fib_1': low,
        'ext_1272': high + diff * 0.272,
        'ext_1618': high + diff * 0.618,
    }


def zone_status(price: float, fib_0618: float, fib_0786: float, tolerance_pct: float) -> str:
    lo = min(fib_0618, fib_0786)
    hi = max(fib_0618, fib_0786)
    tol = hi * tolerance_pct / 100.0
    if lo <= price <= hi:
        return 'in_zone'
    if (lo - tol) <= price <= (hi + tol):
        return 'near_zone'
    return 'out_zone'


def recent_bullish_swing(df: pd.DataFrame, pivot_highs: pd.DataFrame, pivot_lows: pd.DataFrame) -> dict | None:
    if len(pivot_highs) < 2 or len(pivot_lows) < 1:
        return None

    highs = pivot_highs.reset_index()
    lows = pivot_lows.reset_index()

    for h_idx in range(len(highs) - 1, 0, -1):
        latest_high_i = int(highs.loc[h_idx, 'index'])
        prev_high_i = int(highs.loc[h_idx - 1, 'index'])
        latest_high = float(highs.loc[h_idx, 'high'])
        prev_high = float(highs.loc[h_idx - 1, 'high'])
        if latest_high <= prev_high:
            continue

        low_candidates = lows[lows['index'] < latest_high_i]
        if low_candidates.empty:
            continue
        anchor_low_row = low_candidates.iloc[-1]
        anchor_low_i = int(anchor_low_row['index'])
        anchor_low = float(anchor_low_row['low'])
        if anchor_low_i >= latest_high_i:
            continue
        if latest_high_i - anchor_low_i < 4:
            continue

        fib = build_retracement(anchor_low, latest_high)
        return {
            'anchor_low_index': anchor_low_i,
            'anchor_high_index': latest_high_i,
            'previous_high_index': prev_high_i,
            'breakout_confirmed': latest_high > prev_high,
            **fib,
        }
    return None
