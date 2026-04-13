from __future__ import annotations

import pandas as pd


def mark_pivots(df: pd.DataFrame, left: int, right: int) -> pd.DataFrame:
    out = df.copy()
    out['pivot_low'] = False
    out['pivot_high'] = False
    for i in range(left, len(out) - right):
        lows = out['low'].iloc[i - left:i + right + 1]
        highs = out['high'].iloc[i - left:i + right + 1]
        if out['low'].iloc[i] == lows.min():
            out.loc[out.index[i], 'pivot_low'] = True
        if out['high'].iloc[i] == highs.max():
            out.loc[out.index[i], 'pivot_high'] = True
    return out


def recent_pivot_lows(df: pd.DataFrame, limit: int = 8) -> pd.DataFrame:
    return df[df['pivot_low']].tail(limit).copy()


def recent_pivot_highs(df: pd.DataFrame, limit: int = 8) -> pd.DataFrame:
    return df[df['pivot_high']].tail(limit).copy()
