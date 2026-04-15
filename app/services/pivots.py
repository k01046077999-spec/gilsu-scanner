from __future__ import annotations

import pandas as pd


def mark_pivots(df: pd.DataFrame, left: int, right: int) -> pd.DataFrame:
    """
    left·right 봉 기준 로컬 고점/저점 마킹.
    - pivot_low  : 왼쪽 left봉, 오른쪽 right봉 중 최저가인 봉
    - pivot_high : 왼쪽 left봉, 오른쪽 right봉 중 최고가인 봉
    """
    out = df.copy()
    out['pivot_low'] = False
    out['pivot_high'] = False
    n = len(out)
    for i in range(left, n - right):
        window_lows = out['low'].iloc[i - left: i + right + 1]
        window_highs = out['high'].iloc[i - left: i + right + 1]
        if out['low'].iloc[i] == window_lows.min():
            out.loc[out.index[i], 'pivot_low'] = True
        if out['high'].iloc[i] == window_highs.max():
            out.loc[out.index[i], 'pivot_high'] = True
    return out


def recent_pivot_lows(df: pd.DataFrame, limit: int = 8) -> pd.DataFrame:
    """최근 N개 pivot_low 반환"""
    return df[df['pivot_low']].tail(limit).copy()


def recent_pivot_highs(df: pd.DataFrame, limit: int = 8) -> pd.DataFrame:
    """최근 N개 pivot_high 반환"""
    return df[df['pivot_high']].tail(limit).copy()
