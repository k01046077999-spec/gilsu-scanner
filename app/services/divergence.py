from __future__ import annotations

import pandas as pd


def detect_bullish(pivots: pd.DataFrame, min_gap: int, max_gap: int, min_chain_span: int) -> dict:
    rows = pivots[['low', 'rsi']].reset_index()
    if len(rows) < 2:
        return {'found': False, 'kind': 'none', 'points': 0}

    best = {'found': False, 'kind': 'none', 'points': 0}
    for end in range(len(rows) - 1, 0, -1):
        i2 = int(rows.loc[end, 'index'])
        p2 = float(rows.loc[end, 'low'])
        r2 = float(rows.loc[end, 'rsi'])
        for start in range(end - 1, -1, -1):
            i1 = int(rows.loc[start, 'index'])
            gap = i2 - i1
            if gap < min_gap:
                continue
            if gap > max_gap:
                break
            p1 = float(rows.loc[start, 'low'])
            r1 = float(rows.loc[start, 'rsi'])
            if not (p2 < p1 and r2 > r1):
                continue
            best = {'found': True, 'kind': 'general', 'points': 2, 'indices': [i1, i2]}
            for prev in range(start - 1, -1, -1):
                i0 = int(rows.loc[prev, 'index'])
                p0 = float(rows.loc[prev, 'low'])
                r0 = float(rows.loc[prev, 'rsi'])
                if i2 - i0 < min_chain_span:
                    continue
                if p1 <= p0 and r1 >= r0 and r2 >= r1:
                    return {'found': True, 'kind': 'chain', 'points': 3, 'indices': [i0, i1, i2]}
            return best
    return best
