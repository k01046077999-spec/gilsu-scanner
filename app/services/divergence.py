from __future__ import annotations

"""
제이드 파동 이론 — 다이버전스 감지 모듈

핵심 원칙 (PDF):
  - 상승 다이버전스: 지수 저점은 낮아지는데 RSI 저점은 올라가는 패턴
  - 다이버전스 '연계'(chain): 최소 3개 꼭지점이 연속으로 이어질 때
      → 가장 신뢰도 높은 진입 신호
  - RSI가 점선(과매도/과매수 구간 경계)을 넘는 자리에서 발생해야 유효
  - 중간 구간(RSI 40~60 사이)의 다이버전스는 신뢰도 낮음

detect_bullish:  매수(롱) 다이버전스 감지
detect_bearish:  매도(숏) 다이버전스 감지
"""

import pandas as pd

# RSI 가 이 임계 이하/이상에서 꼭지를 찍어야 유효한 다이버전스로 간주
RSI_OVERSOLD_THRESHOLD = 42.0   # 저점 RSI
RSI_OVERBOUGHT_THRESHOLD = 58.0  # 고점 RSI


def _is_rsi_extreme_low(rsi_val: float) -> bool:
    return rsi_val <= RSI_OVERSOLD_THRESHOLD


def _is_rsi_extreme_high(rsi_val: float) -> bool:
    return rsi_val >= RSI_OVERBOUGHT_THRESHOLD


def detect_bullish(pivots: pd.DataFrame, min_gap: int, max_gap: int, min_chain_span: int) -> dict:
    """
    상승 다이버전스 감지.
    Returns:
        found       : bool
        kind        : 'none' | 'general' | 'chain'
        points      : 꼭지점 수
        rsi_extreme : 마지막 꼭지점이 RSI 극값 구간인지
    """
    rows = pivots[['low', 'rsi']].reset_index()
    if len(rows) < 2:
        return {'found': False, 'kind': 'none', 'points': 0, 'rsi_extreme': False}

    best: dict = {'found': False, 'kind': 'none', 'points': 0, 'rsi_extreme': False}

    for end in range(len(rows) - 1, 0, -1):
        i2 = int(rows.loc[end, 'index'])
        p2 = float(rows.loc[end, 'low'])
        r2 = float(rows.loc[end, 'rsi'])

        # RSI 극값 체크 (PDF: 점선 영역 넘는 자리만 유효)
        rsi_ext = _is_rsi_extreme_low(r2)

        for start in range(end - 1, -1, -1):
            i1 = int(rows.loc[start, 'index'])
            gap = i2 - i1
            if gap < min_gap:
                continue
            if gap > max_gap:
                break

            p1 = float(rows.loc[start, 'low'])
            r1 = float(rows.loc[start, 'rsi'])

            # 조건: 지수 저점 낮아짐 + RSI 저점 높아짐
            if not (p2 < p1 and r2 > r1):
                continue

            best = {
                'found': True,
                'kind': 'general',
                'points': 2,
                'rsi_extreme': rsi_ext,
                'indices': [i1, i2],
            }

            # chain(3개 연계) 탐색
            for prev in range(start - 1, -1, -1):
                i0 = int(rows.loc[prev, 'index'])
                p0 = float(rows.loc[prev, 'low'])
                r0 = float(rows.loc[prev, 'rsi'])

                if i2 - i0 < min_chain_span:
                    continue
                # p0 >= p1 >= p2 (저점 낮아짐), r0 <= r1 <= r2 (RSI 높아짐)
                if p1 <= p0 and r1 >= r0 and r2 >= r1:
                    return {
                        'found': True,
                        'kind': 'chain',
                        'points': 3,
                        'rsi_extreme': rsi_ext,
                        'indices': [i0, i1, i2],
                    }
            return best

    return best


def detect_bearish(pivots: pd.DataFrame, min_gap: int, max_gap: int, min_chain_span: int) -> dict:
    """
    하락 다이버전스 감지.
    Returns:
        found       : bool
        kind        : 'none' | 'general' | 'chain'
        points      : 꼭지점 수
        rsi_extreme : 마지막 꼭지점이 RSI 극값 구간인지
    """
    rows = pivots[['high', 'rsi']].reset_index()
    if len(rows) < 2:
        return {'found': False, 'kind': 'none', 'points': 0, 'rsi_extreme': False}

    best: dict = {'found': False, 'kind': 'none', 'points': 0, 'rsi_extreme': False}

    for end in range(len(rows) - 1, 0, -1):
        i2 = int(rows.loc[end, 'index'])
        p2 = float(rows.loc[end, 'high'])
        r2 = float(rows.loc[end, 'rsi'])

        rsi_ext = _is_rsi_extreme_high(r2)

        for start in range(end - 1, -1, -1):
            i1 = int(rows.loc[start, 'index'])
            gap = i2 - i1
            if gap < min_gap:
                continue
            if gap > max_gap:
                break

            p1 = float(rows.loc[start, 'high'])
            r1 = float(rows.loc[start, 'rsi'])

            # 조건: 지수 고점 높아짐 + RSI 고점 낮아짐
            if not (p2 > p1 and r2 < r1):
                continue

            best = {
                'found': True,
                'kind': 'general',
                'points': 2,
                'rsi_extreme': rsi_ext,
                'indices': [i1, i2],
            }

            for prev in range(start - 1, -1, -1):
                i0 = int(rows.loc[prev, 'index'])
                p0 = float(rows.loc[prev, 'high'])
                r0 = float(rows.loc[prev, 'rsi'])

                if i2 - i0 < min_chain_span:
                    continue
                if p1 >= p0 and r1 <= r0 and r2 <= r1:
                    return {
                        'found': True,
                        'kind': 'chain',
                        'points': 3,
                        'rsi_extreme': rsi_ext,
                        'indices': [i0, i1, i2],
                    }
            return best

    return best
