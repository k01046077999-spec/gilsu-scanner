from __future__ import annotations

"""
제이드 파동 이론 — 피보나치 되돌림 모듈

핵심 원칙 (PDF):
  - 상승 파동이 멈춘 구간에서 저점~고점 피보나치를 긋는다
  - 0.618 ~ 0.786 사이가 평균적 되돌림 구간 (매수 대기 구간)
  - 피보나치 1 (= 저점) 이탈 시 파동 무효 → 손절 기준
  - 익절 1차 : 직전 고점 (피보나치 0)
  - 익절 2차 : 1.272 또는 1.618 확장 레벨 (강도에 따라 선택)
"""

import pandas as pd


def build_retracement(low: float, high: float) -> dict:
    """저점(low)~고점(high) 기준 피보나치 되돌림 & 확장 레벨 계산"""
    diff = high - low
    return {
        'anchor_low': low,
        'anchor_high': high,
        'fib_0': high,           # 고점 = 피보나치 0
        'fib_0236': high - diff * 0.236,
        'fib_0382': high - diff * 0.382,
        'fib_05': high - diff * 0.5,
        'fib_0618': high - diff * 0.618,   # 1차 매수 대기
        'fib_0786': high - diff * 0.786,   # 2차 매수 대기
        'fib_1': low,            # 저점 = 손절 기준
        'ext_1272': high + diff * 0.272,   # 표준 익절 2차
        'ext_1618': high + diff * 0.618,   # 강세 익절 2차
    }


def zone_status(price: float, fib_0618: float, fib_0786: float, tolerance_pct: float) -> str:
    """
    현재가가 피보나치 매수 구간(0.618~0.786)에 있는지 판단.
    - in_zone   : 0.618~0.786 사이 (이상적 매수 자리)
    - near_zone : tolerance 이내 (허용 범위)
    - out_zone  : 구간 외 (추격 매수 금지)
    """
    lo = min(fib_0618, fib_0786)
    hi = max(fib_0618, fib_0786)
    tol = hi * tolerance_pct / 100.0
    if lo <= price <= hi:
        return 'in_zone'
    if (lo - tol) <= price <= (hi + tol):
        return 'near_zone'
    return 'out_zone'


def recent_bullish_swing(
    df: pd.DataFrame,
    pivot_highs: pd.DataFrame,
    pivot_lows: pd.DataFrame,
) -> dict | None:
    """
    가장 최근의 유효한 상승 스윙(저점→고점) 탐지.

    로직:
      1. 최근 pivot_high 중 직전 pivot_high 를 돌파(breakout)한 고점 탐색
      2. 그 고점 이전에 위치한 pivot_low 를 anchor_low 로 선택
      3. 피보나치 구조 반환

    breakout_confirmed = True 이면 파동의 목적(직전고 돌파) 달성 → 더 안전한 진입
    """
    if len(pivot_highs) < 2 or len(pivot_lows) < 1:
        return None

    highs = pivot_highs.reset_index()
    lows = pivot_lows.reset_index()

    for h_idx in range(len(highs) - 1, 0, -1):
        latest_high_i = int(highs.loc[h_idx, 'index'])
        prev_high_i = int(highs.loc[h_idx - 1, 'index'])
        latest_high = float(highs.loc[h_idx, 'high'])
        prev_high = float(highs.loc[h_idx - 1, 'high'])

        # 직전 고점 돌파 여부 (파동의 목적 달성 여부)
        breakout = latest_high > prev_high

        # anchor_low: 이 고점 이전에 있는 가장 최근 저점
        low_candidates = lows[lows['index'] < latest_high_i]
        if low_candidates.empty:
            continue

        anchor_low_row = low_candidates.iloc[-1]
        anchor_low_i = int(anchor_low_row['index'])
        anchor_low = float(anchor_low_row['low'])

        if anchor_low_i >= latest_high_i:
            continue
        if latest_high_i - anchor_low_i < 4:  # 너무 짧은 스윙 제외
            continue

        fib = build_retracement(anchor_low, latest_high)
        return {
            'anchor_low_index': anchor_low_i,
            'anchor_high_index': latest_high_i,
            'previous_high_index': prev_high_i,
            'breakout_confirmed': breakout,
            **fib,
        }

    return None
