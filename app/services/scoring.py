from __future__ import annotations

"""
제이드 파동 이론 — 신호 점수 및 등급 산출

점수 구성:
  - 다이버전스 (chain=더 높음)
  - 피보나치 zone
  - 직전고 돌파 (파동 목적 달성)
  - 시장 환경 (BTC 레짐)
  - 거래량
  - 손절·수익비

등급:
  S: 90+  — 매우 강한 신호, 즉각 진입 검토
  A: 75+  — 강한 신호
  B: 60+  — 보통 신호, 분할 진입
  C: 45+  — 약한 신호, 소량만
  D: ~    — 관심 제외
"""


def grade_from_score(score: float) -> str:
    if score >= 90:
        return 'S'
    if score >= 75:
        return 'A'
    if score >= 60:
        return 'B'
    if score >= 45:
        return 'C'
    return 'D'
