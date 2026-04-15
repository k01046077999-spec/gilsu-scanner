from typing import Literal
from pydantic import BaseModel, Field


class RiskPlan(BaseModel):
    """
    진입·손절·익절 플랜.
    - stop_price  : 피보나치 1 (= swing 저점), 이 가격 종가 이탈 시 손절
    - tp1_price   : 직전 고점 (anchor_high = 피보나치 0)
    - tp2_price   : 1.272 or 1.618 확장 레벨 (강도에 따라 동적 선택)
    """
    entry_price: float
    stop_price: float
    stop_loss_pct: float

    tp1_price: float
    tp1_pct: float
    tp2_price: float
    tp2_pct: float
    rr_tp1: float
    rr_tp2: float

    fib_anchor_low: float
    fib_anchor_high: float
    fib_0618: float
    fib_0786: float
    extension_basis: str   # 'fib_1.272_extension' | 'fib_1.618_extension'


class DirectAction(BaseModel):
    """GPT 연동용: 현재 즉각 행동 가이드"""
    status: Literal['buyable', 'wait', 'reject']
    action: str
    message: str


class DivergenceDetail(BaseModel):
    """다이버전스 세부 정보"""
    found: bool
    kind: str          # 'none' | 'general' | 'chain'
    points: int = 0    # 꼭지점 수 (2=general, 3=chain)
    timeframe: str     # '15m' | '1h' | '4h'


class ScanSignal(BaseModel):
    symbol: str
    state: str         # 'candidate' | 'wait' | 'reject'
    grade: str         # 'S' | 'A' | 'B' | 'C' | 'D'
    score: float

    current_price: float

    # 다이버전스
    primary_divergence: DivergenceDetail
    divergence_1h: DivergenceDetail
    divergence_4h: DivergenceDetail

    # 진입 구간
    entry_zone_status: str    # 'in_zone' | 'near_zone' | 'out_zone'
    breakout_confirmed: bool  # 직전 고점 돌파 여부 (파동 목적 달성)

    # 시장 환경
    market_regime: str        # 'risk_on' | 'neutral_ok' | 'risk_off'

    # 지표
    rsi_1h: float
    rsi_15m: float
    volume_ratio: float
    resistance_room_pct: float
    strength_profile: str     # 'strong' | 'standard'

    # 리스크 플랜
    risk: RiskPlan

    # GPT 행동 가이드
    direct: DirectAction

    filters_passed: bool
    rejected_reasons: list[str] = Field(default_factory=list)
    reason_summary: str


class ScanResponse(BaseModel):
    scanned_symbols: int
    matched_symbols: int
    elapsed_seconds: float
    top_picks: list[ScanSignal]
    signals: list[ScanSignal]
    warnings: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str
    version: str


class ReadyResponse(BaseModel):
    version: str
    scan_market_limit: int
    top_pick_count: int
    max_stop_pct: float
    min_rr_tp1: float
    min_rr_tp2: float
