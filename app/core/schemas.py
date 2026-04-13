from typing import Literal
from pydantic import BaseModel, Field


class RiskPlan(BaseModel):
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
    extension_basis: str


class DirectAction(BaseModel):
    status: Literal['buyable', 'wait', 'reject']
    action: str
    message: str


class ScanSignal(BaseModel):
    symbol: str
    state: str
    grade: str
    score: float
    current_price: float
    entry_zone_status: str
    divergence_timeframe: str
    divergence_kind: str
    chain_points: int = 0
    breakout_confirmed: bool
    market_regime: str
    volume_ratio: float
    resistance_room_pct: float
    strength_profile: str
    risk: RiskPlan
    direct: DirectAction
    filters_passed: bool
    rejected_reasons: list[str] = Field(default_factory=list)
    reason_summary: str


class ScanResponse(BaseModel):
    mode: str
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
