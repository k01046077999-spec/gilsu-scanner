from typing import Any, Literal
from pydantic import BaseModel, Field


Mode = Literal["main", "sub"]
Side = Literal["bullish", "bearish"]
Grade = Literal["main", "sub", "reject"]


class SignalResponse(BaseModel):
    symbol: str
    timeframe: str
    mode: Mode
    side: Side | None = None
    grade: Grade
    score: float = Field(..., description="0~100")
    entry_zone: list[float] | None = None
    stop_loss: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    current_price: float | None = None
    reasons: list[str] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)


class TopPick(BaseModel):
    symbol: str
    side: Side | None = None
    grade: Grade | None = None
    score: float
    rank_score: float
    rr_tp2: float | None = None
    tp2_pct: float | None = None
    volume_ratio: float | None = None
    current_price: float | None = None
    stop_loss: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    reason: str


class ScanResponse(BaseModel):
    mode: Mode
    count: int
    results: list[SignalResponse]
    top_picks: list[TopPick] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
