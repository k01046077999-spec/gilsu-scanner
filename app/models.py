from typing import Literal
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
    metrics: dict = Field(default_factory=dict)


class ScanResponse(BaseModel):
    mode: Mode
    count: int
    results: list[SignalResponse]
