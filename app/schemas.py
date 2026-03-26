from pydantic import BaseModel, Field
from typing import Literal


class HealthResponse(BaseModel):
    status: str
    system: str
    version: str


class ScanItem(BaseModel):
    passed: bool = False
    symbol: str
    state: Literal["ready", "watch"] = "watch"
    direction: str | None = None
    stop_pct: float | None = None
    tp1_pct: float | None = None
    tp2_pct: float | None = None
    rr: float | None = None
    message: str = ""
    warnings: list[str] = Field(default_factory=list)
    rejected_by: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    reason_summary: str | None = None


class ScanResponse(BaseModel):
    status: str
    mode: str
    count: int = 0
    candidate_pool: int = 0
    stage1_checked: int = 0
    stage2_checked: int = 0
    scan_seconds: float = 0.0
    stopped_reason: str | None = None
    items: list[ScanItem] = Field(default_factory=list)
    message: str = ""
    errors: list[str] = Field(default_factory=list)
    cache_status: str | None = None
