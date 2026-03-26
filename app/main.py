import asyncio
from contextlib import suppress
from fastapi import FastAPI, Query

from app.schemas import HealthResponse, ScanItem, ScanResponse
from core import config
from core.engine import PresidentTradingEngine
from storage.cache import InMemoryCache

app = FastAPI(
    title="대통령매매법 API",
    version=config.APP_VERSION,
    servers=[{"url": config.BASE_URL}],
)

engine = PresidentTradingEngine()
cache = InMemoryCache()
refresh_locks = {"main": asyncio.Lock(), "sub": asyncio.Lock()}
background_tasks = []

def _refreshing_payload(mode: str) -> dict:
    return {
        "status": "refreshing",
        "mode": mode,
        "count": 0,
        "candidate_pool": 0,
        "stage1_checked": 0,
        "stage2_checked": 0,
        "scan_seconds": 0.0,
        "stopped_reason": None,
        "items": [],
        "message": config.REFRESHING_MESSAGE,
        "errors": [],
        "cache_status": "warming",
    }

async def _refresh_mode(mode: str) -> None:
    async with refresh_locks[mode]:
        result = await asyncio.to_thread(engine.scan, mode, 10 if mode == "main" else 12)
        result["cache_status"] = "fresh"
        cache.set(mode, result)

async def _loop_refresh(mode: str, interval: int) -> None:
    await _refresh_mode(mode)
    while True:
        await asyncio.sleep(interval)
        with suppress(Exception):
            await _refresh_mode(mode)

def _snapshot(mode: str) -> dict:
    cached = cache.get(mode)
    if not cached:
        return _refreshing_payload(mode)
    payload = dict(cached)
    payload.setdefault("cache_status", "fresh")
    return payload

@app.on_event("startup")
async def startup_event() -> None:
    background_tasks.clear()
    background_tasks.append(asyncio.create_task(_loop_refresh("main", config.REFRESH_INTERVAL_MAIN)))
    background_tasks.append(asyncio.create_task(_loop_refresh("sub", config.REFRESH_INTERVAL_SUB)))

@app.on_event("shutdown")
async def shutdown_event() -> None:
    for task in background_tasks:
        task.cancel()
    for task in background_tasks:
        with suppress(asyncio.CancelledError):
            await task

@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", system="president-trading-system", version=config.APP_VERSION)

@app.get("/scan/main", response_model=ScanResponse, response_model_exclude_none=True)
def scan_main(limit: int = Query(default=10, ge=1, le=30)) -> ScanResponse:
    return ScanResponse(**engine.scan(mode="main", limit=limit))

@app.get("/scan/sub", response_model=ScanResponse, response_model_exclude_none=True)
def scan_sub(limit: int = Query(default=12, ge=1, le=40)) -> ScanResponse:
    return ScanResponse(**engine.scan(mode="sub", limit=limit))

@app.get("/scan/symbol/{symbol}", response_model=ScanItem, response_model_exclude_none=True)
def scan_symbol(symbol: str, mode: str = Query(default="main", pattern="^(main|sub)$")) -> ScanItem:
    return ScanItem(**engine.analyze_symbol(symbol.upper(), mode=mode))

@app.get("/gpt/main", response_model=ScanResponse, response_model_exclude_none=True)
def gpt_main() -> ScanResponse:
    return ScanResponse(**_snapshot("main"))

@app.get("/gpt/sub", response_model=ScanResponse, response_model_exclude_none=True)
def gpt_sub() -> ScanResponse:
    return ScanResponse(**_snapshot("sub"))

@app.post("/refresh/main", response_model=ScanResponse, response_model_exclude_none=True)
async def refresh_main() -> ScanResponse:
    await _refresh_mode("main")
    return ScanResponse(**_snapshot("main"))

@app.post("/refresh/sub", response_model=ScanResponse, response_model_exclude_none=True)
async def refresh_sub() -> ScanResponse:
    await _refresh_mode("sub")
    return ScanResponse(**_snapshot("sub"))
