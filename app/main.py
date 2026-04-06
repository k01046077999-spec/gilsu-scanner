from __future__ import annotations

from fastapi import FastAPI, Query

from app.config import settings
from app.models import ScanResponse, SignalResponse
from app.services.binance_client import close_client, get_client
from app.services.scanner import analyze_symbol, scan_symbols

app = FastAPI(
    title="길수매매법 코인 검색기",
    version="0.3.0",
    description="1시간봉 중심 RSI 다이버전스 연계 + Fib 기반 메인/서브 코인 검색기 실전 스캔 버전",
)


@app.on_event("startup")
async def startup_event():
    await get_client()


@app.on_event("shutdown")
async def shutdown_event():
    await close_client()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gilsu-scanner"}


@app.get("/ready")
async def ready():
    await get_client()
    return {"status": "ready", "service": "gilsu-scanner"}


@app.get("/scan/main", response_model=ScanResponse)
async def scan_main(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None
    if symbol_list is not None and not symbol_list:
        symbol_list = settings.default_symbols
    results, diagnostics = await scan_symbols(symbol_list, mode="main")
    return ScanResponse(mode="main", count=len(results), results=results, diagnostics=diagnostics)


@app.get("/scan/sub", response_model=ScanResponse)
async def scan_sub(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None
    if symbol_list is not None and not symbol_list:
        symbol_list = settings.default_symbols
    results, diagnostics = await scan_symbols(symbol_list, mode="sub")
    return ScanResponse(mode="sub", count=len(results), results=results, diagnostics=diagnostics)


@app.get("/scan/symbol/{symbol}", response_model=SignalResponse)
async def scan_symbol(symbol: str, mode: str = Query("main", pattern="^(main|sub)$")):
    return await analyze_symbol(symbol.upper(), mode=mode)
