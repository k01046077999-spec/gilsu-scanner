from __future__ import annotations

import logging

from fastapi import FastAPI, Query

from app.config import settings
from app.models import ScanResponse, SignalResponse
from app.services.binance_client import shutdown_http_client, startup_http_client
from app.services.scanner import analyze_symbol, scan_symbols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(
    title="길수매매법 코인 검색기",
    version="0.2.0",
    description="1시간봉 중심 RSI 다이버전스 연계 + Fib 기반 메인/서브 코인 검색기",
)


@app.on_event("startup")
async def on_startup() -> None:
    await startup_http_client()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await shutdown_http_client()


@app.get("/")
async def root():
    return {
        "service": "gilsu-scanner",
        "version": "0.2.0",
        "endpoints": ["/health", "/ready", "/scan/main", "/scan/sub", "/scan/symbol/{symbol}"],
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gilsu-scanner"}


@app.get("/ready")
async def ready():
    return {
        "status": "ready",
        "service": "gilsu-scanner",
        "default_symbol_count": len(settings.default_symbols),
        "scan_concurrency": settings.scan_concurrency,
    }


@app.get("/scan/main", response_model=ScanResponse)
async def scan_main(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else settings.default_symbols
    results, diagnostics = await scan_symbols(symbol_list, mode="main")
    return ScanResponse(mode="main", count=len(results), results=results, diagnostics=diagnostics)


@app.get("/scan/sub", response_model=ScanResponse)
async def scan_sub(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else settings.default_symbols
    results, diagnostics = await scan_symbols(symbol_list, mode="sub")
    return ScanResponse(mode="sub", count=len(results), results=results, diagnostics=diagnostics)


@app.get("/scan/symbol/{symbol}", response_model=SignalResponse)
async def scan_symbol(symbol: str, mode: str = Query("main", pattern="^(main|sub)$")):
    return await analyze_symbol(symbol.upper(), mode=mode)
