from __future__ import annotations

from fastapi import FastAPI, Query

from app.config import settings
from app.models import ScanResponse, SignalResponse
from app.services.scanner import analyze_symbol, scan_symbols

app = FastAPI(
    title="길수매매법 코인 검색기",
    version="0.1.0",
    description="1시간봉 중심 RSI 다이버전스 연계 + Fib 기반 메인/서브 코인 검색기 MVP",
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gilsu-scanner"}


@app.get("/scan/main", response_model=ScanResponse)
async def scan_main(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else settings.default_symbols
    results = await scan_symbols(symbol_list, mode="main")
    return ScanResponse(mode="main", count=len(results), results=results)


@app.get("/scan/sub", response_model=ScanResponse)
async def scan_sub(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else settings.default_symbols
    results = await scan_symbols(symbol_list, mode="sub")
    return ScanResponse(mode="sub", count=len(results), results=results)


@app.get("/scan/symbol/{symbol}", response_model=SignalResponse)
async def scan_symbol(symbol: str, mode: str = Query(default="main", pattern="^(main|sub)$")):
    return await analyze_symbol(symbol.upper(), mode=mode)  # type: ignore[arg-type]
