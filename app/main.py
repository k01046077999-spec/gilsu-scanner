from __future__ import annotations

from fastapi import FastAPI, Query

from app.config import settings
from app.models import ScanResponse, SignalResponse, TopPicksResponse
from app.services.upbit_client import close_client, get_client, normalize_market_symbol
from app.services.scanner import analyze_symbol, scan_symbols

app = FastAPI(
    title="길수매매법 코인 검색기",
    version="0.7.4",
    description="업비트 KRW 마켓 전용 1시간봉 중심 RSI 다이버전스 연계 + Fib 기반 메인/서브 코인 검색기",
)


@app.on_event("startup")
async def startup_event():
    await get_client()


@app.on_event("shutdown")
async def shutdown_event():
    await close_client()


@app.get("/")
async def root():
    return {
        "service": "gilsu-scanner",
        "version": "0.7.4",
        "endpoints": ["/health", "/ready", "/scan/main", "/scan/sub", "/scan/symbol/{symbol}"],
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gilsu-scanner", "version": "0.7.4"}


@app.get("/ready")
async def ready():
    await get_client()
    return {
        "status": "ready",
        "service": "gilsu-scanner",
        "version": "0.7.4",
        "universe_size": settings.universe_size,
        "prefilter_size": settings.prefilter_size,
        "scan_concurrency": settings.scan_concurrency,
    }


@app.get("/scan/main", response_model=ScanResponse)
async def scan_main(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None
    if symbol_list is not None and not symbol_list:
        symbol_list = settings.default_symbols
    results, diagnostics, top_picks = await scan_symbols(symbol_list, mode="main")
    return ScanResponse(mode="main", count=len(results), results=results, top_picks=top_picks, diagnostics=diagnostics)


@app.get("/scan/sub", response_model=ScanResponse)
async def scan_sub(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None
    if symbol_list is not None and not symbol_list:
        symbol_list = settings.default_symbols
    results, diagnostics, top_picks = await scan_symbols(symbol_list, mode="sub")
    return ScanResponse(mode="sub", count=len(results), results=results, top_picks=top_picks, diagnostics=diagnostics)




@app.get("/scan/sub/top", response_model=TopPicksResponse)
async def scan_sub_top(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None
    if symbol_list is not None and not symbol_list:
        symbol_list = settings.default_symbols
    results, diagnostics, top_picks = await scan_symbols(symbol_list, mode="sub")
    compact_results = [
        {
            "symbol": p.symbol,
            "side": p.side,
            "score": p.score,
            "rank_score": p.rank_score,
            "rr_tp2": p.rr_tp2,
            "tp2_pct": p.tp2_pct,
            "volume_ratio": p.volume_ratio,
            "current_price": p.current_price,
            "stop_loss": p.stop_loss,
            "tp1": p.tp1,
            "tp2": p.tp2,
            "reason": p.reason,
        }
        for p in top_picks
    ]
    return TopPicksResponse(mode="sub", count=len(compact_results), top_picks=top_picks, diagnostics=diagnostics)


@app.get("/scan/symbol/{symbol}", response_model=SignalResponse)
async def scan_symbol(symbol: str, mode: str = Query("main", pattern="^(main|sub)$")):
    return await analyze_symbol(normalize_market_symbol(symbol), mode=mode)
