from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from app.config import settings
from app.models import SignalResponse
from app.services.binance_client import shutdown_http_client, startup_http_client
from app.services.scanner import analyze_symbol, scan_symbols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await startup_http_client()
    yield
    await shutdown_http_client()


app = FastAPI(
    title="길수매매법 코인 검색기",
    version="0.2.0",
    description="1시간봉 중심 RSI 다이버전스 연계 + Fib 기반 메인/서브 코인 검색기 MVP",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gilsu-scanner", "version": app.version}


@app.get("/ready")
async def ready():
    return {
        "status": "ready",
        "service": "gilsu-scanner",
        "max_symbols_per_scan": settings.max_symbols_per_scan,
        "scan_concurrency": settings.scan_concurrency,
    }


@app.get("/scan/main")
async def scan_main(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else settings.default_symbols
    return await scan_symbols(symbol_list, mode="main")


@app.get("/scan/sub")
async def scan_sub(symbols: str | None = Query(default=None, description="comma separated symbols")):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else settings.default_symbols
    return await scan_symbols(symbol_list, mode="sub")


@app.get("/scan/symbol/{symbol}", response_model=SignalResponse)
async def scan_symbol(symbol: str, mode: str = Query(default="main", pattern="^(main|sub)$")):
    try:
        return await analyze_symbol(symbol.upper(), mode=mode)  # type: ignore[arg-type]
    except Exception as exc:
        logger.exception("single symbol scan failed symbol=%s mode=%s error=%s", symbol, mode, repr(exc))
        raise HTTPException(status_code=502, detail=f"symbol scan failed: {exc}") from exc
