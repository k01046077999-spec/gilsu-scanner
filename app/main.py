from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import APP_VERSION, DEFAULT_MARKET, SCAN_LIMIT
from app.scanner import analyze_ticker, scan

app = FastAPI(
    title="Nongsa Scanner API",
    description="224일선·쌍바닥·공구리 기반 농사매매 후보 스캐너",
    version=APP_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "service": "nongsa-scanner",
        "version": APP_VERSION,
        "status": "ok",
        "endpoints": [
            "/health",
            "/scan/main?market=ALL&limit=120",
            "/scan/all?market=ALL&limit=120",
            "/analyze/{ticker}",
            "/debug/{ticker}",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok", "version": APP_VERSION}


@app.get("/scan/main")
def scan_main(
    market: str = Query(DEFAULT_MARKET, pattern="^(ALL|KOSPI|KOSDAQ)$"),
    limit: int = Query(SCAN_LIMIT, ge=1, le=300),
):
    return scan(market=market, limit=limit, main_only=True)


@app.get("/scan/all")
def scan_all(
    market: str = Query(DEFAULT_MARKET, pattern="^(ALL|KOSPI|KOSDAQ)$"),
    limit: int = Query(SCAN_LIMIT, ge=1, le=300),
):
    return scan(market=market, limit=limit, main_only=False)


@app.get("/analyze/{ticker}")
def analyze(ticker: str):
    if not ticker.isdigit() or len(ticker) != 6:
        raise HTTPException(status_code=400, detail="ticker must be a 6-digit KRX code")
    result = analyze_ticker(ticker, include_reject_reasons=False)
    if result is None:
        return {"ticker": ticker, "matched": False, "message": "조건 미충족 또는 데이터 부족"}
    return result


@app.get("/debug/{ticker}")
def debug(ticker: str):
    if not ticker.isdigit() or len(ticker) != 6:
        raise HTTPException(status_code=400, detail="ticker must be a 6-digit KRX code")
    result = analyze_ticker(ticker, include_reject_reasons=True)
    return result or {"ticker": ticker, "matched": False, "message": "분석 불가"}
