from __future__ import annotations

from fastapi import FastAPI, HTTPException

from app.core.config import settings
from app.core.schemas import HealthResponse, ReadyResponse, ScanResponse
from app.services.engine import ScannerEngine

app = FastAPI(title=settings.app_name, version=settings.app_version)
engine = ScannerEngine()


@app.get('/', response_model=HealthResponse)
async def root() -> HealthResponse:
    return engine.health()


@app.get('/health', response_model=HealthResponse)
async def health() -> HealthResponse:
    return engine.health()


@app.get('/ready', response_model=ReadyResponse)
async def ready() -> ReadyResponse:
    return engine.ready()


@app.get('/scan/final', response_model=ScanResponse)
async def scan_final() -> ScanResponse:
    return await engine.scan()


@app.get('/scan/symbol/{symbol}')
async def scan_symbol(symbol: str):
    normalized = symbol.upper().replace('/', '').replace('_', '-')
    if not normalized.startswith('KRW-'):
        normalized = f'KRW-{normalized.replace("KRW", "").replace("-", "")}'
    result = await engine.analyze_symbol(normalized)
    if result is None:
        raise HTTPException(status_code=404, detail='signal_not_found')
    return result
