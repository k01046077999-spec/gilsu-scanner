from __future__ import annotations

from fastapi import FastAPI, HTTPException

from app.core.config import settings
from app.core.schemas import HealthResponse, ReadyResponse, ScanResponse
from app.services.engine import ScannerEngine

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "제이드 파동 이론 기반 업비트 스캐너. "
        "RSI 다이버전스 연계 + 피보나치 되돌림 + 직전고 돌파 조건을 모두 확인합니다."
    ),
)

engine = ScannerEngine()


@app.get('/', response_model=HealthResponse, tags=['System'])
async def root() -> HealthResponse:
    return engine.health()


@app.get('/health', response_model=HealthResponse, tags=['System'])
async def health() -> HealthResponse:
    return engine.health()


@app.get('/ready', response_model=ReadyResponse, tags=['System'])
async def ready() -> ReadyResponse:
    return engine.ready()


@app.get('/scan', response_model=ScanResponse, tags=['Scanner'],
         summary='전체 스캔',
         description=(
             "거래대금 상위 KRW 마켓을 전부 분석합니다. "
             "통과 신호는 top_picks에, 전체 결과는 signals에 담깁니다."
         ))
async def scan() -> ScanResponse:
    return await engine.scan()


@app.get('/scan/symbol/{symbol}', tags=['Scanner'],
         summary='단일 종목 분석',
         description=(
             "특정 종목 1개를 즉시 분석합니다. "
             "symbol 예: BTC, ETH, KRW-SOL"
         ))
async def scan_symbol(symbol: str):
    normalized = symbol.upper().replace('/', '').replace('_', '-')
    if not normalized.startswith('KRW-'):
        normalized = f'KRW-{normalized.replace("KRW", "").replace("-", "")}'
    result = await engine.analyze_symbol(normalized)
    if result is None:
        raise HTTPException(status_code=404, detail='signal_not_found')
    return result
