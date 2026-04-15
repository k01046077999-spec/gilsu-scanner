from __future__ import annotations

from typing import Any

import httpx
import pandas as pd

from app.core.config import settings


class UpbitClient:
    def __init__(self) -> None:
        self.base_url = settings.upbit_base_url.rstrip('/')
        self.timeout = httpx.Timeout(15.0, connect=8.0)
        self.headers = {'Accept': 'application/json'}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        async with httpx.AsyncClient(timeout=self.timeout, headers=self.headers) as client:
            res = await client.get(f'{self.base_url}{path}', params=params)
            res.raise_for_status()
            return res.json()

    async def markets(self) -> list[dict[str, Any]]:
        return await self._get('/v1/market/all', params={'isDetails': 'false'})

    async def tickers(self, markets: list[str]) -> list[dict[str, Any]]:
        if not markets:
            return []
        # 업비트 최대 100개 한 번에 조회
        chunks = [markets[i:i + 100] for i in range(0, len(markets), 100)]
        results: list[dict[str, Any]] = []
        for chunk in chunks:
            data = await self._get('/v1/ticker', params={'markets': ','.join(chunk)})
            if isinstance(data, list):
                results.extend(data)
        return results

    async def candles(self, market: str, unit: int, count: int) -> pd.DataFrame:
        """분봉 캔들 조회. unit = 15 | 60 | 240"""
        raw = await self._get(
            f'/v1/candles/minutes/{unit}',
            params={'market': market, 'count': count},
        )
        df = pd.DataFrame(raw)
        if df.empty:
            return df

        df = df.rename(columns={
            'candle_date_time_utc': 'open_time',
            'opening_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'trade_price': 'close',
            'candle_acc_trade_volume': 'volume',
            'candle_acc_trade_price': 'quote_volume',
        })
        df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
        for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.sort_values('open_time').reset_index(drop=True)
        df['close_time'] = df['open_time']
        return (
            df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume']]
            .dropna()
            .reset_index(drop=True)
        )

    async def top_markets(self, limit: int) -> list[str]:
        """거래대금 상위 KRW 마켓 목록 반환"""
        market_rows = await self.markets()
        krw_markets = [x['market'] for x in market_rows if str(x.get('market', '')).startswith('KRW-')]

        excluded = {x.strip().upper() for x in settings.exclude_markets.split(',') if x.strip()}
        krw_markets = [m for m in krw_markets if m.upper() not in excluded]

        tickers = await self.tickers(krw_markets)
        ranked: list[tuple[str, float]] = []
        for t in tickers:
            market = str(t.get('market', ''))
            acc_trade_price = float(t.get('acc_trade_price_24h') or 0.0)
            trade_price = float(t.get('trade_price') or 0.0)
            if not market.startswith('KRW-') or trade_price <= 0:
                continue
            if acc_trade_price < settings.min_daily_acc_trade_price_krw:
                continue
            ranked.append((market, acc_trade_price))

        ranked.sort(key=lambda x: x[1], reverse=True)
        return [m for m, _ in ranked[:limit]]
