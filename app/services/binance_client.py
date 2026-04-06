from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pandas as pd

from app.config import settings

BINANCE_FAPI_BASE = "https://fapi.binance.com"
logger = logging.getLogger(__name__)
_client: httpx.AsyncClient | None = None


async def startup_http_client() -> None:
    global _client
    if _client is None:
        timeout = httpx.Timeout(
            connect=settings.request_timeout_seconds,
            read=settings.request_timeout_seconds,
            write=settings.request_timeout_seconds,
            pool=settings.request_timeout_seconds,
        )
        _client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "gilsu-scanner/0.2.0"})


async def shutdown_http_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def get_http_client() -> httpx.AsyncClient:
    if _client is None:
        await startup_http_client()
    assert _client is not None
    return _client


async def _request_json(url: str, params: dict[str, Any]) -> Any:
    client = await get_http_client()
    last_error: Exception | None = None

    for attempt in range(1, settings.request_retries + 1):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_error = exc
            logger.warning(
                "binance_request_failed symbol=%s interval=%s attempt=%s error=%s",
                params.get("symbol"),
                params.get("interval"),
                attempt,
                repr(exc),
            )
            if attempt < settings.request_retries:
                await asyncio.sleep(0.6 * attempt)

    assert last_error is not None
    raise last_error


async def fetch_klines(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    data = await _request_json(url, params)

    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        raise ValueError(f"empty klines returned for {symbol} {interval}")

    for c in ["open", "high", "low", "close", "volume", "quote_asset_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df
