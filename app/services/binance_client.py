from __future__ import annotations

import asyncio
import logging

import httpx
import pandas as pd

from app.config import settings

BINANCE_FAPI_BASE = "https://fapi.binance.com"
logger = logging.getLogger(__name__)

_client: httpx.AsyncClient | None = None


def _build_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=settings.connect_timeout,
        read=settings.read_timeout,
        write=settings.read_timeout,
        pool=settings.pool_timeout,
    )


async def startup_http_client() -> None:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=_build_timeout(),
            base_url=BINANCE_FAPI_BASE,
            headers={"User-Agent": "gilsu-scanner/0.2.0"},
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )
        logger.info("http client started")


async def shutdown_http_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
        logger.info("http client closed")


def _get_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("HTTP client is not initialized")
    return _client


async def fetch_klines(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    client = _get_client()
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    last_error: Exception | None = None

    for attempt in range(1, settings.api_retries + 1):
        try:
            resp = await client.get("/fapi/v1/klines", params=params)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) < 30:
                raise ValueError(f"insufficient kline rows for {symbol} {interval}")

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
            for c in ["open", "high", "low", "close", "volume", "quote_asset_volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
            df = df.dropna(subset=["open", "high", "low", "close", "volume"]).reset_index(drop=True)
            if len(df) < 30:
                raise ValueError(f"too many NaN rows for {symbol} {interval}")
            return df
        except (httpx.TimeoutException, httpx.HTTPError, ValueError) as exc:
            last_error = exc
            logger.warning(
                "fetch_klines failed symbol=%s interval=%s attempt=%s/%s error=%s",
                symbol,
                interval,
                attempt,
                settings.api_retries,
                repr(exc),
            )
            if attempt < settings.api_retries:
                await asyncio.sleep(settings.retry_backoff_seconds * attempt)

    raise RuntimeError(f"failed to fetch klines for {symbol} {interval}: {last_error!r}")
