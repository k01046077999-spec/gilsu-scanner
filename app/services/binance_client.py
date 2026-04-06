from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pandas as pd

from app.config import settings

BINANCE_FAPI_BASE = "https://fapi.binance.com"
_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()


async def get_client() -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None:
            _client = httpx.AsyncClient(timeout=settings.request_timeout)
        return _client


async def close_client() -> None:
    global _client
    async with _client_lock:
        if _client is not None:
            await _client.aclose()
            _client = None


async def _get_json(path: str, params: dict[str, Any] | None = None, retries: int = 3) -> Any:
    client = await get_client()
    url = f"{BINANCE_FAPI_BASE}{path}"
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt == retries - 1:
                break
            await asyncio.sleep(0.7 * (attempt + 1))
    assert last_err is not None
    raise last_err


async def fetch_klines(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    data = await _get_json(
        "/fapi/v1/klines",
        params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
    )
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
    return df


async def fetch_top_symbols(limit: int = 100) -> list[str]:
    exchange_info, tickers = await asyncio.gather(
        _get_json("/fapi/v1/exchangeInfo"),
        _get_json("/fapi/v1/ticker/24hr"),
    )

    tradable = {
        item["symbol"]
        for item in exchange_info.get("symbols", [])
        if item.get("quoteAsset") == "USDT"
        and item.get("contractType") == "PERPETUAL"
        and item.get("status") == "TRADING"
    }

    ranked: list[tuple[str, float]] = []
    for row in tickers:
        symbol = row.get("symbol")
        if symbol not in tradable:
            continue
        if not symbol.endswith("USDT"):
            continue
        if symbol.endswith(("BULLUSDT", "BEARUSDT", "UPUSDT", "DOWNUSDT")):
            continue
        try:
            quote_volume = float(row.get("quoteVolume", 0.0))
            last_price = float(row.get("lastPrice", 0.0))
            price_change_pct = abs(float(row.get("priceChangePercent", 0.0)))
        except (TypeError, ValueError):
            continue
        if quote_volume <= 0 or last_price <= 0:
            continue
        ranked.append((symbol, quote_volume * (1.0 - min(price_change_pct, 40.0) / 200.0)))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in ranked[:limit]]
