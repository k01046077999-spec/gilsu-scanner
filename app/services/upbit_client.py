from __future__ import annotations

import asyncio
import random
from typing import Any

import httpx
import pandas as pd

from app.config import settings

UPBIT_BASE = "https://api.upbit.com"
_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()
_request_gate = asyncio.Lock()
_last_request_ts = 0.0
MIN_REQUEST_GAP_SEC = 0.22
DEFAULT_RETRIES = 5


def normalize_market_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if symbol.startswith("KRW-"):
        return symbol
    if symbol.startswith("USDT-") or symbol.startswith("BTC-"):
        asset = symbol.split("-", 1)[1]
        return f"KRW-{asset}"
    if symbol.endswith("USDT"):
        return f"KRW-{symbol[:-4]}"
    return f"KRW-{symbol}"


async def get_client() -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None:
            _client = httpx.AsyncClient(
                timeout=settings.request_timeout,
                headers={"User-Agent": "gilsu-scanner/0.8.0"},
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )
        return _client


async def close_client() -> None:
    global _client
    async with _client_lock:
        if _client is not None:
            await _client.aclose()
            _client = None


async def _paced_get(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
    global _last_request_ts
    async with _request_gate:
        now = asyncio.get_running_loop().time()
        wait = MIN_REQUEST_GAP_SEC - (now - _last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        resp = await client.get(url, params=params)
        _last_request_ts = asyncio.get_running_loop().time()
        return resp


async def _get_json(path: str, params: dict[str, Any] | None = None, retries: int = DEFAULT_RETRIES) -> Any:
    client = await get_client()
    url = f"{UPBIT_BASE}{path}"
    last_err: Exception | None = None
    backoff = 1.0

    for attempt in range(retries):
        try:
            resp = await _paced_get(client, url, params=params)
            if resp.status_code == 429:
                raise RuntimeError("rate_limit_429")
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt == retries - 1:
                break

            sleep_for = backoff + random.uniform(0.1, 0.5)
            if isinstance(exc, RuntimeError) and str(exc) == "rate_limit_429":
                sleep_for = max(sleep_for, 2.4 + attempt * 0.9)
            elif isinstance(exc, httpx.HTTPStatusError) and exc.response is not None and exc.response.status_code in {429, 500, 502, 503, 504}:
                sleep_for = max(sleep_for, 2.0 + attempt * 0.7)
            elif isinstance(exc, (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError, httpx.RemoteProtocolError)):
                sleep_for = max(sleep_for, 1.4 + attempt * 0.5)
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 1.6, 6.0)

    assert last_err is not None
    raise last_err


async def fetch_klines(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    market = normalize_market_symbol(symbol)
    interval_map = {
        "30m": ("/v1/candles/minutes/30", "candle_date_time_utc"),
        "1h": ("/v1/candles/minutes/60", "candle_date_time_utc"),
        "4h": ("/v1/candles/minutes/240", "candle_date_time_utc"),
        "1d": ("/v1/candles/days", "candle_date_time_utc"),
    }
    if interval not in interval_map:
        raise ValueError(f"Unsupported interval: {interval}")
    path, time_key = interval_map[interval]
    data = await _get_json(path, params={"market": market, "count": limit})
    if not data:
        raise ValueError(f"No candle data for {market}")

    df = pd.DataFrame(data)
    df = df.rename(columns={
        "opening_price": "open",
        "high_price": "high",
        "low_price": "low",
        "trade_price": "close",
        "candle_acc_trade_volume": "volume",
        "candle_acc_trade_price": "quote_asset_volume",
    })
    for c in ["open", "high", "low", "close", "volume", "quote_asset_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df[time_key], utc=True)
    df = df.sort_values("open_time").reset_index(drop=True)
    df["close_time"] = df["open_time"]
    return df[["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume"]]


async def fetch_top_symbols(limit: int = 100) -> list[str]:
    markets, tickers = await asyncio.gather(
        _get_json("/v1/market/all", params={"isDetails": "false"}),
        _get_json("/v1/ticker/all", params={"quote_currencies": "KRW"}),
    )

    tradable = {m["market"] for m in markets if str(m.get("market", "")).startswith("KRW-")}
    blocked_suffixes = {"BTC", "ETH"}
    ranked: list[tuple[str, float]] = []
    for row in tickers:
        market = row.get("market")
        if market not in tradable:
            continue
        asset = market.split("-", 1)[1]
        if asset in blocked_suffixes:
            continue
        try:
            acc_trade_price_24h = float(row.get("acc_trade_price_24h", 0.0))
            trade_price = float(row.get("trade_price", 0.0))
            signed_change_rate = abs(float(row.get("signed_change_rate", 0.0))) * 100.0
        except (TypeError, ValueError):
            continue
        if acc_trade_price_24h <= 0 or trade_price <= 0:
            continue
        if signed_change_rate > 35.0:
            continue
        liquidity_score = acc_trade_price_24h * (1.0 - min(signed_change_rate, 30.0) / 180.0)
        ranked.append((market, liquidity_score))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in ranked[:limit]]
