from __future__ import annotations

import asyncio
from typing import Literal

from app.config import settings
from app.models import SignalResponse
from app.services.binance_client import fetch_klines
from app.services.divergence import (
    detect_bearish_divergence_chain,
    detect_bullish_divergence_chain,
)
from app.services.fibonacci import bearish_fib_zone, bullish_fib_zone
from app.services.indicators import enrich_indicators
from app.services.swings import find_swings, latest_swing_highs, latest_swing_lows

Mode = Literal["main", "sub"]


def _volume_ok(df) -> bool:
    row = df.iloc[-1]
    if row["vol_ma_20"] == 0 or row["vol_ma_20"] != row["vol_ma_20"]:
        return False
    return row["vol_ma_5"] > row["vol_ma_20"] * 1.1



def _overheated(df) -> bool:
    val = float(df["pct_from_20_low"].iloc[-1])
    return val >= 25.0



def _resistance_room(df, side: str) -> bool:
    current = float(df["close"].iloc[-1])
    if side == "bullish":
        recent_high = float(df["high"].tail(40).max())
        room_pct = (recent_high / current - 1.0) * 100
        return room_pct >= 5.0
    recent_low = float(df["low"].tail(40).min())
    room_pct = (current / recent_low - 1.0) * 100
    return room_pct >= 5.0


async def analyze_symbol(symbol: str, mode: Mode = "main") -> SignalResponse:
    tf_1h, tf_30m, tf_4h = await asyncio.gather(
        fetch_klines(symbol, "1h", settings.default_limit),
        fetch_klines(symbol, "30m", settings.default_limit),
        fetch_klines(symbol, "4h", settings.default_limit),
    )

    df_1h = find_swings(enrich_indicators(tf_1h, settings.rsi_period), settings.swing_window)
    df_30m = find_swings(enrich_indicators(tf_30m, settings.rsi_period), settings.swing_window)
    df_4h = find_swings(enrich_indicators(tf_4h, settings.rsi_period), settings.swing_window)

    bull_1h = detect_bullish_divergence_chain(latest_swing_lows(df_1h, 4))
    bear_1h = detect_bearish_divergence_chain(latest_swing_highs(df_1h, 4))
    bull_30m = detect_bullish_divergence_chain(latest_swing_lows(df_30m, 4))
    bear_30m = detect_bearish_divergence_chain(latest_swing_highs(df_30m, 4))
    bull_4h = detect_bullish_divergence_chain(latest_swing_lows(df_4h, 3))
    bear_4h = detect_bearish_divergence_chain(latest_swing_highs(df_4h, 3))

    current_price = float(df_1h["close"].iloc[-1])

    bull_fib = bullish_fib_zone(df_1h)
    bear_fib = bearish_fib_zone(df_1h)

    bull_score = 0.0
    bull_reasons: list[str] = []
    if bull_1h.get("chain"):
        bull_score += 30
        bull_reasons.append("1h 상승 다이버전스 연계 감지")
    elif bull_1h.get("general") and mode == "sub":
        bull_score += 18
        bull_reasons.append("1h 일반 상승 다이버전스 감지")

    if bull_30m.get("found"):
        bull_score += 15
        bull_reasons.append("30m 보조 확인")
    if bull_4h.get("found"):
        bull_score += 15
        bull_reasons.append("4h 상위 주기 방향 확인")
    if bull_fib.get("in_zone"):
        bull_score += 15
        bull_reasons.append("Fib 0.618~0.786 핵심 구간 진입")
    elif bull_fib.get("near_zone") and mode == "sub":
        bull_score += 8
        bull_reasons.append("Fib 핵심 구간 인접")
    if bull_1h.get("extreme"):
        bull_score += 10
        bull_reasons.append("RSI 극단 구간 저점 확인")
    if _volume_ok(df_1h):
        bull_score += 5
        bull_reasons.append("거래량 증가 확인")
    if _resistance_room(df_1h, "bullish"):
        bull_score += 5
        bull_reasons.append("상단 저항 여유 존재")
    if not _overheated(df_1h):
        bull_score += 5
        bull_reasons.append("최근 과열 아님")
    if bull_fib.get("invalidated"):
        bull_score = 0
        bull_reasons.append("Fib 1 이탈로 무효")

    bear_score = 0.0
    bear_reasons: list[str] = []
    if bear_1h.get("chain"):
        bear_score += 30
        bear_reasons.append("1h 하락 다이버전스 연계 감지")
    elif bear_1h.get("general") and mode == "sub":
        bear_score += 18
        bear_reasons.append("1h 일반 하락 다이버전스 감지")

    if bear_30m.get("found"):
        bear_score += 15
        bear_reasons.append("30m 보조 확인")
    if bear_4h.get("found"):
        bear_score += 15
        bear_reasons.append("4h 상위 주기 방향 확인")
    if bear_fib.get("in_zone"):
        bear_score += 15
        bear_reasons.append("Fib 0.618~0.786 핵심 구간 진입")
    elif bear_fib.get("near_zone") and mode == "sub":
        bear_score += 8
        bear_reasons.append("Fib 핵심 구간 인접")
    if bear_1h.get("extreme"):
        bear_score += 10
        bear_reasons.append("RSI 극단 구간 고점 확인")
    if _volume_ok(df_1h):
        bear_score += 5
        bear_reasons.append("거래량 증가 확인")
    if _resistance_room(df_1h, "bearish"):
        bear_score += 5
        bear_reasons.append("하단 여유 존재")
    if not _overheated(df_1h):
        bear_score += 5
        bear_reasons.append("최근 과열 아님")
    if bear_fib.get("invalidated"):
        bear_score = 0
        bear_reasons.append("Fib 1 이탈로 무효")

    chosen_side = "bullish" if bull_score >= bear_score else "bearish"
    score = max(bull_score, bear_score)
    reasons = bull_reasons if chosen_side == "bullish" else bear_reasons
    fib = bull_fib if chosen_side == "bullish" else bear_fib

    if mode == "main":
        grade = "main" if score >= 70 else "reject"
    else:
        grade = "sub" if score >= 45 else "reject"

    entry_zone = [round(x, 6) for x in fib.get("entry_zone", [])] if fib.get("entry_zone") else None
    stop_loss = round(float(fib.get("fib_1")), 6) if fib.get("fib_1") is not None else None

    if chosen_side == "bullish":
        tp1 = round(current_price * 1.25, 6)
        tp2 = round(current_price * 1.50, 6)
    else:
        tp1 = round(current_price * 0.75, 6)
        tp2 = round(current_price * 0.50, 6)

    metrics = {
        "bull_score": bull_score,
        "bear_score": bear_score,
        "current_price": current_price,
        "rsi_1h": round(float(df_1h["rsi"].iloc[-1]), 2),
        "volume_ratio": round(float(df_1h["vol_ma_5"].iloc[-1] / df_1h["vol_ma_20"].iloc[-1]), 2)
        if float(df_1h["vol_ma_20"].iloc[-1] or 0) != 0
        else None,
        "pct_from_20_low": round(float(df_1h["pct_from_20_low"].iloc[-1]), 2),
        "fib": {
            "0.618": round(float(fib.get("fib_618", 0)), 6) if fib.get("fib_618") else None,
            "0.786": round(float(fib.get("fib_786", 0)), 6) if fib.get("fib_786") else None,
            "1.0": round(float(fib.get("fib_1", 0)), 6) if fib.get("fib_1") else None,
        },
    }

    return SignalResponse(
        symbol=symbol,
        timeframe="1h",
        mode=mode,
        side=chosen_side,
        grade=grade,
        score=round(score, 2),
        entry_zone=entry_zone,
        stop_loss=stop_loss,
        tp1=tp1,
        tp2=tp2,
        current_price=round(current_price, 6),
        reasons=reasons,
        metrics=metrics,
    )


async def scan_symbols(symbols: list[str], mode: Mode = "main") -> list[SignalResponse]:
    tasks = [analyze_symbol(sym, mode=mode) for sym in symbols[: settings.max_symbols_per_scan]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    clean: list[SignalResponse] = []
    for result in results:
        if isinstance(result, Exception):
            continue
        if mode == "main" and result.grade == "main":
            clean.append(result)
        elif mode == "sub" and result.grade == "sub":
            clean.append(result)
    clean.sort(key=lambda x: x.score, reverse=True)
    return clean
