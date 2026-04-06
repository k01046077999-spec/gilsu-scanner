from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Literal

from app.config import settings
from app.models import SignalResponse
from app.services.binance_client import fetch_klines, fetch_top_symbols
from app.services.divergence import (
    detect_bearish_divergence_chain,
    detect_bullish_divergence_chain,
)
from app.services.fibonacci import bearish_fib_zone, bullish_fib_zone
from app.services.indicators import enrich_indicators
from app.services.swings import find_swings, latest_swing_highs, latest_swing_lows

Mode = Literal["main", "sub"]


class ScanError(Exception):
    pass


def _volume_ok(df) -> bool:
    row = df.iloc[-1]
    if row["vol_ma_20"] == 0 or row["vol_ma_20"] != row["vol_ma_20"]:
        return False
    return row["vol_ma_5"] > row["vol_ma_20"] * 1.05



def _overheated(df) -> bool:
    val = float(df["pct_from_20_low"].iloc[-1])
    return val >= 28.0



def _resistance_room(df, side: str, min_pct: float = 3.0) -> bool:
    current = float(df["close"].iloc[-1])
    if side == "bullish":
        recent_high = float(df["high"].tail(40).max())
        room_pct = (recent_high / current - 1.0) * 100
        return room_pct >= min_pct
    recent_low = float(df["low"].tail(40).min())
    room_pct = (current / recent_low - 1.0) * 100
    return room_pct >= min_pct



def _calc_risk_management(current_price: float, fib: dict, chosen_side: str, df_1h):
    fib_0_5 = fib.get("fib_0_5")
    fib_1 = fib.get("fib_1")
    fib_1272 = fib.get("fib_1272")
    recent_high = float(df_1h["high"].tail(40).max())
    recent_low = float(df_1h["low"].tail(40).min())

    if chosen_side == "bullish":
        stop_loss = float(fib_1)
        tp1 = recent_high
        tp2 = float(fib_1272) if fib_1272 is not None else current_price * 1.08
        risk = current_price - stop_loss
        reward_tp1 = tp1 - current_price
        reward_tp2 = tp2 - current_price
    else:
        stop_loss = float(fib_1)
        tp1 = recent_low
        tp2 = float(fib_1272) if fib_1272 is not None else current_price * 0.92
        risk = stop_loss - current_price
        reward_tp1 = current_price - tp1
        reward_tp2 = current_price - tp2

    stop_loss_pct = ((stop_loss / current_price) - 1.0) * 100 if chosen_side == "bullish" else ((current_price / stop_loss) - 1.0) * 100
    tp1_pct = ((tp1 / current_price) - 1.0) * 100 if chosen_side == "bullish" else ((current_price / tp1) - 1.0) * 100
    tp2_pct = ((tp2 / current_price) - 1.0) * 100 if chosen_side == "bullish" else ((current_price / tp2) - 1.0) * 100
    rr_tp1 = reward_tp1 / risk if risk > 0 else 0.0
    rr_tp2 = reward_tp2 / risk if risk > 0 else 0.0

    return {
        "entry_reference": round(current_price, 6),
        "stop_loss": round(stop_loss, 6),
        "stop_loss_pct": round(stop_loss_pct, 2),
        "tp1": round(tp1, 6),
        "tp1_pct": round(tp1_pct, 2),
        "tp2": round(tp2, 6),
        "tp2_pct": round(tp2_pct, 2),
        "rr_tp1": round(rr_tp1, 2),
        "rr_tp2": round(rr_tp2, 2),
        "invalidation_rule": "fib_1_break",
        "fib_entry_reference": round(float(fib_0_5 or current_price), 6),
    }



def _passes_practical_filter(signal: SignalResponse, mode: Mode) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    metrics = signal.metrics
    risk = metrics.get("risk_management", {})
    fib = metrics.get("fib", {})
    current = metrics.get("current_price")
    fib_05 = fib.get("0.5")

    if current is None or fib_05 is None:
        reasons.append("fib/current_price_missing")
    elif signal.side == "bullish" and current > fib_05:
        reasons.append("late_entry_above_fib_0.5")
    elif signal.side == "bearish" and current < fib_05:
        reasons.append("late_entry_below_fib_0.5")

    min_rr = 1.2 if mode == "main" else 0.9
    if risk.get("rr_tp2", 0) < min_rr:
        reasons.append(f"rr_tp2_below_{min_rr}")

    min_tp1_pct = 1.0
    if risk.get("tp1_pct", 0) < min_tp1_pct:
        reasons.append(f"tp1_pct_below_{min_tp1_pct}")

    return len(reasons) == 0, reasons



def _prefilter_score(df_1h, mode: Mode) -> float:
    score = 0.0
    lows = latest_swing_lows(df_1h, 4)
    highs = latest_swing_highs(df_1h, 4)
    bull = detect_bullish_divergence_chain(lows)
    bear = detect_bearish_divergence_chain(highs)
    fib_bull = bullish_fib_zone(df_1h)
    fib_bear = bearish_fib_zone(df_1h)

    for signal, fib in ((bull, fib_bull), (bear, fib_bear)):
        if signal.get("chain"):
            score = max(score, 40)
        elif signal.get("general") and mode == "sub":
            score = max(score, 24)
        if signal.get("extreme"):
            score += 10
        if fib.get("in_zone"):
            score += 20
        elif fib.get("near_zone"):
            score += 10
    if _volume_ok(df_1h):
        score += 10
    if not _overheated(df_1h):
        score += 5
    return score


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
        bull_score += 12
        bull_reasons.append("30m 보조 확인")
    if bull_4h.get("found"):
        bull_score += 12
        bull_reasons.append("4h 상위 주기 방향 확인")
    if bull_fib.get("in_zone"):
        bull_score += 15
        bull_reasons.append("Fib 0.618~0.786 핵심 구간 진입")
    elif bull_fib.get("near_zone"):
        bull_score += 8
        bull_reasons.append("Fib 핵심 구간 인접")
    if bull_1h.get("extreme"):
        bull_score += 10
        bull_reasons.append("RSI 극단 구간 저점 확인")
    if _volume_ok(df_1h):
        bull_score += 8
        bull_reasons.append("거래량 증가 확인")
    if _resistance_room(df_1h, "bullish", min_pct=3.0):
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
        bear_score += 12
        bear_reasons.append("30m 보조 확인")
    if bear_4h.get("found"):
        bear_score += 12
        bear_reasons.append("4h 상위 주기 방향 확인")
    if bear_fib.get("in_zone"):
        bear_score += 15
        bear_reasons.append("Fib 0.618~0.786 핵심 구간 진입")
    elif bear_fib.get("near_zone"):
        bear_score += 8
        bear_reasons.append("Fib 핵심 구간 인접")
    if bear_1h.get("extreme"):
        bear_score += 10
        bear_reasons.append("RSI 극단 구간 고점 확인")
    if _volume_ok(df_1h):
        bear_score += 8
        bear_reasons.append("거래량 증가 확인")
    if _resistance_room(df_1h, "bearish", min_pct=3.0):
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

    grade = "main" if mode == "main" and score >= 62 else "sub" if mode == "sub" and score >= 42 else "reject"
    entry_zone = [round(x, 6) for x in fib.get("entry_zone", [])] if fib.get("entry_zone") else None
    risk_management = _calc_risk_management(current_price, fib, chosen_side, df_1h)

    metrics = {
        "bull_score": round(bull_score, 2),
        "bear_score": round(bear_score, 2),
        "current_price": round(current_price, 6),
        "rsi_1h": round(float(df_1h["rsi"].iloc[-1]), 2),
        "volume_ratio": round(float(df_1h["vol_ma_5"].iloc[-1] / df_1h["vol_ma_20"].iloc[-1]), 2)
        if float(df_1h["vol_ma_20"].iloc[-1] or 0) != 0
        else None,
        "pct_from_20_low": round(float(df_1h["pct_from_20_low"].iloc[-1]), 2),
        "fib": {
            "0.382": round(float(fib.get("fib_382", 0)), 6) if fib.get("fib_382") else None,
            "0.5": round(float(fib.get("fib_0_5", 0)), 6) if fib.get("fib_0_5") else None,
            "0.618": round(float(fib.get("fib_618", 0)), 6) if fib.get("fib_618") else None,
            "0.786": round(float(fib.get("fib_786", 0)), 6) if fib.get("fib_786") else None,
            "1.0": round(float(fib.get("fib_1", 0)), 6) if fib.get("fib_1") else None,
            "1.272": round(float(fib.get("fib_1272", 0)), 6) if fib.get("fib_1272") else None,
            "1.618": round(float(fib.get("fib_1618", 0)), 6) if fib.get("fib_1618") else None,
        },
        "risk_management": risk_management,
    }

    signal = SignalResponse(
        symbol=symbol,
        timeframe="1h",
        mode=mode,
        side=chosen_side,
        grade=grade,
        score=round(score, 2),
        entry_zone=entry_zone,
        stop_loss=risk_management["stop_loss"],
        tp1=risk_management["tp1"],
        tp2=risk_management["tp2"],
        current_price=round(current_price, 6),
        reasons=reasons,
        metrics=metrics,
    )
    passed, filter_reasons = _passes_practical_filter(signal, mode)
    signal.metrics["practical_filter_passed"] = passed
    signal.metrics["practical_filter_reasons"] = filter_reasons
    if not passed:
        signal.grade = "reject"
    return signal


async def _prefilter_candidates(symbols: list[str], mode: Mode) -> tuple[list[str], dict]:
    sem = asyncio.Semaphore(settings.scan_concurrency)
    failures: list[str] = []

    async def score_symbol(symbol: str):
        async with sem:
            try:
                tf_1h = await fetch_klines(symbol, "1h", settings.prefilter_limit)
                df_1h = find_swings(enrich_indicators(tf_1h, settings.rsi_period), settings.swing_window)
                score = _prefilter_score(df_1h, mode)
                return symbol, score
            except Exception:  # noqa: BLE001
                failures.append(symbol)
                return symbol, -1.0

    scored = await asyncio.gather(*[score_symbol(sym) for sym in symbols])
    ranked = sorted(scored, key=lambda x: x[1], reverse=True)
    selected = [sym for sym, score in ranked[: settings.prefilter_size] if score > 0]
    return selected, {
        "prefilter_requested": len(symbols),
        "prefilter_selected": len(selected),
        "prefilter_failed": len(failures),
        "prefilter_failed_symbols": failures[:20],
    }


async def scan_symbols(symbols: list[str] | None = None, mode: Mode = "main") -> tuple[list[SignalResponse], dict]:
    start = perf_counter()
    diagnostics: dict = {"mode": mode}

    if symbols:
        universe = symbols[: settings.max_symbols_per_scan]
        diagnostics["symbol_source"] = "manual"
    else:
        universe = await fetch_top_symbols(settings.universe_size)
        diagnostics["symbol_source"] = "binance_top_volume"
    diagnostics["requested_count"] = len(universe)

    candidates, pre = await _prefilter_candidates(universe, mode)
    diagnostics.update(pre)

    sem = asyncio.Semaphore(settings.scan_concurrency)
    failures: list[str] = []

    async def guarded_analyze(symbol: str):
        async with sem:
            try:
                return await analyze_symbol(symbol, mode=mode)
            except Exception:  # noqa: BLE001
                failures.append(symbol)
                return None

    results = await asyncio.gather(*[guarded_analyze(sym) for sym in candidates])
    clean = [r for r in results if r is not None]
    filtered = [
        r for r in clean
        if (mode == "main" and r.grade == "main") or (mode == "sub" and r.grade == "sub")
    ]
    filtered.sort(key=lambda x: (x.metrics.get("risk_management", {}).get("rr_tp2", 0), x.score), reverse=True)

    diagnostics.update({
        "scanned_count": len(candidates),
        "success_count": len(clean),
        "failed_count": len(failures),
        "filtered_count": len(clean) - len(filtered),
        "failed_symbols": failures[:20],
        "duration_ms": int((perf_counter() - start) * 1000),
    })
    return filtered, diagnostics
