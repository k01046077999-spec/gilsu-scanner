
from __future__ import annotations

import asyncio
import logging
import time
from typing import Literal

from app.config import settings
from app.models import RiskManagement, ScanDiagnostics, SignalResponse
from app.services.binance_client import fetch_klines
from app.services.divergence import (
    detect_bearish_divergence_chain,
    detect_bullish_divergence_chain,
)
from app.services.fibonacci import bearish_fib_zone, bullish_fib_zone
from app.services.indicators import enrich_indicators
from app.services.swings import find_swings, latest_swing_highs, latest_swing_lows

Mode = Literal["main", "sub"]
logger = logging.getLogger(__name__)
_SCAN_CACHE: dict[tuple[str, tuple[str, ...]], dict] = {}


class SymbolAnalysisError(Exception):
    def __init__(self, symbol: str, reason: str):
        self.symbol = symbol
        self.reason = reason
        super().__init__(f"{symbol}: {reason}")


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


def _safe_pct(entry: float | None, target: float | None, side: str) -> float | None:
    if entry is None or target is None or entry <= 0 or target <= 0:
        return None
    if side == "bullish":
        return round(((target / entry) - 1.0) * 100, 2)
    return round(((entry / target) - 1.0) * 100, 2)


def _safe_rr(entry: float | None, stop: float | None, target: float | None, side: str) -> float | None:
    if entry is None or stop is None or target is None:
        return None
    if side == "bullish":
        risk = entry - stop
        reward = target - entry
    else:
        risk = stop - entry
        reward = entry - target
    if risk <= 0:
        return None
    return round(reward / risk, 2)


def _build_risk_management(current_price: float, fib: dict, side: str) -> RiskManagement:
    stop_loss = float(fib.get("fib_1")) if fib.get("fib_1") is not None else None

    if side == "bullish":
        tp1 = float(fib.get("swing_high")) if fib.get("swing_high") is not None else None
        tp2 = float(fib.get("ext_1272")) if fib.get("ext_1272") is not None else None
    else:
        tp1 = float(fib.get("swing_low")) if fib.get("swing_low") is not None else None
        tp2 = float(fib.get("ext_1272")) if fib.get("ext_1272") is not None else None

    return RiskManagement(
        entry_reference=round(current_price, 6),
        stop_loss=round(stop_loss, 6) if stop_loss is not None else None,
        stop_loss_pct=_safe_pct(current_price, stop_loss, side),
        tp1=round(tp1, 6) if tp1 is not None else None,
        tp1_pct=_safe_pct(current_price, tp1, side),
        tp2=round(tp2, 6) if tp2 is not None else None,
        tp2_pct=_safe_pct(current_price, tp2, side),
        rr_tp1=_safe_rr(current_price, stop_loss, tp1, side),
        rr_tp2=_safe_rr(current_price, stop_loss, tp2, side),
        invalidation_rule="fib_1_break",
    )


def _entry_too_late(current_price: float, fib: dict, side: str) -> bool:
    fib_618 = fib.get("fib_618")
    if fib_618 is None:
        return False
    fib_618 = float(fib_618)
    if side == "bullish":
        return current_price > fib_618
    return current_price < fib_618


def _practical_trade_filter(current_price: float, fib: dict, risk: RiskManagement, side: str, mode: Mode) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    if _entry_too_late(current_price, fib, side):
        reasons.append("진입이 Fib 0.618 바깥으로 늦어져 제외")

    min_rr_tp2 = settings.main_min_rr_tp2 if mode == "main" else settings.sub_min_rr_tp2
    min_tp1_pct = settings.main_min_tp1_pct if mode == "main" else settings.sub_min_tp1_pct

    if risk.rr_tp2 is None or risk.rr_tp2 < min_rr_tp2:
        reasons.append(f"RR 부족(tp2<{min_rr_tp2})")

    tp1_pct_abs = abs(risk.tp1_pct) if risk.tp1_pct is not None else None
    if tp1_pct_abs is None or tp1_pct_abs < min_tp1_pct:
        reasons.append(f"1차 익절 여유 부족(tp1<{min_tp1_pct}%)")

    stop_pct_abs = abs(risk.stop_loss_pct) if risk.stop_loss_pct is not None else None
    if stop_pct_abs is None or stop_pct_abs <= 0:
        reasons.append("손절 구조 계산 불가")

    if side == "bearish" and not settings.allow_short_signals:
        reasons.append("숏 신호 비활성화")

    return len(reasons) == 0, reasons


async def analyze_symbol(symbol: str, mode: Mode = "main") -> SignalResponse:
    try:
        tf_1h, tf_30m, tf_4h = await asyncio.gather(
            fetch_klines(symbol, "1h", settings.default_limit),
            fetch_klines(symbol, "30m", settings.default_limit),
            fetch_klines(symbol, "4h", settings.default_limit),
        )
    except Exception as exc:
        raise SymbolAnalysisError(symbol, f"fetch_failed: {exc}") from exc

    try:
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
        risk = _build_risk_management(current_price, fib, chosen_side)
        practical_ok, practical_fail_reasons = _practical_trade_filter(current_price, fib, risk, chosen_side, mode)
        if grade != "reject" and not practical_ok:
            grade = "reject"
            reasons = reasons + practical_fail_reasons

        metrics = {
            "bull_score": bull_score,
            "bear_score": bear_score,
            "current_price": current_price,
            "rsi_1h": round(float(df_1h["rsi"].iloc[-1]), 2),
            "volume_ratio": round(float(df_1h["vol_ma_5"].iloc[-1] / df_1h["vol_ma_20"].iloc[-1]), 2)
            if float(df_1h["vol_ma_20"].iloc[-1] or 0) != 0
            else None,
            "pct_from_20_low": round(float(df_1h["pct_from_20_low"].iloc[-1]), 2),
            "practical_filter_passed": practical_ok,
            "practical_filter_reasons": practical_fail_reasons,
            "fib": {
                "0.382": round(float(fib.get("fib_382", 0)), 6) if fib.get("fib_382") else None,
                "0.5": round(float(fib.get("fib_5", 0)), 6) if fib.get("fib_5") else None,
                "0.618": round(float(fib.get("fib_618", 0)), 6) if fib.get("fib_618") else None,
                "0.786": round(float(fib.get("fib_786", 0)), 6) if fib.get("fib_786") else None,
                "1.0": round(float(fib.get("fib_1", 0)), 6) if fib.get("fib_1") else None,
                "1.272": round(float(fib.get("ext_1272", 0)), 6) if fib.get("ext_1272") else None,
                "1.618": round(float(fib.get("ext_1618", 0)), 6) if fib.get("ext_1618") else None,
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
            stop_loss=risk.stop_loss,
            tp1=risk.tp1,
            tp2=risk.tp2,
            current_price=round(current_price, 6),
            reasons=reasons,
            metrics=metrics,
            risk_management=risk,
        )
    except Exception as exc:
        raise SymbolAnalysisError(symbol, f"analysis_failed: {exc}") from exc


async def _analyze_with_semaphore(semaphore: asyncio.Semaphore, symbol: str, mode: Mode) -> SignalResponse:
    async with semaphore:
        return await analyze_symbol(symbol, mode=mode)


async def scan_symbols(symbols: list[str], mode: Mode = "main") -> tuple[list[SignalResponse], ScanDiagnostics]:
    requested_symbols = [sym.strip().upper() for sym in symbols[: settings.max_symbols_per_scan]]
    cache_key = (mode, tuple(requested_symbols))
    now = time.monotonic()
    cached = _SCAN_CACHE.get(cache_key)
    if cached and now - cached["ts"] <= settings.cache_ttl_seconds:
        diagnostics: ScanDiagnostics = cached["diagnostics"].model_copy(update={"cache_hit": True})
        return cached["results"], diagnostics

    start = time.perf_counter()
    semaphore = asyncio.Semaphore(settings.scan_concurrency)
    tasks = [_analyze_with_semaphore(semaphore, sym, mode=mode) for sym in requested_symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    clean: list[SignalResponse] = []
    failed_symbols: list[str] = []
    success_count = 0

    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            symbol = requested_symbols[idx]
            failed_symbols.append(symbol)
            logger.warning("scan_symbol_failed symbol=%s mode=%s error=%s", symbol, mode, repr(result))
            continue

        success_count += 1
        if mode == "main" and result.grade == "main":
            clean.append(result)
        elif mode == "sub" and result.grade == "sub":
            clean.append(result)

    clean.sort(key=lambda x: (x.risk_management.rr_tp2 or 0, x.score), reverse=True)
    duration_ms = int((time.perf_counter() - start) * 1000)
    diagnostics = ScanDiagnostics(
        requested_count=len(requested_symbols),
        scanned_count=len(results),
        success_count=success_count,
        failed_count=len(failed_symbols),
        filtered_count=max(success_count - len(clean), 0),
        failed_symbols=failed_symbols,
        duration_ms=duration_ms,
        cache_hit=False,
    )

    _SCAN_CACHE[cache_key] = {"ts": now, "results": clean, "diagnostics": diagnostics}
    return clean, diagnostics
