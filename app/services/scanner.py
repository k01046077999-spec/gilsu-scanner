from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Literal

from app.config import settings
from app.models import SignalResponse, TopPick
from app.services.upbit_client import fetch_klines, fetch_top_symbols, normalize_market_symbol
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



def _trend_guard(df, side: str) -> bool:
    closes = df["close"].tail(10)
    if len(closes) < 10:
        return True
    change_pct = (float(closes.iloc[-1]) / float(closes.iloc[0]) - 1.0) * 100.0
    if side == "bullish":
        return change_pct > -6.0
    return change_pct < 6.0



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
        stop_loss_pct = ((stop_loss / current_price) - 1.0) * 100
        tp1_pct = ((tp1 / current_price) - 1.0) * 100
        tp2_pct = ((tp2 / current_price) - 1.0) * 100
    else:
        stop_loss = float(fib_1)
        tp1 = recent_low
        tp2 = float(fib_1272) if fib_1272 is not None else current_price * 0.92
        risk = stop_loss - current_price
        reward_tp1 = current_price - tp1
        reward_tp2 = current_price - tp2
        stop_loss_pct = ((current_price / stop_loss) - 1.0) * 100
        tp1_pct = ((current_price / tp1) - 1.0) * 100
        tp2_pct = ((current_price / tp2) - 1.0) * 100

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
    fib_0382 = fib.get("0.382")
    fib_05 = fib.get("0.5")
    fib_0618 = fib.get("0.618")

    if current is None or fib_05 is None:
        reasons.append("fib/current_price_missing")
    elif mode == "main":
        if signal.side == "bullish" and current > fib_05:
            reasons.append("late_entry_above_fib_0.5")
        elif signal.side == "bearish" and current < fib_05:
            reasons.append("late_entry_below_fib_0.5")
    else:
        if fib_0382 is not None:
            if signal.side == "bullish" and current > fib_0382:
                reasons.append("late_entry_above_fib_0.382")
            elif signal.side == "bearish" and current < fib_0382:
                reasons.append("late_entry_below_fib_0.382")
        elif fib_0618 is not None:
            if signal.side == "bullish" and current > fib_0618 * 1.02:
                reasons.append("late_entry_far_above_fib_0.618")
            elif signal.side == "bearish" and current < fib_0618 * 0.98:
                reasons.append("late_entry_far_below_fib_0.618")

    min_rr = 1.25 if mode == "main" else 0.8
    if risk.get("rr_tp2", 0) < min_rr:
        reasons.append(f"rr_tp2_below_{min_rr}")

    min_tp1_pct = 1.0 if mode == "main" else 0.6
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
        score = max(score, signal.get("strength", 0.0))
        if signal.get("chain"):
            score += 8
        elif signal.get("general") and mode == "sub":
            score += 4
        if fib.get("in_zone"):
            score += 18
        elif fib.get("near_zone"):
            score += 8
        if fib.get("anchor_source") == "swing_anchored":
            score += 5
    if _volume_ok(df_1h):
        score += 10
    if not _overheated(df_1h):
        score += 5
    return score



def _build_top_picks(results: list[SignalResponse], mode: Mode) -> list[TopPick]:
    if not results:
        return []

    def rank_components(signal: SignalResponse):
        metrics = signal.metrics or {}
        risk = metrics.get("risk_management", {}) or {}
        rr = float(risk.get("rr_tp2") or 0.0)
        volume_ratio = float(metrics.get("volume_ratio") or 0.0)
        score = float(signal.score or 0.0)
        tp2_pct = float(risk.get("tp2_pct") or 0.0)
        rsi = float(metrics.get("rsi_1h") or 50.0)

        rsi_bonus = 0.0
        if signal.side == "bullish" and 28 <= rsi <= 45:
            rsi_bonus = 3.0
        elif signal.side == "bearish" and 55 <= rsi <= 72:
            rsi_bonus = 3.0

        rank_score = (rr * 50.0) + (volume_ratio * 12.0) + (score * 0.35) + min(tp2_pct, 25.0) + rsi_bonus
        return rank_score, rr, volume_ratio, tp2_pct

    eligible = []
    for signal in results:
        metrics = signal.metrics or {}
        if not metrics.get("practical_filter_passed", False):
            continue
        risk = metrics.get("risk_management", {}) or {}
        rr = float(risk.get("rr_tp2") or 0.0)
        tp2_pct = float(risk.get("tp2_pct") or 0.0)
        min_rr = 1.25 if mode == "main" else 0.8
        min_tp2_pct = 5.0 if mode == "main" else 4.0
        if rr < min_rr or tp2_pct < min_tp2_pct:
            continue
        rank_score, rr, volume_ratio, tp2_pct = rank_components(signal)
        eligible.append((signal, rank_score, rr, volume_ratio, tp2_pct))

    eligible.sort(key=lambda x: x[1], reverse=True)
    picks: list[TopPick] = []

    for signal, rank_score, rr, volume_ratio, tp2_pct in eligible[: settings.top_pick_count]:
        reason_parts = []
        if rr >= 3:
            reason_parts.append("RR 우수")
        elif rr >= 1.6:
            reason_parts.append("RR 양호")
        else:
            reason_parts.append("RR 기준 통과")
        if volume_ratio >= 1.5:
            reason_parts.append("거래량 강함")
        elif volume_ratio >= 1.0:
            reason_parts.append("거래량 무난")
        if signal.score >= 75:
            reason_parts.append("구조 우수")
        elif signal.score >= 60:
            reason_parts.append("구조 안정")
        if tp2_pct >= 12:
            reason_parts.append("목표 여유 큼")
        elif tp2_pct >= 7:
            reason_parts.append("목표 여유 확보")

        picks.append(
            TopPick(
                symbol=signal.symbol,
                side=signal.side,
                grade=signal.grade,
                score=round(float(signal.score), 2),
                rank_score=round(rank_score, 2),
                rr_tp2=round(rr, 2),
                volume_ratio=round(volume_ratio, 2),
                current_price=signal.current_price,
                stop_loss=signal.stop_loss,
                tp1=signal.tp1,
                tp2=signal.tp2,
                stop_loss_pct=round(float(signal.metrics.get("risk_management", {}).get("stop_loss_pct", 0.0)), 2),
                tp1_pct=round(float(signal.metrics.get("risk_management", {}).get("tp1_pct", 0.0)), 2),
                tp2_pct=round(float(signal.metrics.get("risk_management", {}).get("tp2_pct", 0.0)), 2),
                reason=" + ".join(reason_parts),
            )
        )
    return picks



def _score_side(primary: dict, lower: dict, higher: dict, fib: dict, df_1h, side: str, mode: Mode) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    side_kr = "상승" if side == "bullish" else "하락"

    if primary.get("chain"):
        score += 34 + min(primary.get("strength", 0.0), 18.0)
        reasons.append(f"1h {side_kr} 다이버전스 연계 감지")
    elif primary.get("general") and mode == "sub":
        if lower.get("chain") or higher.get("found"):
            score += 20 + min(primary.get("strength", 0.0), 10.0)
            reasons.append(f"1h 일반 {side_kr} 다이버전스 + 보조주기 확인")
        else:
            score += 10
            reasons.append(f"1h 일반 {side_kr} 다이버전스 감지")

    if lower.get("chain"):
        score += 14
        reasons.append("30m 다이버전스 연계 재확인")
    elif lower.get("general") and not primary.get("chain"):
        score += 6
        reasons.append("30m 일반 다이버전스 보조 확인")

    if higher.get("found"):
        score += 10
        reasons.append("4h 상위 주기 방향 확인")

    if fib.get("in_zone"):
        score += 18
        reasons.append("Fib 0.618~0.786 핵심 구간 진입")
    elif fib.get("near_zone"):
        score += 9 if mode == "main" else 14
        reasons.append("Fib 핵심 구간 인접")

    if fib.get("anchor_source") == "swing_anchored":
        score += 6
        reasons.append("Fib 스윙 기준 앵커링")

    if primary.get("extreme"):
        score += 8
        reasons.append(f"RSI 극단 구간 {('저점' if side == 'bullish' else '고점')} 확인")
    if primary.get("strong_extreme"):
        score += 4

    if _volume_ok(df_1h):
        score += 8
        reasons.append("거래량 증가 확인")
    elif mode == "sub":
        score += 3
        reasons.append("거래량 평이")
    if _resistance_room(df_1h, side, min_pct=3.0):
        score += 5
        reasons.append("목표 방향 여유 존재")
    if not _overheated(df_1h):
        score += 4
        reasons.append("최근 과열 아님")
    if not _trend_guard(df_1h, side):
        score -= 6
        reasons.append("단기 흐름 역행 부담")
    if fib.get("invalidated"):
        score = 0.0
        reasons.append("Fib 1 이탈로 무효")

    return round(score, 2), reasons


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

    bull_score, bull_reasons = _score_side(bull_1h, bull_30m, bull_4h, bull_fib, df_1h, "bullish", mode)
    bear_score, bear_reasons = _score_side(bear_1h, bear_30m, bear_4h, bear_fib, df_1h, "bearish", mode)

    chosen_side = "bullish" if bull_score >= bear_score else "bearish"
    score = max(bull_score, bear_score)
    reasons = bull_reasons if chosen_side == "bullish" else bear_reasons
    fib = bull_fib if chosen_side == "bullish" else bear_fib
    primary = bull_1h if chosen_side == "bullish" else bear_1h
    lower = bull_30m if chosen_side == "bullish" else bear_30m
    higher = bull_4h if chosen_side == "bullish" else bear_4h

    if mode == "main":
        structural_ok = primary.get("chain") and fib.get("in_zone") and not fib.get("invalidated")
        grade = "main" if structural_ok and score >= settings.main_threshold else "reject"
    else:
        structural_ok = (
            primary.get("chain")
            or (primary.get("general") and (lower.get("found") or higher.get("found")))
            or (primary.get("general") and fib.get("near_zone"))
        )
        grade = "sub" if structural_ok and score >= settings.sub_threshold and not fib.get("invalidated") else "reject"

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
        "primary_divergence": primary,
        "lower_timeframe_confirmation": lower,
        "higher_timeframe_confirmation": higher,
        "fib": {
            "anchor_source": fib.get("anchor_source"),
            "anchor_low": round(float(fib.get("anchor_low", 0)), 6) if fib.get("anchor_low") is not None else None,
            "anchor_high": round(float(fib.get("anchor_high", 0)), 6) if fib.get("anchor_high") is not None else None,
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
        symbol=normalize_market_symbol(symbol),
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
        stop_loss_pct=risk_management["stop_loss_pct"],
        tp1_pct=risk_management["tp1_pct"],
        tp2_pct=risk_management["tp2_pct"],
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
            except Exception:
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


async def scan_symbols(symbols: list[str] | None = None, mode: Mode = "main") -> tuple[list[SignalResponse], dict, list[TopPick]]:
    start = perf_counter()
    diagnostics: dict = {"mode": mode}

    if symbols:
        universe = symbols[: settings.max_symbols_per_scan]
        diagnostics["symbol_source"] = "manual"
    else:
        universe = await fetch_top_symbols(settings.universe_size)
        diagnostics["symbol_source"] = "upbit_krw_top_volume"
    diagnostics["requested_count"] = len(universe)

    candidates, pre = await _prefilter_candidates(universe, mode)
    diagnostics.update(pre)

    sem = asyncio.Semaphore(settings.scan_concurrency)
    failures: list[str] = []

    async def guarded_analyze(symbol: str):
        async with sem:
            try:
                return await analyze_symbol(symbol, mode=mode)
            except Exception:
                failures.append(symbol)
                return None

    results = await asyncio.gather(*[guarded_analyze(sym) for sym in candidates])
    clean = [r for r in results if r is not None]
    filtered = [
        r for r in clean
        if (mode == "main" and r.grade == "main") or (mode == "sub" and r.grade == "sub")
    ]
    filtered.sort(key=lambda x: (x.metrics.get("risk_management", {}).get("rr_tp2", 0), x.score), reverse=True)

    top_picks = _build_top_picks(filtered, mode)
    diagnostics.update({
        "scanned_count": len(candidates),
        "success_count": len(clean),
        "failed_count": len(failures),
        "filtered_count": len(clean) - len(filtered),
        "failed_symbols": failures[:20],
        "duration_ms": int((perf_counter() - start) * 1000),
        "top_pick_count": len(top_picks),
    })
    return filtered, diagnostics, top_picks
