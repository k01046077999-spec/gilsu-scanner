from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pykrx import stock

from app.config import (
    CACHE_TTL_SEC,
    MA_LONG,
    MA_MID,
    MAX_PRICE,
    MAX_RESULTS,
    MIN_PRICE,
    MIN_TRADING_VALUE,
    OHLCV_DAYS,
    PYKRX_SLEEP_SEC,
)

EXCLUDE_KEYWORDS = [
    "스팩", "SPAC", "ETN", "ETF", "리츠", "인버스", "레버리지", "선물", "채권", "액티브", "코스피200",
]
PREFERRED_SHARE_PATTERN = re.compile(r"(우$|우B$|우C$|우선주|[0-9]우$)", re.IGNORECASE)


def ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _cache_bucket() -> int:
    return int(time.time() // CACHE_TTL_SEC)


@lru_cache(maxsize=16)
def latest_business_date_cached(bucket: int) -> str:
    for i in range(0, 14):
        d = ymd(datetime.now() - timedelta(days=i))
        try:
            df = stock.get_market_ohlcv_by_ticker(d, market="KOSPI")
            if df is not None and len(df) > 0:
                return d
        except Exception:
            continue
    return ymd(datetime.now() - timedelta(days=1))


def latest_business_date() -> str:
    return latest_business_date_cached(_cache_bucket())


@lru_cache(maxsize=8)
def get_tickers_cached(market: str, bucket: int) -> Tuple[str, ...]:
    date = latest_business_date()
    market = market.upper()
    if market == "ALL":
        tickers = stock.get_market_ticker_list(date, market="KOSPI") + stock.get_market_ticker_list(date, market="KOSDAQ")
    else:
        tickers = stock.get_market_ticker_list(date, market=market)
    return tuple(tickers)


def get_tickers(market: str = "ALL") -> List[str]:
    return list(get_tickers_cached(market.upper(), _cache_bucket()))


@lru_cache(maxsize=4096)
def safe_name(ticker: str) -> str:
    try:
        return stock.get_market_ticker_name(ticker)
    except Exception:
        return ticker


def is_excluded_name(name: str) -> bool:
    upper = name.upper().replace(" ", "")
    if any(k.upper() in upper for k in EXCLUDE_KEYWORDS):
        return True
    return bool(PREFERRED_SHARE_PATTERN.search(upper))


@lru_cache(maxsize=512)
def fetch_ohlcv_cached(ticker: str, bucket: int, days: int = OHLCV_DAYS) -> Tuple[Tuple[str, float, float, float, float, float, float], ...]:
    end = datetime.now()
    start = end - timedelta(days=days * 2 + 30)
    df = stock.get_market_ohlcv_by_date(ymd(start), ymd(end), ticker)
    time.sleep(PYKRX_SLEEP_SEC)
    if df is None or df.empty:
        return tuple()
    df = df.rename(
        columns={"시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume", "거래대금": "trading_value"}
    )
    required = ["open", "high", "low", "close", "volume", "trading_value"]
    for col in required:
        if col not in df.columns:
            df[col] = 0
    df = df[required].dropna().tail(days).copy()
    rows = []
    for idx, row in df.iterrows():
        rows.append((idx.strftime("%Y-%m-%d"), float(row.open), float(row.high), float(row.low), float(row.close), float(row.volume), float(row.trading_value)))
    return tuple(rows)


def fetch_ohlcv(ticker: str) -> pd.DataFrame:
    rows = fetch_ohlcv_cached(ticker, _cache_bucket(), OHLCV_DAYS)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume", "trading_value"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    return df


def pct(a: float, b: float) -> float:
    if b == 0 or math.isnan(a) or math.isnan(b):
        return 0.0
    return (a / b - 1.0) * 100.0


def local_extrema(series: pd.Series, kind: str = "low", window: int = 3) -> List[int]:
    values = series.to_numpy(dtype=float)
    idxs: List[int] = []
    n = len(values)
    for i in range(window, n - window):
        left = values[i - window:i]
        right = values[i + 1:i + window + 1]
        cur = values[i]
        if kind == "low":
            if cur <= np.min(left) and cur <= np.min(right):
                idxs.append(i)
        else:
            if cur >= np.max(left) and cur >= np.max(right):
                idxs.append(i)
    return idxs


def atr_percent(df: pd.DataFrame, period: int = 14) -> float:
    if len(df) < period + 2:
        return 0.0
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(period).mean().iloc[-1])
    close = float(c.iloc[-1])
    return atr / close * 100 if close else 0.0


def detect_w_bottom(df: pd.DataFrame, lookback: int = 100) -> Tuple[bool, Dict[str, Any]]:
    """
    쌍바닥 판정.
    사람 눈의 W패턴을 아래 조건으로 수치화한다.
    1) 두 저점 간격 10~65거래일
    2) 두 저점 가격 차이 -12%~+15% 이내
    3) 두 저점 사이 목선 반등폭 8% 이상
    4) 현재가가 2저점 대비 3% 이상 반등
    5) 현재가가 목선에 너무 멀리 과열되지 않음
    """
    recent = df.tail(lookback).copy()
    if len(recent) < 55:
        return False, {"ok": False, "reason": "데이터 부족", "score": 0}

    lows = local_extrema(recent["low"], "low", window=3)
    if len(lows) < 2:
        return False, {"ok": False, "reason": "의미 있는 저점 2개 부족", "score": 0}

    close_now = float(recent["close"].iloc[-1])
    atrp = max(3.0, atr_percent(recent))
    best: Optional[Dict[str, Any]] = None

    for x in range(len(lows) - 1):
        for y in range(x + 1, len(lows)):
            i, j = lows[x], lows[y]
            gap = j - i
            if gap < 10 or gap > 65:
                continue
            low1 = float(recent["low"].iloc[i])
            low2 = float(recent["low"].iloc[j])
            if low1 <= 0 or low2 <= 0:
                continue
            low2_vs_low1_pct = pct(low2, low1)
            if not (-12.0 <= low2_vs_low1_pct <= 15.0):
                continue

            middle = recent.iloc[i:j + 1]
            neckline = float(middle["high"].max())
            neckline_pos = int(middle["high"].to_numpy().argmax()) + i
            if not (i < neckline_pos < j):
                continue
            neckline_rebound_pct = pct(neckline, min(low1, low2))
            if neckline_rebound_pct < 8.0:
                continue

            rebound2_pct = pct(close_now, low2)
            neckline_distance_pct = pct(close_now, neckline)
            # 목선을 이미 35% 이상 멀리 이탈한 종목은 농사 초입이 아니라 추격 구간으로 간주
            not_chasing = close_now <= neckline * 1.35
            # 2저점이 너무 최근이면 미확인, 최소 3봉은 지나야 함
            bars_after_low2 = len(recent) - 1 - j
            confirmed_after_low2 = bars_after_low2 >= 3

            score = 0.0
            score += max(0, 25 - abs(low2_vs_low1_pct) * 1.4)
            score += min(25, neckline_rebound_pct * 1.4)
            score += min(20, max(0, rebound2_pct) * 1.5)
            score += 10 if confirmed_after_low2 else 0
            score += 10 if not_chasing else -10
            score += 10 if close_now >= neckline * 0.90 else 0
            # 변동성이 큰 종목은 저점 오차를 조금 더 허용하되 점수는 낮춘다
            if abs(low2_vs_low1_pct) <= atrp * 1.8:
                score += 5

            cand = {
                "ok": bool(rebound2_pct >= 3 and confirmed_after_low2 and not_chasing and score >= 55),
                "score": round(score, 2),
                "low1_date": recent.index[i].strftime("%Y-%m-%d"),
                "low2_date": recent.index[j].strftime("%Y-%m-%d"),
                "gap_days": int(gap),
                "low1": round(low1, 2),
                "low2": round(low2, 2),
                "low2_vs_low1_pct": round(low2_vs_low1_pct, 2),
                "neckline": round(neckline, 2),
                "neckline_date": recent.index[neckline_pos].strftime("%Y-%m-%d"),
                "neckline_rebound_pct": round(neckline_rebound_pct, 2),
                "rebound_from_low2_pct": round(rebound2_pct, 2),
                "distance_to_neckline_pct": round(neckline_distance_pct, 2),
                "bars_after_low2": int(bars_after_low2),
                "interpretation": "쌍바닥 후보: 두 저점 유사 + 중간 목선 반등 + 2저점 후 반등 확인",
            }
            if best is None or cand["score"] > best["score"]:
                best = cand

    if best is None:
        return False, {"ok": False, "reason": "쌍바닥 가격/간격/목선 조건 미충족", "score": 0}
    return bool(best["ok"]), best


def detect_gonguri(df: pd.DataFrame, lookback: int = 130) -> Tuple[bool, Dict[str, Any]]:
    """
    공구리 판정.
    직전 고점 돌파 이후 그 고점 부근을 깨지 않고 지지하는지 확인한다.
    1) 최근 15~45일 사이 돌파 발생
    2) 돌파 기준선은 그 이전 45~110일 구간의 피벗 고점/박스권 상단
    3) 돌파봉 종가가 기준선 1.5% 이상 상회
    4) 돌파봉 거래량이 20일 평균 대비 1.2배 이상
    5) 이후 저가/종가가 기준선 -5% 이내에서 버팀
    6) 현재가가 기준선 대비 25% 이상 과열이면 제외
    """
    recent = df.tail(lookback).copy()
    if len(recent) < 90:
        return False, {"ok": False, "reason": "데이터 부족", "score": 0}

    close = recent["close"].to_numpy(dtype=float)
    high = recent["high"].to_numpy(dtype=float)
    low = recent["low"].to_numpy(dtype=float)
    volume = recent["volume"].to_numpy(dtype=float)
    current_close = float(close[-1])

    best: Optional[Dict[str, Any]] = None
    # 최근 45일 안에서 돌파봉을 찾되, 너무 최근 1~2봉은 지지 확인이 부족하므로 감점
    for b in range(len(recent) - 45, len(recent) - 2):
        if b < 50:
            continue
        base_start = max(0, b - 90)
        base_end = b - 5
        base = recent.iloc[base_start:base_end]
        if len(base) < 35:
            continue

        # 기준선: 피벗 고점이 있으면 상위 피벗 고점, 없으면 박스권 고점
        highs_idx = local_extrema(base["high"], "high", window=3)
        if highs_idx:
            resistance = float(np.percentile(base["high"].iloc[highs_idx], 85))
        else:
            resistance = float(base["high"].max())
        if resistance <= 0:
            continue

        breakout_close_pct = pct(float(close[b]), resistance)
        breakout_high_pct = pct(float(high[b]), resistance)
        if breakout_close_pct < 1.5 and breakout_high_pct < 3.0:
            continue

        vol20 = float(np.mean(volume[max(0, b - 20):b])) if b > 5 else 0
        vol_ratio = float(volume[b] / vol20) if vol20 > 0 else 0
        if vol_ratio < 1.05:
            continue

        after_low = float(np.min(low[b + 1:]))
        after_close_min = float(np.min(close[b + 1:]))
        after_high = float(np.max(high[b:]))
        support_by_low_pct = pct(after_low, resistance)
        support_by_close_pct = pct(after_close_min, resistance)
        current_distance_pct = pct(current_close, resistance)
        days_after_breakout = len(recent) - 1 - b

        support_ok = support_by_close_pct >= -5.0 and current_close >= resistance * 0.97
        not_failed = after_close_min >= resistance * 0.93
        not_overheated = current_close <= resistance * 1.25
        enough_time = days_after_breakout >= 3

        score = 0.0
        score += min(25, max(0, breakout_close_pct) * 3.5)
        score += min(20, vol_ratio * 8)
        score += 20 if support_ok else max(0, 15 + support_by_close_pct)
        score += 10 if not_failed else -15
        score += 10 if not_overheated else -20
        score += 10 if enough_time else 2
        score += 5 if -3 <= current_distance_pct <= 15 else 0

        cand = {
            "ok": bool(score >= 58 and support_ok and not_failed and not_overheated and enough_time),
            "score": round(score, 2),
            "resistance": round(resistance, 2),
            "breakout_date": recent.index[b].strftime("%Y-%m-%d"),
            "breakout_close": round(float(close[b]), 2),
            "breakout_close_pct": round(breakout_close_pct, 2),
            "breakout_high_pct": round(breakout_high_pct, 2),
            "breakout_volume_ratio_20d": round(vol_ratio, 2),
            "days_after_breakout": int(days_after_breakout),
            "after_low": round(after_low, 2),
            "after_low_vs_resistance_pct": round(support_by_low_pct, 2),
            "after_min_close_vs_resistance_pct": round(support_by_close_pct, 2),
            "current_close": round(current_close, 2),
            "current_vs_resistance_pct": round(current_distance_pct, 2),
            "after_high": round(after_high, 2),
            "support_ok": bool(support_ok),
            "not_overheated": bool(not_overheated),
            "interpretation": "공구리 후보: 직전 고점 돌파 후 기준선 부근 지지 확인",
        }
        if best is None or cand["score"] > best["score"]:
            best = cand

    if best is None:
        return False, {"ok": False, "reason": "돌파봉/거래량/지지 조건 미충족", "score": 0}
    return bool(best["ok"]), best


def base_long_ma_conditions(df: pd.DataFrame) -> Dict[str, Any]:
    close_now = float(df["close"].iloc[-1])
    ma224 = float(df[f"ma{MA_LONG}"].iloc[-1])
    ma112 = float(df[f"ma{MA_MID}"].iloc[-1])
    below_ratio_100 = float((df["close"].tail(100) < df[f"ma{MA_LONG}"].tail(100)).mean())
    below_ratio_180 = float((df["close"].tail(180) < df[f"ma{MA_LONG}"].tail(180)).mean())
    gap_pct = pct(close_now, ma224)
    under_or_near = close_now <= ma224 * 1.08 and close_now >= ma224 * 0.55
    long_below = below_ratio_100 >= 0.60 or below_ratio_180 >= 0.55
    return {
        "close": close_now,
        "ma112": ma112,
        "ma224": ma224,
        "ma224_gap_pct": gap_pct,
        "under_224": close_now < ma224,
        "near_224": close_now <= ma224 * 1.08,
        "under_or_near_224_zone": under_or_near,
        "below_224_ratio_100d": below_ratio_100,
        "below_224_ratio_180d": below_ratio_180,
        "long_below_224": long_below,
    }


def analyze_ticker(ticker: str, include_reject_reasons: bool = False) -> Optional[Dict[str, Any]]:
    name = safe_name(ticker)
    if is_excluded_name(name):
        return {"ticker": ticker, "name": name, "matched": False, "reject_reason": "ETF/ETN/스팩/우선주 등 제외"} if include_reject_reasons else None

    df = fetch_ohlcv(ticker)
    if df.empty or len(df) < MA_LONG + 40:
        return {"ticker": ticker, "name": name, "matched": False, "reject_reason": "OHLCV 데이터 부족"} if include_reject_reasons else None

    df[f"ma{MA_MID}"] = df["close"].rolling(MA_MID).mean()
    df[f"ma{MA_LONG}"] = df["close"].rolling(MA_LONG).mean()
    df = df.dropna().copy()
    if len(df) < 80:
        return {"ticker": ticker, "name": name, "matched": False, "reject_reason": "이평선 계산 후 데이터 부족"} if include_reject_reasons else None

    close_now = float(df["close"].iloc[-1])
    if close_now < MIN_PRICE or close_now > MAX_PRICE:
        return {"ticker": ticker, "name": name, "matched": False, "reject_reason": "가격 필터 미통과"} if include_reject_reasons else None

    avg_tv20 = float(df["trading_value"].tail(20).mean())
    if avg_tv20 < MIN_TRADING_VALUE:
        return {"ticker": ticker, "name": name, "matched": False, "reject_reason": "20일 평균 거래대금 부족"} if include_reject_reasons else None

    ma_cond = base_long_ma_conditions(df)
    w_ok, w = detect_w_bottom(df)
    g_ok, g = detect_gonguri(df)

    vol20 = float(df["volume"].tail(20).mean())
    vol60 = float(df["volume"].tail(60).mean())
    vol_ratio = vol20 / vol60 if vol60 else 0
    tv_score = min(10, avg_tv20 / 1_000_000_000)

    a_type = bool(ma_cond["under_or_near_224_zone"] and ma_cond["long_below_224"] and w_ok and g_ok)
    b_type = bool(ma_cond["under_or_near_224_zone"] and ma_cond["long_below_224"] and (w_ok or g_ok))

    if not (a_type or b_type):
        if include_reject_reasons:
            return {
                "ticker": ticker,
                "name": name,
                "matched": False,
                "reject_reason": "농사매매 핵심 조합 미충족",
                "conditions": {
                    "ma": ma_cond,
                    "w_bottom": w,
                    "gonguri": g,
                    "avg_trading_value_20d": int(avg_tv20),
                },
            }
        return None

    score = 0.0
    score += 25 if a_type else 12
    score += min(25, float(w.get("score", 0)) * 0.35) if w_ok else 0
    score += min(25, float(g.get("score", 0)) * 0.35) if g_ok else 0
    score += 10 if ma_cond["under_224"] else 6
    score += min(8, max(0, vol_ratio * 4))
    score += min(7, tv_score)

    buy_zone = {
        "primary": "기준선 부근 분할 접근. A타입은 1%, B타입은 0.5% 이하 탐색.",
        "risk_cut": "-30% 1회 물타기 전략을 쓰더라도, 감사의견/관리종목/거래정지 리스크는 별도 제외 필요.",
        "avoid": "공구리 기준선 대비 +25% 이상 벌어진 종목은 추격 금지.",
    }

    return {
        "ticker": ticker,
        "name": name,
        "matched": True,
        "type": "A" if a_type else "B",
        "score": round(score, 2),
        "close": round(close_now, 2),
        "ma112": round(ma_cond["ma112"], 2),
        "ma224": round(ma_cond["ma224"], 2),
        "ma224_gap_pct": round(ma_cond["ma224_gap_pct"], 2),
        "under_224": bool(ma_cond["under_224"]),
        "near_224": bool(ma_cond["near_224"]),
        "below_224_ratio_100d": round(ma_cond["below_224_ratio_100d"], 2),
        "below_224_ratio_180d": round(ma_cond["below_224_ratio_180d"], 2),
        "avg_trading_value_20d": int(avg_tv20),
        "volume_ratio_20d_vs_60d": round(vol_ratio, 2),
        "w_bottom": bool(w_ok),
        "w_detail": w,
        "gonguri": bool(g_ok),
        "gonguri_detail": g,
        "position_rule": "A타입: 자산 1% 매수 후보" if a_type else "B타입: 자산 0.5% 탐색 매수 후보",
        "buy_zone_note": buy_zone,
    }


def scan(market: str = "ALL", limit: int = 120, main_only: bool = False) -> Dict[str, Any]:
    tickers = get_tickers(market)
    limit = max(1, int(limit))
    tickers = tickers[:limit]
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    started = time.time()
    for ticker in tickers:
        try:
            item = analyze_ticker(ticker)
            if not item:
                continue
            if main_only and item.get("type") != "A":
                continue
            results.append(item)
        except Exception as e:
            errors.append({"ticker": ticker, "error": str(e)[:180]})
            continue

    results = sorted(results, key=lambda x: x.get("score", 0), reverse=True)[:MAX_RESULTS]
    return {
        "service": "nongsa-scanner",
        "as_of": latest_business_date(),
        "market": market.upper(),
        "scan_mode": "main_A_only" if main_only else "all_A_and_B",
        "scanned_count": len(tickers),
        "result_count": len(results),
        "elapsed_sec": round(time.time() - started, 2),
        "results": results,
        "errors_count": len(errors),
        "errors_sample": errors[:5],
        "logic_summary": {
            "A": "224일선 근처/아래 + 장기간 224일선 아래 + 쌍바닥 + 공구리",
            "B": "224일선 근처/아래 + 장기간 224일선 아래 + 쌍바닥 또는 공구리 중 하나",
            "liquidity": f"20일 평균 거래대금 {MIN_TRADING_VALUE:,}원 이상",
        },
        "disclaimer": "투자 추천이 아닌 조건 검색 결과입니다. 재무, 공시, 뉴스, 관리종목, 감사의견 리스크는 최종 매수 전 별도 확인해야 합니다.",
    }
