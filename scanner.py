import math
import time
import traceback
from typing import Dict, List, Optional, Tuple

import ccxt
import numpy as np
import pandas as pd


# =========================
# CONFIG
# =========================
EXCHANGE_ID = "binance"
DEFAULT_TIMEFRAME = "1h"
OHLCV_LIMIT = 260

# 유동성 필터: 최근 24개 봉 기준 평균 거래대금(종가 * 거래량)
MIN_NOTIONAL_24H = 1_500_000

# 과도한 상승 제외: 최근 20봉 상승률
MAX_20BAR_PCT_RISE = 22.0

# 상단 저항 인접 제외: 최근 120봉 최고가 대비 현재가 위치
RESISTANCE_BUFFER_PCT = 3.0

# 피보나치 되돌림 허용 구간
FIB_LOW = 0.618
FIB_HIGH = 0.786

# 피벗 탐지
PIVOT_LEFT = 3
PIVOT_RIGHT = 3

# 상위 코인 수집 개수
MAX_SYMBOLS_TO_SCAN = 120


# =========================
# EXCHANGE
# =========================
def get_exchange() -> ccxt.Exchange:
    exchange_class = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_class({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    exchange.load_markets()
    return exchange


# =========================
# DATA
# =========================
def fetch_ohlcv_df(exchange: ccxt.Exchange, symbol: str, timeframe: str, limit: int = OHLCV_LIMIT) -> pd.DataFrame:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not ohlcv or len(ohlcv) < 100:
        raise ValueError(f"Not enough OHLCV data for {symbol}")

    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.dropna().reset_index(drop=True)
    return df


def get_candidate_symbols(exchange: ccxt.Exchange, max_symbols: int = MAX_SYMBOLS_TO_SCAN) -> List[str]:
    markets = exchange.markets
    tickers = exchange.fetch_tickers()

    candidates = []

    for symbol, market in markets.items():
        try:
            if not market.get("active", False):
                continue
            if market.get("spot") is not True:
                continue
            if market.get("quote") != "USDT":
                continue

            # 레버리지 토큰, 스테이블 위주, 잡토큰 일부 제외
            base = market.get("base", "")
            if any(x in base for x in ["UP", "DOWN", "BULL", "BEAR"]):
                continue
            if base in {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP"}:
                continue

            ticker = tickers.get(symbol, {})
            quote_volume = ticker.get("quoteVolume", 0) or 0
            last = ticker.get("last", 0) or 0

            if quote_volume <= 0 or last <= 0:
                continue

            candidates.append({
                "symbol": symbol,
                "quote_volume": float(quote_volume)
            })
        except Exception:
            continue

    candidates = sorted(candidates, key=lambda x: x["quote_volume"], reverse=True)
    return [x["symbol"] for x in candidates[:max_symbols]]


# =========================
# INDICATORS
# =========================
def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.fillna(method="bfill")
    return rsi


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["rsi"] = compute_rsi(out["close"], period=14)
    out["notional"] = out["close"] * out["volume"]
    return out


# =========================
# PIVOTS / DIVERGENCE
# =========================
def is_pivot_low(values: pd.Series, idx: int, left: int = PIVOT_LEFT, right: int = PIVOT_RIGHT) -> bool:
    if idx - left < 0 or idx + right >= len(values):
        return False

    center = values.iloc[idx]
    left_slice = values.iloc[idx - left: idx]
    right_slice = values.iloc[idx + 1: idx + 1 + right]

    return center <= left_slice.min() and center <= right_slice.min()


def find_pivot_lows(df: pd.DataFrame, col: str = "low") -> List[int]:
    pivots = []
    for i in range(len(df)):
        if is_pivot_low(df[col], i):
            pivots.append(i)
    return pivots


def find_recent_bullish_divergence(df: pd.DataFrame) -> Optional[Dict]:
    """
    일반 강세 다이버전스:
    - 가격 저점은 낮아짐 (lower low)
    - RSI 저점은 높아짐 (higher low)
    """
    pivots = find_pivot_lows(df, col="low")
    if len(pivots) < 2:
        return None

    # 최근 pivot lows 중 마지막 2개 비교
    for i in range(len(pivots) - 1, 0, -1):
        p2 = pivots[i]
        p1 = pivots[i - 1]

        if p2 - p1 < 5:
            continue

        price1 = float(df.iloc[p1]["low"])
        price2 = float(df.iloc[p2]["low"])
        rsi1 = float(df.iloc[p1]["rsi"])
        rsi2 = float(df.iloc[p2]["rsi"])

        # lower low + higher low
        if price2 < price1 and rsi2 > rsi1:
            return {
                "pivot_1_index": p1,
                "pivot_2_index": p2,
                "price_low_1": round(price1, 6),
                "price_low_2": round(price2, 6),
                "rsi_low_1": round(rsi1, 2),
                "rsi_low_2": round(rsi2, 2)
            }

    return None


# =========================
# FILTERS
# =========================
def pass_liquidity_filter(df: pd.DataFrame) -> Tuple[bool, str]:
    recent = df.tail(24)
    avg_notional = float(recent["notional"].mean())
    passed = avg_notional >= MIN_NOTIONAL_24H
    reason = f"avg_notional_24h={avg_notional:,.0f}"
    return passed, reason


def pass_overheat_filter(df: pd.DataFrame) -> Tuple[bool, str]:
    if len(df) < 21:
        return False, "not_enough_data_for_overheat_check"

    prev_close = float(df.iloc[-21]["close"])
    current_close = float(df.iloc[-1]["close"])
    pct_rise = ((current_close - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0

    passed = pct_rise <= MAX_20BAR_PCT_RISE
    reason = f"20bar_rise_pct={pct_rise:.2f}"
    return passed, reason


def pass_resistance_filter(df: pd.DataFrame) -> Tuple[bool, str]:
    if len(df) < 120:
        return False, "not_enough_data_for_resistance_check"

    current_close = float(df.iloc[-1]["close"])
    rolling_high = float(df["high"].tail(120).max())

    if rolling_high <= 0:
        return False, "invalid_rolling_high"

    distance_pct = ((rolling_high - current_close) / rolling_high) * 100
    passed = distance_pct > RESISTANCE_BUFFER_PCT
    reason = f"distance_to_120bar_high_pct={distance_pct:.2f}"
    return passed, reason


def fib_retracement_zone(df: pd.DataFrame, lookback: int = 120) -> Tuple[bool, str, Dict]:
    """
    최근 lookback 구간의 스윙 하이/로우 기준으로
    현재가가 0.618~0.786 되돌림 구간에 있는지 체크
    """
    if len(df) < lookback:
        return False, "not_enough_data_for_fib_check", {}

    recent = df.tail(lookback).reset_index(drop=True)
    swing_low = float(recent["low"].min())
    swing_high = float(recent["high"].max())
    current_close = float(recent.iloc[-1]["close"])

    if swing_high <= swing_low:
        return False, "invalid_swing_range", {}

    range_size = swing_high - swing_low
    fib_618 = swing_high - range_size * FIB_LOW
    fib_786 = swing_high - range_size * FIB_HIGH

    lower_bound = min(fib_618, fib_786)
    upper_bound = max(fib_618, fib_786)

    in_zone = lower_bound <= current_close <= upper_bound

    details = {
        "swing_low": round(swing_low, 6),
        "swing_high": round(swing_high, 6),
        "fib_618": round(fib_618, 6),
        "fib_786": round(fib_786, 6),
        "current_close": round(current_close, 6)
    }
    return in_zone, f"fib_zone={lower_bound:.6f}~{upper_bound:.6f}", details


def pass_rsi_extreme_bias(df: pd.DataFrame, divergence: Dict) -> Tuple[bool, str]:
    """
    사용자가 말한 '중간 구간보다 과매도 쾅 구간의 뾰족한 컨펌'을 반영.
    pivot 시점 RSI 중 적어도 하나가 35 이하이면 통과.
    """
    rsi1 = divergence["rsi_low_1"]
    rsi2 = divergence["rsi_low_2"]

    passed = (rsi1 <= 35) or (rsi2 <= 35)
    reason = f"pivot_rsi=({rsi1},{rsi2})"
    return passed, reason


# =========================
# SCORING / DECISION
# =========================
def analyze_symbol(exchange: ccxt.Exchange, symbol: str, timeframe: str) -> Optional[Dict]:
    df = fetch_ohlcv_df(exchange, symbol, timeframe=timeframe, limit=OHLCV_LIMIT)
    df = add_indicators(df)

    divergence = find_recent_bullish_divergence(df)
    if divergence is None:
        return None

    reasons_pass = []
    reasons_fail = []

    # 1) divergence
    reasons_pass.append("bullish_divergence_detected")

    # 2) RSI extreme bias
    rsi_ok, rsi_reason = pass_rsi_extreme_bias(df, divergence)
    if rsi_ok:
        reasons_pass.append(f"rsi_extreme_ok:{rsi_reason}")
    else:
        reasons_fail.append(f"rsi_extreme_fail:{rsi_reason}")

    # 3) liquidity
    liquidity_ok, liquidity_reason = pass_liquidity_filter(df)
    if liquidity_ok:
        reasons_pass.append(f"liquidity_ok:{liquidity_reason}")
    else:
        reasons_fail.append(f"liquidity_fail:{liquidity_reason}")

    # 4) overheat
    overheat_ok, overheat_reason = pass_overheat_filter(df)
    if overheat_ok:
        reasons_pass.append(f"overheat_ok:{overheat_reason}")
    else:
        reasons_fail.append(f"overheat_fail:{overheat_reason}")

    # 5) resistance
    resistance_ok, resistance_reason = pass_resistance_filter(df)
    if resistance_ok:
        reasons_pass.append(f"resistance_ok:{resistance_reason}")
    else:
        reasons_fail.append(f"resistance_fail:{resistance_reason}")

    # 6) fibonacci zone
    fib_ok, fib_reason, fib_details = fib_retracement_zone(df, lookback=120)
    if fib_ok:
        reasons_pass.append(f"fib_ok:{fib_reason}")
    else:
        reasons_fail.append(f"fib_fail:{fib_reason}")

    current_price = float(df.iloc[-1]["close"])

    # 실전판 길수매매법 판정
    # 필수: divergence + liquidity + overheat + resistance
    # 가점: fib zone, extreme RSI
    core_pass = liquidity_ok and overheat_ok and resistance_ok
    setup_quality = sum([rsi_ok, fib_ok])

    if core_pass and setup_quality >= 1:
        signal = "candidate"
    elif core_pass:
        signal = "watch"
    else:
        signal = "reject"

    if signal == "reject":
        return None

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "signal": signal,
        "current_price": round(current_price, 8),
        "divergence": divergence,
        "passes": reasons_pass,
        "fails": reasons_fail,
        "fib": fib_details,
        "risk_rules": {
            "stop_loss_pct": -10,
            "take_profit_1_pct": 25,
            "take_profit_2_pct": 50,
            "max_positions": 5
        }
    }


def sort_results(results: List[Dict]) -> List[Dict]:
    """
    단순 정렬:
    1) candidate 우선
    2) fail 적은 순
    3) pass 많은 순
    """
    signal_rank = {
        "candidate": 0,
        "watch": 1
    }
    return sorted(
        results,
        key=lambda x: (
            signal_rank.get(x["signal"], 99),
            len(x.get("fails", [])),
            -len(x.get("passes", []))
        )
    )


# =========================
# MAIN SCAN
# =========================
def run_gilsu_scan(limit: int = 15, timeframe: str = DEFAULT_TIMEFRAME, market: str = "spot") -> Dict:
    start_ts = time.time()
    exchange = get_exchange()

    symbols = get_candidate_symbols(exchange, max_symbols=MAX_SYMBOLS_TO_SCAN)

    results = []
    errors = []

    for symbol in symbols:
        try:
            analyzed = analyze_symbol(exchange, symbol, timeframe)
            if analyzed:
                results.append(analyzed)
        except Exception as e:
            errors.append({
                "symbol": symbol,
                "error": str(e)
            })
            continue

    results = sort_results(results)[:limit]
    elapsed = round(time.time() - start_ts, 2)

    return {
        "status": "success",
        "strategy": "gilsu_trading_method",
        "market": market,
        "timeframe": timeframe,
        "scanned_symbols": len(symbols),
        "result_count": len(results),
        "elapsed_seconds": elapsed,
        "results": results,
        "notes": [
            "Market cap filter is approximated by liquidity filter in this version.",
            "Primary divergence logic is bullish RSI divergence on recent pivot lows.",
            "Designed for deployment stability first, rule sophistication second."
        ],
        "errors_sample": errors[:10]
    }
