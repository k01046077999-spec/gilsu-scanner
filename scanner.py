import time
from typing import Dict, List, Optional

import ccxt
import numpy as np
import pandas as pd


EXCHANGE_ID = "binance"
OHLCV_LIMIT = 250
MAX_SYMBOLS_TO_SCAN = 80

# 길수매매법 실전 필터(1차 안정화 버전)
MIN_AVG_NOTIONAL = 2_000_000          # 최근 평균 거래대금 최소치
MAX_20BAR_RISE_PCT = 25.0             # 최근 20봉 과열 제외
RESISTANCE_BUFFER_PCT = 3.0           # 최근 고점 너무 근접 제외
FIB_MIN = 0.618
FIB_MAX = 0.786


def get_exchange():
    exchange_class = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_class({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    exchange.load_markets()
    return exchange


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.bfill()

    return rsi


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int = OHLCV_LIMIT) -> pd.DataFrame:
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

    if not raw or len(raw) < 120:
        raise ValueError(f"Not enough OHLCV data for {symbol}")

    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna().reset_index(drop=True)
    df["rsi"] = compute_rsi(df["close"], 14)
    df["notional"] = df["close"] * df["volume"]

    return df


def get_candidate_symbols(exchange, max_symbols: int = MAX_SYMBOLS_TO_SCAN) -> List[str]:
    tickers = exchange.fetch_tickers()
    candidates = []

    for symbol, market in exchange.markets.items():
        try:
            if not market.get("active", False):
                continue
            if market.get("spot") is not True:
                continue
            if market.get("quote") != "USDT":
                continue

            base = market.get("base", "")
            if any(x in base for x in ["UP", "DOWN", "BULL", "BEAR"]):
                continue
            if base in {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "USDP"}:
                continue

            ticker = tickers.get(symbol, {})
            quote_volume = ticker.get("quoteVolume", 0) or 0
            last = ticker.get("last", 0) or 0

            if quote_volume <= 0 or last <= 0:
                continue

            candidates.append((symbol, float(quote_volume)))
        except Exception:
            continue

    candidates.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in candidates[:max_symbols]]


def is_pivot_low(series: pd.Series, idx: int, left: int = 3, right: int = 3) -> bool:
    if idx - left < 0 or idx + right >= len(series):
        return False

    center = series.iloc[idx]
    left_vals = series.iloc[idx - left:idx]
    right_vals = series.iloc[idx + 1:idx + 1 + right]

    return center <= left_vals.min() and center <= right_vals.min()


def get_pivot_lows(df: pd.DataFrame) -> List[int]:
    pivots = []
    for i in range(len(df)):
        if is_pivot_low(df["low"], i, left=3, right=3):
            pivots.append(i)
    return pivots


def find_bullish_divergence(df: pd.DataFrame) -> Optional[Dict]:
    pivots = get_pivot_lows(df)

    if len(pivots) < 2:
        return None

    for i in range(len(pivots) - 1, 0, -1):
        p1 = pivots[i - 1]
        p2 = pivots[i]

        if p2 - p1 < 5:
            continue

        price1 = float(df.iloc[p1]["low"])
        price2 = float(df.iloc[p2]["low"])
        rsi1 = float(df.iloc[p1]["rsi"])
        rsi2 = float(df.iloc[p2]["rsi"])

        # 일반 강세 다이버전스
        if price2 < price1 and rsi2 > rsi1:
            return {
                "type": "bullish_divergence",
                "pivot_1_index": p1,
                "pivot_2_index": p2,
                "price_low_1": round(price1, 8),
                "price_low_2": round(price2, 8),
                "rsi_low_1": round(rsi1, 2),
                "rsi_low_2": round(rsi2, 2)
            }

    return None


def check_liquidity(df: pd.DataFrame):
    recent = df.tail(24)
    avg_notional = float(recent["notional"].mean())
    return avg_notional >= MIN_AVG_NOTIONAL, round(avg_notional, 2)


def check_overheat(df: pd.DataFrame):
    if len(df) < 21:
        return False, "not_enough_data"

    prev_close = float(df.iloc[-21]["close"])
    current_close = float(df.iloc[-1]["close"])

    if prev_close <= 0:
        return False, "invalid_prev_close"

    rise_pct = ((current_close - prev_close) / prev_close) * 100
    return rise_pct <= MAX_20BAR_RISE_PCT, round(rise_pct, 2)


def check_resistance(df: pd.DataFrame):
    recent = df.tail(120)
    current_close = float(recent.iloc[-1]["close"])
    highest = float(recent["high"].max())

    if highest <= 0:
        return False, "invalid_high"

    distance_pct = ((highest - current_close) / highest) * 100
    return distance_pct > RESISTANCE_BUFFER_PCT, round(distance_pct, 2)


def check_fibonacci_zone(df: pd.DataFrame):
    recent = df.tail(120)

    swing_low = float(recent["low"].min())
    swing_high = float(recent["high"].max())
    current_close = float(recent.iloc[-1]["close"])

    if swing_high <= swing_low:
        return False, {}

    diff = swing_high - swing_low
    fib_618 = swing_high - diff * FIB_MIN
    fib_786 = swing_high - diff * FIB_MAX

    lower = min(fib_618, fib_786)
    upper = max(fib_618, fib_786)

    in_zone = lower <= current_close <= upper

    info = {
        "swing_low": round(swing_low, 8),
        "swing_high": round(swing_high, 8),
        "fib_618": round(fib_618, 8),
        "fib_786": round(fib_786, 8),
        "current_close": round(current_close, 8)
    }

    return in_zone, info


def check_rsi_extreme(divergence: Dict):
    rsi1 = divergence["rsi_low_1"]
    rsi2 = divergence["rsi_low_2"]
    return (rsi1 <= 35) or (rsi2 <= 35), {"rsi_low_1": rsi1, "rsi_low_2": rsi2}


def analyze_symbol(exchange, symbol: str, timeframe: str) -> Optional[Dict]:
    df = fetch_ohlcv_df(exchange, symbol, timeframe)
    divergence = find_bullish_divergence(df)

    if divergence is None:
        return None

    passed = []
    failed = []

    liquidity_ok, liquidity_value = check_liquidity(df)
    if liquidity_ok:
        passed.append(f"liquidity_ok:{liquidity_value}")
    else:
        failed.append(f"liquidity_fail:{liquidity_value}")

    overheat_ok, overheat_value = check_overheat(df)
    if overheat_ok:
        passed.append(f"overheat_ok:{overheat_value}")
    else:
        failed.append(f"overheat_fail:{overheat_value}")

    resistance_ok, resistance_value = check_resistance(df)
    if resistance_ok:
        passed.append(f"resistance_ok:{resistance_value}")
    else:
        failed.append(f"resistance_fail:{resistance_value}")

    fib_ok, fib_info = check_fibonacci_zone(df)
    if fib_ok:
        passed.append("fib_ok")
    else:
        failed.append("fib_fail")

    rsi_ok, rsi_info = check_rsi_extreme(divergence)
    if rsi_ok:
        passed.append("rsi_extreme_ok")
    else:
        failed.append("rsi_extreme_fail")

    core_pass = liquidity_ok and overheat_ok and resistance_ok

    if core_pass and (fib_ok or rsi_ok):
        signal = "candidate"
    elif core_pass:
        signal = "watch"
    else:
        return None

    current_price = float(df.iloc[-1]["close"])

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "signal": signal,
        "current_price": round(current_price, 8),
        "divergence": divergence,
        "fib_info": fib_info,
        "rsi_extreme_info": rsi_info,
        "passed": passed,
        "failed": failed,
        "risk_rules": {
            "stop_loss_pct": -10,
            "take_profit_1_pct": 25,
            "take_profit_2_pct": 50,
            "max_positions": 5
        }
    }


def run_gilsu_scan(limit: int = 10, timeframe: str = "1h") -> Dict:
    started_at = time.time()
    exchange = get_exchange()

    symbols = get_candidate_symbols(exchange, MAX_SYMBOLS_TO_SCAN)

    results = []
    errors = []

    for symbol in symbols:
        try:
            result = analyze_symbol(exchange, symbol, timeframe)
            if result is not None:
                results.append(result)
        except Exception as e:
            errors.append({
                "symbol": symbol,
                "error": str(e)
            })

    # candidate 우선 정렬
    signal_rank = {"candidate": 0, "watch": 1}
    results.sort(key=lambda x: (signal_rank.get(x["signal"], 99), len(x.get("failed", []))))

    elapsed = round(time.time() - started_at, 2)

    return {
        "status": "success",
        "strategy": "gilsu_trading_method",
        "timeframe": timeframe,
        "scanned_symbols": len(symbols),
        "result_count": min(len(results), limit),
        "elapsed_seconds": elapsed,
        "results": results[:limit],
        "notes": [
            "This is a stability-first deployment version.",
            "Market cap filter is replaced by liquidity filter in this version.",
            "Bullish RSI divergence is the core trigger."
        ],
        "errors_sample": errors[:10]
    }
