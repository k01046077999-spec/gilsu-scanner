import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
UPBIT_CANDLE_URL = "https://api.upbit.com/v1/candles/minutes/60"
UPBIT_TICKER_URL = "https://api.upbit.com/v1/ticker"

TOP_N_BY_TRADE_VALUE = 30


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def get_now_kst():
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")


def get_upbit_markets():
    resp = requests.get(UPBIT_MARKET_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return [x["market"] for x in data if x["market"].startswith("KRW-")]


def get_ticker_map(markets):
    result = {}
    for group in chunked(markets, 50):
        resp = requests.get(
            UPBIT_TICKER_URL,
            params={"markets": ",".join(group)},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        for item in data:
            result[item["market"]] = item
    return result


def get_top_markets_by_trade_value(markets, ticker_map, top_n=TOP_N_BY_TRADE_VALUE):
    ranked = []
    for market in markets:
        t = ticker_map.get(market, {})
        trade_value_24h = float(t.get("acc_trade_price_24h", 0))
        ranked.append((market, trade_value_24h))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in ranked[:top_n]]


def get_candles(market, count=60):
    resp = requests.get(
        UPBIT_CANDLE_URL,
        params={"market": market, "count": count},
        timeout=10
    )
    resp.raise_for_status()
    data = resp.json()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df = df[[
        "candle_date_time_kst",
        "trade_price",
        "candle_acc_trade_volume",
        "candle_acc_trade_price"
    ]].copy()

    df.columns = ["time", "close", "volume", "trade_value"]
    df = df.iloc[::-1].reset_index(drop=True)
    return df


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def get_low_point(segment):
    idx = segment["close"].idxmin()
    return {
        "idx": idx,
        "price": float(segment.loc[idx, "close"]),
        "rsi": float(segment.loc[idx, "rsi"])
    }


def analyze_divergence(df):
    if df.empty or len(df) < 48:
        return None

    df = df.copy()
    df["rsi"] = rsi(df["close"], 14)

    recent = df.tail(24).reset_index(drop=True)
    if recent["rsi"].isna().all():
        return None

    part1 = recent.iloc[:8]
    part2 = recent.iloc[8:16]
    part3 = recent.iloc[16:24]

    p1 = get_low_point(part1)
    p2 = get_low_point(part2)
    p3 = get_low_point(part3)

    chain_price_ok = (
        p2["price"] <= p1["price"] * 1.05 and
        p3["price"] <= p2["price"] * 1.05
    )
    chain_rsi_ok = (
        p2["rsi"] > p1["rsi"] and
        p3["rsi"] > p2["rsi"]
    )

    two_price_ok = p3["price"] <= p1["price"] * 1.05
    two_rsi_ok = p3["rsi"] > p1["rsi"]

    oversold_ok = min(p1["rsi"], p2["rsi"], p3["rsi"]) < 40

    if chain_price_ok and chain_rsi_ok:
        return {
            "type": "3-point divergence chain",
            "points": 3,
            "oversold_ok": oversold_ok,
            "rsi_values": [round(p1["rsi"], 2), round(p2["rsi"], 2), round(p3["rsi"], 2)],
            "price_values": [round(p1["price"], 4), round(p2["price"], 4), round(p3["price"], 4)]
        }

    if two_price_ok and two_rsi_ok:
        return {
            "type": "2-point bullish divergence",
            "points": 2,
            "oversold_ok": oversold_ok,
            "rsi_values": [round(p1["rsi"], 2), round(p3["rsi"], 2)],
            "price_values": [round(p1["price"], 4), round(p3["price"], 4)]
        }

    return None


def compute_fib_proxy(df):
    if df.empty or len(df) < 60:
        return {
            "fib_ratio": None,
            "fib_zone_hit": False,
            "fib_invalid": False
        }

    recent60 = df.tail(60).reset_index(drop=True)
    pre = recent60.iloc[:36]
    tail = recent60.iloc[36:]

    if pre.empty or tail.empty:
        return {
            "fib_ratio": None,
            "fib_zone_hit": False,
            "fib_invalid": False
        }

    swing_high = float(pre["close"].max())
    swing_low = float(tail["close"].min())
    last_price = float(recent60.iloc[-1]["close"])

    denom = swing_high - swing_low
    if denom <= 0:
        return {
            "fib_ratio": None,
            "fib_zone_hit": False,
            "fib_invalid": False
        }

    rebound_ratio = (last_price - swing_low) / denom

    return {
        "fib_ratio": round(rebound_ratio, 4),
        "fib_zone_hit": 0.618 <= rebound_ratio <= 0.786,
        "fib_invalid": rebound_ratio > 1.0
    }


def compute_filters(df, ticker):
    last_price = float(df.iloc[-1]["close"])

    if len(df) >= 21:
        rise20_pct = ((last_price / float(df.iloc[-21]["close"])) - 1) * 100
    else:
        rise20_pct = 0

    recent60 = df.tail(60)
    high60 = float(recent60["close"].max())
    resistance_gap_pct = ((high60 - last_price) / last_price) * 100 if last_price > 0 else 0

    recent5_vol = df.tail(5)["volume"].mean() if len(df) >= 5 else 0
    prev20_vol = df.iloc[-25:-5]["volume"].mean() if len(df) >= 25 else 0
    vol_ratio = (recent5_vol / prev20_vol) if prev20_vol and prev20_vol > 0 else 0

    trade_value_24h = float(ticker.get("acc_trade_price_24h", 0)) if ticker else 0

    return {
        "rise20_pct": round(rise20_pct, 2),
        "resistance_gap_pct": round(resistance_gap_pct, 2),
        "vol_ratio": round(vol_ratio, 2),
        "trade_value_24h": round(trade_value_24h, 2),
        "not_overextended": rise20_pct <= 15,
        "not_near_resistance": resistance_gap_pct > 5,
        "volume_ok": vol_ratio >= 1.3,
        "not_microcap": True
    }


def build_candidate(market, df, ticker):
    div = analyze_divergence(df)
    if not div:
        return None

    fib = compute_fib_proxy(df)
    if fib["fib_invalid"]:
        return None

    filters = compute_filters(df, ticker)
    last_price = float(df.iloc[-1]["close"])

    reasons = []
    if div["points"] == 3:
        reasons.append("3꼭지 다이버전스 연계")
    else:
        reasons.append("2점 상승 다이버전스")

    if div["oversold_ok"]:
        reasons.append("RSI 과매도 구간 컨펌")

    if fib["fib_zone_hit"]:
        reasons.append("피보 0.618~0.786 구간")

    if filters["volume_ok"]:
        reasons.append(f"거래량 증가 {filters['vol_ratio']}배")

    score = 50
    if div["points"] == 3:
        score += 20
    if div["oversold_ok"]:
        score += 10
    if fib["fib_zone_hit"]:
        score += 8
    if filters["volume_ok"]:
        score += 7
    if filters["not_overextended"]:
        score += 5
    if filters["not_near_resistance"]:
        score += 5

    return {
        "market": market,
        "symbol": market.replace("KRW-", ""),
        "score": score,
        "signal_type": div["type"],
        "divergence_points": div["points"],
        "last_price": round(last_price, 4),
        "rsi_values": div["rsi_values"],
        "price_values": div["price_values"],
        "fib_ratio": fib["fib_ratio"],
        "fib_zone_hit": fib["fib_zone_hit"],
        "rise20_pct": filters["rise20_pct"],
        "resistance_gap_pct": filters["resistance_gap_pct"],
        "vol_ratio": filters["vol_ratio"],
        "trade_value_24h": filters["trade_value_24h"],
        "filters": {
            "not_overextended": filters["not_overextended"],
            "not_near_resistance": filters["not_near_resistance"],
            "volume_ok": filters["volume_ok"],
            "not_microcap": filters["not_microcap"]
        },
        "reasons": reasons,
        "risk_note": "피보 1 이탈 또는 구조 붕괴 시 무효"
    }


def scan_all():
    all_markets = get_upbit_markets()
    ticker_map = get_ticker_map(all_markets)

    markets = get_top_markets_by_trade_value(all_markets, ticker_map, TOP_N_BY_TRADE_VALUE)

    sub_results = []
    main_results = []

    for market in markets:
        try:
            df = get_candles(market, count=60)
            ticker = ticker_map.get(market, {})

            candidate = build_candidate(market, df, ticker)
            if not candidate:
                continue

            sub_results.append(candidate)

            f = candidate["filters"]
            if (
                f["not_overextended"]
                and f["not_near_resistance"]
                and f["volume_ok"]
            ):
                main_results.append(candidate)

        except Exception as e:
            print(f"ERROR {market}: {e}")

    sub_results = sorted(sub_results, key=lambda x: x["score"], reverse=True)
    main_results = sorted(main_results, key=lambda x: x["score"], reverse=True)

    return {
        "scan_time_kst": get_now_kst(),
        "universe_count": len(markets),
        "main": main_results[:10],
        "sub": sub_results[:20]
    }
