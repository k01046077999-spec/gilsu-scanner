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
    try:
        resp = requests.get(UPBIT_MARKET_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return [x["market"] for x in data if x["market"].startswith("KRW-")]
    except:
        return []


def get_ticker_map(markets):
    result = {}
    for group in chunked(markets, 50):
        try:
            resp = requests.get(
                UPBIT_TICKER_URL,
                params={"markets": ",".join(group)},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            for item in data:
                result[item["market"]] = item
        except:
            continue
    return result


def get_top_markets_by_trade_value(markets, ticker_map):
    ranked = []
    for m in markets:
        t = ticker_map.get(m, {})
        val = float(t.get("acc_trade_price_24h", 0))
        ranked.append((m, val))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in ranked[:TOP_N_BY_TRADE_VALUE]]


def get_candles(market, count=60):
    try:
        resp = requests.get(
            UPBIT_CANDLE_URL,
            params={"market": market, "count": count},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            return None

        df = pd.DataFrame(data)
        df = df[[
            "candle_date_time_kst",
            "trade_price",
            "candle_acc_trade_volume"
        ]].copy()

        df.columns = ["time", "close", "volume"]
        df = df.iloc[::-1].reset_index(drop=True)
        return df

    except:
        return None


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def analyze_divergence(df):
    try:
        if df is None or len(df) < 48:
            return None

        df = df.copy()
        df["rsi"] = rsi(df["close"], 14)

        df = df.dropna()

        if len(df) < 24:
            return None

        recent = df.tail(24).reset_index(drop=True)

        p1 = recent.iloc[:8]
        p2 = recent.iloc[8:16]
        p3 = recent.iloc[16:24]

        p1_low = p1.loc[p1["close"].idxmin()]
        p2_low = p2.loc[p2["close"].idxmin()]
        p3_low = p3.loc[p3["close"].idxmin()]

        chain = (
            p2_low["close"] <= p1_low["close"] * 1.05 and
            p3_low["close"] <= p2_low["close"] * 1.05 and
            p2_low["rsi"] > p1_low["rsi"] and
            p3_low["rsi"] > p2_low["rsi"]
        )

        if chain:
            return {
                "type": "3-point",
                "points": 3
            }

        two = (
            p3_low["close"] <= p1_low["close"] * 1.05 and
            p3_low["rsi"] > p1_low["rsi"]
        )

        if two:
            return {
                "type": "2-point",
                "points": 2
            }

        return None

    except:
        return None


def compute_filters(df):
    try:
        last = float(df.iloc[-1]["close"])
        base = float(df.iloc[-21]["close"]) if len(df) > 21 else last

        rise = ((last / base) - 1) * 100 if base > 0 else 0

        high60 = df.tail(60)["close"].max()
        gap = ((high60 - last) / last) * 100 if last > 0 else 0

        vol5 = df.tail(5)["volume"].mean()
        vol20 = df.iloc[-25:-5]["volume"].mean() if len(df) > 25 else 0
        vol_ratio = (vol5 / vol20) if vol20 else 0

        return {
            "rise": rise,
            "gap": gap,
            "vol": vol_ratio,
            "ok": rise <= 15 and gap > 5 and vol_ratio >= 1.3
        }

    except:
        return {"ok": False}


def scan_all():
    markets = get_upbit_markets()
    if not markets:
        return {"main": [], "sub": []}

    ticker_map = get_ticker_map(markets)
    markets = get_top_markets_by_trade_value(markets, ticker_map)

    sub = []
    main = []

    for m in markets:
        df = get_candles(m)
        if df is None:
            continue

        div = analyze_divergence(df)
        if not div:
            continue

        f = compute_filters(df)

        item = {
            "market": m,
            "type": div["type"],
            "points": div["points"]
        }

        sub.append(item)

        if f["ok"]:
            main.append(item)

    return {
        "scan_time_kst": get_now_kst(),
        "main": main[:10],
        "sub": sub[:20]
    }
