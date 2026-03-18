import requests
import pandas as pd


def get_upbit_markets():
    url = "https://api.upbit.com/v1/market/all"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return [m["market"] for m in data if m["market"].startswith("KRW-")]


def get_candles(market, count=200):
    url = "https://api.upbit.com/v1/candles/minutes/60"
    params = {"market": market, "count": count}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df = df[["candle_date_time_kst", "trade_price"]].copy()
    df.columns = ["time", "close"]
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


def has_bullish_divergence(df):
    if df.empty or len(df) < 40:
        return False

    df = df.copy()
    df["rsi"] = rsi(df["close"], 14)

    recent = df.tail(20).reset_index(drop=True)

    # 최근 20봉에서 앞쪽/뒤쪽 저점 하나씩 비교
    first_half = recent.iloc[:10]
    second_half = recent.iloc[10:]

    if first_half.empty or second_half.empty:
        return False

    low1_idx = first_half["close"].idxmin()
    low2_idx = second_half["close"].idxmin()

    low1_price = recent.loc[low1_idx, "close"]
    low2_price = recent.loc[low2_idx, "close"]
    low1_rsi = recent.loc[low1_idx, "rsi"]
    low2_rsi = recent.loc[low2_idx, "rsi"]

    if pd.isna(low1_rsi) or pd.isna(low2_rsi):
        return False

    price_condition = low2_price <= low1_price * 1.02
    rsi_condition = low2_rsi > low1_rsi
    rsi_zone_condition = (low1_rsi < 45) or (low2_rsi < 45)

    return price_condition and rsi_condition and rsi_zone_condition


def scan():
    results = []
    markets = get_upbit_markets()

    for market in markets:
        try:
            df = get_candles(market)
            if has_bullish_divergence(df):
                results.append(market)
        except Exception as e:
            print(f"ERROR {market}: {e}")

    return results
