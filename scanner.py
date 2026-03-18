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
    rsi_series = 100 - (100 / (1 + rs))
    return rsi_series


def has_bullish_divergence(df):
    if df.empty or len(df) < 30:
        return False

    df = df.copy()
    df["rsi"] = rsi(df["close"], 14)

    if df["rsi"].isna().all():
        return False

    recent = df.tail(20).reset_index(drop=True)

    # 최근 20개 봉 안에서 가격 저점 2개 비교
    low1_idx = recent["close"].idxmin()
    low1_price = recent.loc[low1_idx, "close"]
    low1_rsi = recent.loc[low1_idx, "rsi"]

    # 첫 저점 이후 구간에서 두 번째 저점 찾기
    later = recent.loc[low1_idx + 3 :]
    if later.empty or len(later) < 3:
        return False

    low2_idx = later["close"].idxmin()
    low2_price = later.loc[low2_idx, "close"]
    low2_rsi = later.loc[low2_idx, "rsi"]

    if pd.isna(low1_rsi) or pd.isna(low2_rsi):
        return False

    # 상승 다이버전스:
    # 가격은 더 낮거나 비슷한 저점, RSI는 더 높은 저점
    price_condition = low2_price <= low1_price * 1.01
    rsi_condition = low2_rsi > low1_rsi

    # RSI가 너무 중간값이면 제외
    rsi_zone_condition = low1_rsi < 40 or low2_rsi < 40

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
