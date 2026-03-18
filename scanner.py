import requests
import pandas as pd
import numpy as np

def get_upbit_markets():
    url = "https://api.upbit.com/v1/market/all"
    return [m['market'] for m in requests.get(url).json() if m['market'].startswith('KRW-')]

def get_candles(market):
    url = f"https://api.upbit.com/v1/candles/minutes/60?market={market}&count=200"
    data = requests.get(url).json()
    df = pd.DataFrame(data)
    df = df[['candle_date_time_kst','trade_price']]
    df.columns = ['time','close']
    return df.iloc[::-1]

def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def find_divergence(df):
    df['rsi'] = rsi(df['close'])
    if len(df) < 50:
        return False
    price_low = df['close'].iloc[-1] < df['close'].iloc[-5]
    rsi_high = df['rsi'].iloc[-1] > df['rsi'].iloc[-5]
    return price_low and rsi_high

def scan():
    results = []
    markets = get_upbit_markets()
    for m in markets:
        try:
            df = get_candles(m)
            if find_divergence(df):
                results.append(m)
        except:
            continue
    return results
