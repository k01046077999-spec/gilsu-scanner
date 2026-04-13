from __future__ import annotations

import numpy as np
import pandas as pd


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    gain = up.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    loss = down.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(method='bfill')


def enrich(df: pd.DataFrame, rsi_period: int, ema_fast: int, ema_slow: int, ema_regime: int) -> pd.DataFrame:
    out = df.copy()
    out['rsi'] = rsi(out['close'], rsi_period)
    out['ema20'] = out['close'].ewm(span=ema_fast, adjust=False).mean()
    out['ema50'] = out['close'].ewm(span=ema_slow, adjust=False).mean()
    out['ema200'] = out['close'].ewm(span=ema_regime, adjust=False).mean()
    out['vol_ma_5'] = out['quote_volume'].rolling(5).mean()
    out['vol_ma_20'] = out['quote_volume'].rolling(20).mean()
    rolling_low = out['low'].rolling(20).min()
    out['pct_from_20_low'] = (out['close'] / rolling_low - 1.0) * 100.0
    return out
