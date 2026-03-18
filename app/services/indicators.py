from __future__ import annotations

import numpy as np
import pandas as pd


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(method="bfill")


def enrich_indicators(df: pd.DataFrame, rsi_period: int = 14) -> pd.DataFrame:
    out = df.copy()
    out["rsi"] = compute_rsi(out["close"], rsi_period)
    out["vol_ma_5"] = out["volume"].rolling(5).mean()
    out["vol_ma_20"] = out["volume"].rolling(20).mean()
    out["pct_from_20_low"] = (out["close"] / out["low"].rolling(20).min() - 1.0) * 100
    out["ret_12"] = out["close"].pct_change(12) * 100
    return out
