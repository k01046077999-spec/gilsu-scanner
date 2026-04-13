from __future__ import annotations

import pandas as pd


def _fallback_chunk(df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    return df.tail(lookback).copy()


def _bullish_anchor_points(df: pd.DataFrame, lookback: int):
    chunk = _fallback_chunk(df, lookback)
    if "swing_low" not in chunk.columns or "swing_high" not in chunk.columns:
        return float(chunk["low"].min()), float(chunk["high"].max()), "range_fallback"

    highs = chunk[chunk["swing_high"]].tail(3)
    for high_idx, high_row in highs.iloc[::-1].iterrows():
        lows_before = chunk.loc[:high_idx]
        lows_before = lows_before[lows_before["swing_low"]]
        if not lows_before.empty:
            low_row = lows_before.iloc[-1]
            if float(high_row["high"]) > float(low_row["low"]):
                return float(low_row["low"]), float(high_row["high"]), "swing_anchored"
    return float(chunk["low"].min()), float(chunk["high"].max()), "range_fallback"



def _bearish_anchor_points(df: pd.DataFrame, lookback: int):
    chunk = _fallback_chunk(df, lookback)
    if "swing_low" not in chunk.columns or "swing_high" not in chunk.columns:
        return float(chunk["low"].min()), float(chunk["high"].max()), "range_fallback"

    lows = chunk[chunk["swing_low"]].tail(3)
    for low_idx, low_row in lows.iloc[::-1].iterrows():
        highs_before = chunk.loc[:low_idx]
        highs_before = highs_before[highs_before["swing_high"]]
        if not highs_before.empty:
            high_row = highs_before.iloc[-1]
            if float(high_row["high"]) > float(low_row["low"]):
                return float(low_row["low"]), float(high_row["high"]), "swing_anchored"
    return float(chunk["low"].min()), float(chunk["high"].max()), "range_fallback"



def bullish_fib_zone(df: pd.DataFrame, lookback: int = 80) -> dict:
    swing_low, swing_high, anchor_source = _bullish_anchor_points(df, lookback)
    current = float(df["close"].iloc[-1])
    range_ = swing_high - swing_low
    if range_ <= 0:
        return {"valid": False}

    fib_382 = swing_high - range_ * 0.382
    fib_05 = swing_high - range_ * 0.5
    fib_618 = swing_high - range_ * 0.618
    fib_786 = swing_high - range_ * 0.786
    fib_1 = swing_low
    fib_1272 = swing_high + range_ * 0.272
    fib_1618 = swing_high + range_ * 0.618
    in_zone = min(fib_618, fib_786) <= current <= max(fib_618, fib_786)
    near_zone = min(fib_05, fib_786) <= current <= max(fib_05, fib_786) * 1.01 and current >= fib_1
    invalidated = current < fib_1

    return {
        "valid": True,
        "anchor_source": anchor_source,
        "anchor_low": swing_low,
        "anchor_high": swing_high,
        "fib_382": fib_382,
        "fib_0_5": fib_05,
        "fib_618": fib_618,
        "fib_786": fib_786,
        "fib_1": fib_1,
        "fib_1272": fib_1272,
        "fib_1618": fib_1618,
        "in_zone": in_zone,
        "near_zone": near_zone,
        "invalidated": invalidated,
        "entry_zone": sorted([fib_786, fib_618]),
    }



def bearish_fib_zone(df: pd.DataFrame, lookback: int = 80) -> dict:
    swing_low, swing_high, anchor_source = _bearish_anchor_points(df, lookback)
    current = float(df["close"].iloc[-1])
    range_ = swing_high - swing_low
    if range_ <= 0:
        return {"valid": False}

    fib_382 = swing_low + range_ * 0.382
    fib_05 = swing_low + range_ * 0.5
    fib_618 = swing_low + range_ * 0.618
    fib_786 = swing_low + range_ * 0.786
    fib_1 = swing_high
    fib_1272 = swing_low - range_ * 0.272
    fib_1618 = swing_low - range_ * 0.618
    in_zone = min(fib_618, fib_786) <= current <= max(fib_618, fib_786)
    near_zone = min(fib_05, fib_618) * 0.99 <= current <= fib_1
    invalidated = current > fib_1

    return {
        "valid": True,
        "anchor_source": anchor_source,
        "anchor_low": swing_low,
        "anchor_high": swing_high,
        "fib_382": fib_382,
        "fib_0_5": fib_05,
        "fib_618": fib_618,
        "fib_786": fib_786,
        "fib_1": fib_1,
        "fib_1272": fib_1272,
        "fib_1618": fib_1618,
        "in_zone": in_zone,
        "near_zone": near_zone,
        "invalidated": invalidated,
        "entry_zone": sorted([fib_618, fib_786]),
    }
