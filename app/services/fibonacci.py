from __future__ import annotations

import pandas as pd


EXT_1272 = 1.272
EXT_1618 = 1.618


def bullish_fib_zone(df: pd.DataFrame, lookback: int = 80) -> dict:
    chunk = df.tail(lookback)
    swing_low = float(chunk["low"].min())
    swing_high = float(chunk["high"].max())
    current = float(df["close"].iloc[-1])

    range_ = swing_high - swing_low
    if range_ <= 0:
        return {"valid": False}

    fib_382 = swing_high - range_ * 0.382
    fib_5 = swing_high - range_ * 0.5
    fib_618 = swing_high - range_ * 0.618
    fib_786 = swing_high - range_ * 0.786
    fib_1 = swing_low
    ext_1272 = swing_high + range_ * (EXT_1272 - 1.0)
    ext_1618 = swing_high + range_ * (EXT_1618 - 1.0)

    in_zone = min(fib_618, fib_786) <= current <= max(fib_618, fib_786)
    near_zone = current <= max(fib_618, fib_786) * 1.02 and current >= fib_1
    invalidated = current < fib_1

    return {
        "valid": True,
        "swing_low": swing_low,
        "swing_high": swing_high,
        "fib_382": fib_382,
        "fib_5": fib_5,
        "fib_618": fib_618,
        "fib_786": fib_786,
        "fib_1": fib_1,
        "ext_1272": ext_1272,
        "ext_1618": ext_1618,
        "in_zone": in_zone,
        "near_zone": near_zone,
        "invalidated": invalidated,
        "entry_zone": sorted([fib_786, fib_618]),
    }



def bearish_fib_zone(df: pd.DataFrame, lookback: int = 80) -> dict:
    chunk = df.tail(lookback)
    swing_low = float(chunk["low"].min())
    swing_high = float(chunk["high"].max())
    current = float(df["close"].iloc[-1])

    range_ = swing_high - swing_low
    if range_ <= 0:
        return {"valid": False}

    fib_382 = swing_low + range_ * 0.382
    fib_5 = swing_low + range_ * 0.5
    fib_618 = swing_low + range_ * 0.618
    fib_786 = swing_low + range_ * 0.786
    fib_1 = swing_high
    ext_1272 = swing_low - range_ * (EXT_1272 - 1.0)
    ext_1618 = swing_low - range_ * (EXT_1618 - 1.0)

    in_zone = min(fib_618, fib_786) <= current <= max(fib_618, fib_786)
    near_zone = current >= min(fib_618, fib_786) * 0.98 and current <= fib_1
    invalidated = current > fib_1

    return {
        "valid": True,
        "swing_low": swing_low,
        "swing_high": swing_high,
        "fib_382": fib_382,
        "fib_5": fib_5,
        "fib_618": fib_618,
        "fib_786": fib_786,
        "fib_1": fib_1,
        "ext_1272": ext_1272,
        "ext_1618": ext_1618,
        "in_zone": in_zone,
        "near_zone": near_zone,
        "invalidated": invalidated,
        "entry_zone": sorted([fib_618, fib_786]),
    }
