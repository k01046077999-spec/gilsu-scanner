from __future__ import annotations

import pandas as pd


EXTREME_OVERSOLD = 35
EXTREME_OVERBOUGHT = 65


def detect_bullish_divergence_chain(swings: pd.DataFrame) -> dict:
    if len(swings) < 2:
        return {"found": False}

    last = swings.tail(3).copy()
    price_lows = last["low"].tolist()
    rsi_lows = last["rsi"].tolist()

    general = price_lows[-1] < price_lows[-2] and rsi_lows[-1] > rsi_lows[-2]
    chain = False
    if len(last) >= 3:
        chain = (
            price_lows[-1] <= price_lows[-2] <= price_lows[-3]
            and rsi_lows[-1] >= rsi_lows[-2] >= rsi_lows[-3]
        )

    extreme = min(rsi_lows[-3:]) <= EXTREME_OVERSOLD
    return {
        "found": general or chain,
        "general": general,
        "chain": chain,
        "extreme": extreme,
        "price_points": price_lows,
        "rsi_points": rsi_lows,
    }



def detect_bearish_divergence_chain(swings: pd.DataFrame) -> dict:
    if len(swings) < 2:
        return {"found": False}

    last = swings.tail(3).copy()
    price_highs = last["high"].tolist()
    rsi_highs = last["rsi"].tolist()

    general = price_highs[-1] > price_highs[-2] and rsi_highs[-1] < rsi_highs[-2]
    chain = False
    if len(last) >= 3:
        chain = (
            price_highs[-1] >= price_highs[-2] >= price_highs[-3]
            and rsi_highs[-1] <= rsi_highs[-2] <= rsi_highs[-3]
        )

    extreme = max(rsi_highs[-3:]) >= EXTREME_OVERBOUGHT
    return {
        "found": general or chain,
        "general": general,
        "chain": chain,
        "extreme": extreme,
        "price_points": price_highs,
        "rsi_points": rsi_highs,
    }
