from __future__ import annotations

import pandas as pd

EXTREME_OVERSOLD = 35
STRONG_OVERSOLD = 30
EXTREME_OVERBOUGHT = 65
STRONG_OVERBOUGHT = 70
MIN_PRICE_MOVE_PCT = 0.35
MIN_RSI_MOVE = 2.0
MIN_BAR_GAP = 2


def _bar_gap_ok(swings: pd.DataFrame) -> bool:
    if len(swings) < 2:
        return False
    idx = list(swings.index)
    return all((idx[i] - idx[i - 1]) >= MIN_BAR_GAP for i in range(1, len(idx)))


def _pct_change(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return abs((b - a) / a) * 100.0


def detect_bullish_divergence_chain(swings: pd.DataFrame) -> dict:
    if len(swings) < 2:
        return {"found": False, "general": False, "chain": False, "extreme": False, "strength": 0.0}

    last2 = swings.tail(2).copy()
    last3 = swings.tail(3).copy()

    p2 = last2["low"].tolist()
    r2 = last2["rsi"].tolist()
    price_move_2 = _pct_change(p2[0], p2[-1])
    rsi_move_2 = r2[-1] - r2[0]
    general = p2[-1] < p2[-2] and r2[-1] > r2[-2] and price_move_2 >= MIN_PRICE_MOVE_PCT and rsi_move_2 >= MIN_RSI_MOVE and _bar_gap_ok(last2)

    chain = False
    chain_strength = 0.0
    if len(last3) >= 3:
        p3 = last3["low"].tolist()
        r3 = last3["rsi"].tolist()
        price_move_3 = _pct_change(p3[0], p3[-1])
        rsi_move_3 = r3[-1] - r3[0]
        chain = (
            p3[-1] <= p3[-2] <= p3[-3]
            and r3[-1] >= r3[-2] >= r3[-3]
            and price_move_3 >= MIN_PRICE_MOVE_PCT
            and rsi_move_3 >= MIN_RSI_MOVE
            and _bar_gap_ok(last3)
        )
        if chain:
            chain_strength += min(price_move_3 * 1.2, 8.0)
            chain_strength += min(rsi_move_3 * 0.8, 10.0)

    extreme_rsi = float(last3["rsi"].min()) if len(last3) >= 3 else float(last2["rsi"].min())
    extreme = extreme_rsi <= EXTREME_OVERSOLD
    strong_extreme = extreme_rsi <= STRONG_OVERSOLD

    strength = 0.0
    if general:
        strength += 12.0 + min(price_move_2, 6.0) + min(rsi_move_2, 8.0)
    if chain:
        strength += 18.0 + chain_strength
    if extreme:
        strength += 5.0
    if strong_extreme:
        strength += 4.0

    price_points = last3["low"].tolist() if len(last3) >= 3 else last2["low"].tolist()
    rsi_points = last3["rsi"].tolist() if len(last3) >= 3 else last2["rsi"].tolist()
    return {
        "found": general or chain,
        "general": general,
        "chain": chain,
        "extreme": extreme,
        "strong_extreme": strong_extreme,
        "strength": round(strength, 2),
        "price_points": price_points,
        "rsi_points": rsi_points,
        "bar_indices": list(last3.index) if len(last3) >= 3 else list(last2.index),
    }



def detect_bearish_divergence_chain(swings: pd.DataFrame) -> dict:
    if len(swings) < 2:
        return {"found": False, "general": False, "chain": False, "extreme": False, "strength": 0.0}

    last2 = swings.tail(2).copy()
    last3 = swings.tail(3).copy()

    p2 = last2["high"].tolist()
    r2 = last2["rsi"].tolist()
    price_move_2 = _pct_change(p2[0], p2[-1])
    rsi_move_2 = r2[0] - r2[-1]
    general = p2[-1] > p2[-2] and r2[-1] < r2[-2] and price_move_2 >= MIN_PRICE_MOVE_PCT and rsi_move_2 >= MIN_RSI_MOVE and _bar_gap_ok(last2)

    chain = False
    chain_strength = 0.0
    if len(last3) >= 3:
        p3 = last3["high"].tolist()
        r3 = last3["rsi"].tolist()
        price_move_3 = _pct_change(p3[0], p3[-1])
        rsi_move_3 = r3[0] - r3[-1]
        chain = (
            p3[-1] >= p3[-2] >= p3[-3]
            and r3[-1] <= r3[-2] <= r3[-3]
            and price_move_3 >= MIN_PRICE_MOVE_PCT
            and rsi_move_3 >= MIN_RSI_MOVE
            and _bar_gap_ok(last3)
        )
        if chain:
            chain_strength += min(price_move_3 * 1.2, 8.0)
            chain_strength += min(rsi_move_3 * 0.8, 10.0)

    extreme_rsi = float(last3["rsi"].max()) if len(last3) >= 3 else float(last2["rsi"].max())
    extreme = extreme_rsi >= EXTREME_OVERBOUGHT
    strong_extreme = extreme_rsi >= STRONG_OVERBOUGHT

    strength = 0.0
    if general:
        strength += 12.0 + min(price_move_2, 6.0) + min(rsi_move_2, 8.0)
    if chain:
        strength += 18.0 + chain_strength
    if extreme:
        strength += 5.0
    if strong_extreme:
        strength += 4.0

    price_points = last3["high"].tolist() if len(last3) >= 3 else last2["high"].tolist()
    rsi_points = last3["rsi"].tolist() if len(last3) >= 3 else last2["rsi"].tolist()
    return {
        "found": general or chain,
        "general": general,
        "chain": chain,
        "extreme": extreme,
        "strong_extreme": strong_extreme,
        "strength": round(strength, 2),
        "price_points": price_points,
        "rsi_points": rsi_points,
        "bar_indices": list(last3.index) if len(last3) >= 3 else list(last2.index),
    }
