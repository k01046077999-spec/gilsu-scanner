from __future__ import annotations

import pandas as pd



def find_swings(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    out = df.copy()
    highs = out["high"]
    lows = out["low"]

    out["swing_high"] = False
    out["swing_low"] = False

    for i in range(window, len(out) - window):
        if highs.iloc[i] == highs.iloc[i - window : i + window + 1].max():
            out.iloc[i, out.columns.get_loc("swing_high")] = True
        if lows.iloc[i] == lows.iloc[i - window : i + window + 1].min():
            out.iloc[i, out.columns.get_loc("swing_low")] = True
    return out


def latest_swing_lows(df: pd.DataFrame, count: int = 4) -> pd.DataFrame:
    lows = df[df["swing_low"]].copy()
    return lows.tail(count)


def latest_swing_highs(df: pd.DataFrame, count: int = 4) -> pd.DataFrame:
    highs = df[df["swing_high"]].copy()
    return highs.tail(count)
