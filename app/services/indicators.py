from __future__ import annotations

import numpy as np
import pandas as pd


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder RSI (EWM 방식, TradingView 기본값과 동일)"""
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50.0)


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def enrich(df: pd.DataFrame, rsi_period: int, ema_fast: int = 20, ema_slow: int = 50, ema_regime: int = 200) -> pd.DataFrame:
    """
    캔들 DataFrame 에 보조 지표 컬럼 추가.
    - rsi       : RSI(14)
    - ema20/50/200
    - vol_ma_5 / vol_ma_20  : 거래량 이동평균 (볼륨 비율 계산용)
    - pct_from_20_low       : 최근 20봉 저가 대비 현재가 상승률 (과열 감지)
    """
    out = df.copy()
    out['rsi'] = rsi(out['close'], rsi_period)
    out['ema20'] = ema(out['close'], ema_fast)
    out['ema50'] = ema(out['close'], ema_slow)
    out['ema200'] = ema(out['close'], ema_regime)
    out['vol_ma_5'] = out['volume'].rolling(5).mean()
    out['vol_ma_20'] = out['volume'].rolling(20).mean()
    low20 = out['low'].rolling(20).min()
    out['pct_from_20_low'] = (out['close'] / low20 - 1.0) * 100.0
    return out
