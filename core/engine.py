import time
import numpy as np
import pandas as pd
from core import config
from core.scoring import classify_signal
from services.binance import BinanceService


class PresidentTradingEngine:
    def __init__(self) -> None:
        self.binance = BinanceService()

    def _rsi(self, series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)

    def _find_two_recent_lows(self, series: pd.Series):
        vals = series.values
        if len(vals) < 12:
            return None
        pivot_idx = []
        for i in range(2, len(vals) - 2):
            window = vals[i-2:i+3]
            if vals[i] == np.min(window):
                pivot_idx.append(i)
        if len(pivot_idx) < 2:
            return None
        return pivot_idx[-2], pivot_idx[-1]

    def _stage1_check(self, df_1h: pd.DataFrame):
        close = df_1h["close"].astype(float)
        vol = df_1h["volume"].astype(float)
        rsi = self._rsi(close)

        lows = self._find_two_recent_lows(close)
        if not lows:
            return {"passed": False, "score": 0.0, "reason": "no_pivots"}

        i1, i2 = lows
        price_div = close.iloc[i2] < close.iloc[i1]
        rsi_div = rsi.iloc[i2] > rsi.iloc[i1]
        vol_improve = vol.tail(8).mean() >= vol.iloc[max(0, len(vol)-24):len(vol)-8].mean() * 0.9
        rsi_zone = rsi.iloc[i2] <= 42

        score = 0.0
        if price_div and rsi_div:
            score += 1.4
        if rsi_zone:
            score += 0.5
        if vol_improve:
            score += 0.4

        return {
            "passed": score >= 1.1,
            "score": score,
            "price_div": price_div,
            "rsi_div": rsi_div,
            "vol_improve": vol_improve,
            "rsi_zone": rsi_zone,
            "rsi_last": float(rsi.iloc[-1]),
        }

    def _fib_levels(self, high: float, low: float):
        diff = high - low
        return {
            "fib_0618": high - diff * 0.618,
            "fib_0786": high - diff * 0.786,
            "fib_1": low,
        }

    def _stage2_analyze(self, symbol: str, mode: str):
        df_1h = self.binance.fetch_ohlcv_df(symbol, "1h", 240)
        stage1 = self._stage1_check(df_1h)
        if not stage1["passed"]:
            return {
                "passed": False,
                "symbol": symbol,
                "state": "watch",
                "message": "1단계 조건 미충족",
                "warnings": [],
                "rejected_by": ["stage1_fail"],
                "errors": [],
            }

        df_30m = self.binance.fetch_ohlcv_df(symbol, "30m", 240)
        df_4h = self.binance.fetch_ohlcv_df(symbol, "4h", 180)

        close_1h = df_1h["close"].astype(float)
        current = float(close_1h.iloc[-1])
        recent_high = float(close_1h.tail(72).max())
        recent_low = float(close_1h.tail(72).min())
        fib = self._fib_levels(recent_high, recent_low)

        fib_ok = fib["fib_0786"] <= current <= fib["fib_0618"]
        stop_price = fib["fib_1"]
        tp1 = recent_high
        tp2 = recent_high + (recent_high - recent_low) * 0.382

        stop_pct = round(((stop_price - current) / current) * 100, 2)
        tp1_pct = round(((tp1 - current) / current) * 100, 2)
        tp2_pct = round(((tp2 - current) / current) * 100, 2)

        risk = abs(stop_pct) if stop_pct != 0 else 999.0
        rr = round((tp1_pct / risk), 2) if risk > 0 else 0.0

        close_30m = df_30m["close"].astype(float)
        close_4h = df_4h["close"].astype(float)
        rsi_30m = self._rsi(close_30m)
        rsi_4h = self._rsi(close_4h)

        div_strength = 0.6
        lows_30m = self._find_two_recent_lows(close_30m)
        if lows_30m:
            j1, j2 = lows_30m
            if close_30m.iloc[j2] < close_30m.iloc[j1] and rsi_30m.iloc[j2] > rsi_30m.iloc[j1]:
                div_strength += 0.25
        if rsi_4h.iloc[-1] <= 48:
            div_strength += 0.15

        passed, state, warnings, rejected_by = classify_signal(
            mode=mode,
            stage1_score=float(stage1["score"]),
            fib_ok=fib_ok,
            rr=rr,
            divergence_strength=div_strength,
        )

        reason = []
        if stage1["price_div"] and stage1["rsi_div"]:
            reason.append("1시간봉 RSI 상승 다이버전스")
        if fib_ok:
            reason.append("Fib 0.618~0.786 구간")
        if div_strength >= 1.0:
            reason.append("30분/4시간 보조 확인")

        message = "진입 가능 구조" if passed and mode == "main" else ("관찰 후보" if passed else "조건 미충족")

        return {
            "passed": passed,
            "symbol": symbol.replace("/", ""),
            "state": state,
            "direction": "long",
            "stop_pct": stop_pct,
            "tp1_pct": tp1_pct,
            "tp2_pct": tp2_pct,
            "rr": rr,
            "message": message,
            "warnings": warnings,
            "rejected_by": rejected_by,
            "errors": [],
            "reason_summary": ", ".join(reason) if reason else "구조 불충분",
        }

    def analyze_symbol(self, symbol: str, mode: str = "main"):
        try:
            sym = symbol if "/" in symbol else symbol.replace("USDT", "/USDT")
            return self._stage2_analyze(sym, mode)
        except Exception as e:
            return {
                "passed": False,
                "symbol": symbol,
                "state": "watch",
                "message": "개별 심볼 분석 실패",
                "warnings": [],
                "rejected_by": [],
                "errors": [f"{type(e).__name__}: {e}"],
            }

    def scan(self, mode: str = "main", limit: int = 10):
        started = time.time()
        errors = []
        items = []

        universe_top_n = config.UNIVERSE_TOP_N_MAIN if mode == "main" else config.UNIVERSE_TOP_N_SUB
        stage1_max = config.STAGE1_MAX_MAIN if mode == "main" else config.STAGE1_MAX_SUB
        stage2_max = config.STAGE2_MAX_MAIN if mode == "main" else config.STAGE2_MAX_SUB

        universe = self.binance.get_dynamic_universe(top_n=universe_top_n)
        stage1_candidates = []

        for symbol in universe:
            if len(stage1_candidates) >= stage1_max:
                break
            try:
                df_1h = self.binance.fetch_ohlcv_df(symbol, "1h", 200)
                stage1 = self._stage1_check(df_1h)
                if stage1["passed"]:
                    stage1_candidates.append(symbol)
            except Exception as e:
                errors.append(f"{symbol}: {type(e).__name__}: {e}")

        for symbol in stage1_candidates[:stage2_max]:
            result = self._stage2_analyze(symbol, mode)
            if result["passed"]:
                items.append(result)
            elif mode == "sub" and result["state"] == "watch" and not result["errors"]:
                items.append(result)

        items.sort(key=lambda x: (x.get("state") != "ready", -(x.get("rr") or 0)))
        items = items[:limit]

        status = "partial" if errors else "ok"
        message = f"현재 {mode} 조건을 만족하는 종목이 없습니다." if len(items) == 0 else f"현재 {mode} 조건 후보 {len(items)}개"

        return {
            "status": status,
            "mode": mode,
            "count": len(items),
            "candidate_pool": len(universe),
            "stage1_checked": min(len(universe), stage1_max),
            "stage2_checked": min(len(stage1_candidates), stage2_max),
            "scan_seconds": round(time.time() - started, 2),
            "stopped_reason": None,
            "items": items,
            "message": message,
            "errors": errors[:20],
        }
