from __future__ import annotations

import asyncio
from time import perf_counter

from app.core.config import settings
from app.core.schemas import DirectAction, HealthResponse, ReadyResponse, RiskPlan, ScanResponse, ScanSignal
from app.services.divergence import detect_bullish
from app.services.fibonacci import recent_bullish_swing, zone_status
from app.services.indicators import enrich
from app.services.pivots import mark_pivots, recent_pivot_highs, recent_pivot_lows
from app.services.scoring import grade_from_score
from app.services.upbit_client import UpbitClient


class ScannerEngine:
    def __init__(self) -> None:
        self.client = UpbitClient()

    async def _frames(self, market: str):
        df15, df1h, df4h = await asyncio.gather(
            self.client.candles(market, 15, settings.candles_limit_15m),
            self.client.candles(market, 60, settings.candles_limit_1h),
            self.client.candles(market, 240, settings.candles_limit_4h),
        )
        df15 = mark_pivots(enrich(df15, settings.rsi_period, settings.ema_fast, settings.ema_slow, settings.ema_regime), settings.pivot_left, settings.pivot_right)
        df1h = mark_pivots(enrich(df1h, settings.rsi_period, settings.ema_fast, settings.ema_slow, settings.ema_regime), settings.pivot_left, settings.pivot_right)
        df4h = mark_pivots(enrich(df4h, settings.rsi_period, settings.ema_fast, settings.ema_slow, settings.ema_regime), settings.pivot_left, settings.pivot_right)
        return df15, df1h, df4h

    def _volume_ratio(self, df) -> float:
        row = df.iloc[-1]
        base = float(row['vol_ma_20']) if row['vol_ma_20'] == row['vol_ma_20'] else 0.0
        if base <= 0:
            return 0.0
        return round(float(row['vol_ma_5']) / base, 2)

    def _market_regime(self, btc1h, btc4h) -> tuple[str, bool]:
        c1 = float(btc1h['close'].iloc[-1])
        e1 = float(btc1h['ema200'].iloc[-1])
        c4 = float(btc4h['close'].iloc[-1])
        e4 = float(btc4h['ema200'].iloc[-1])
        r1 = float(btc1h['rsi'].iloc[-1])
        if c1 >= e1 and c4 >= e4 and r1 >= 45:
            return 'risk_on', True
        if c1 >= e1 and r1 >= 40:
            return 'neutral_ok', True
        return 'risk_off', False

    def _resistance_room(self, df1h, anchor_high: float) -> float:
        current = float(df1h['close'].iloc[-1])
        recent_high = float(df1h['high'].tail(80).max())
        ref = max(anchor_high, recent_high)
        return round((ref / current - 1.0) * 100.0, 2)

    def _overheated(self, df1h) -> bool:
        val = float(df1h['pct_from_20_low'].iloc[-1])
        return val >= settings.overheated_pct_from_20_low

    def _extension_profile(self, chain_points: int, volume_ratio: float, breakout_confirmed: bool, market_regime: str, rsi_1h: float) -> tuple[str, str]:
        strength = 0
        if chain_points >= 3:
            strength += 1
        if volume_ratio >= 1.15:
            strength += 1
        if breakout_confirmed:
            strength += 1
        if market_regime == 'risk_on':
            strength += 1
        if 40 <= rsi_1h <= 60:
            strength += 1
        if strength >= 4:
            return 'strong', 'fib_1.618_extension'
        return 'standard', 'fib_1.272_extension'

    def _risk_plan(self, current: float, swing: dict, extension_basis: str, resistance_room_pct: float) -> RiskPlan:
        stop_price = float(swing['fib_1']) * (1.0 - settings.stop_buffer_pct / 100.0)
        tp1_price = float(swing['fib_0'])
        raw_tp2 = float(swing['ext_1618'] if extension_basis == 'fib_1.618_extension' else swing['ext_1272'])
        resistance_cap = current * (1.0 + max(resistance_room_pct, 0.0) / 100.0)
        tp2_price = min(raw_tp2, resistance_cap) if resistance_room_pct > 0 else raw_tp2

        stop_pct = (stop_price / current - 1.0) * 100.0
        tp1_pct = (tp1_price / current - 1.0) * 100.0
        tp2_pct = (tp2_price / current - 1.0) * 100.0
        stop_abs = abs(stop_pct) if stop_pct != 0 else 999.0

        return RiskPlan(
            entry_price=round(current, 8),
            stop_price=round(stop_price, 8),
            stop_loss_pct=round(stop_pct, 2),
            tp1_price=round(tp1_price, 8),
            tp1_pct=round(tp1_pct, 2),
            tp2_price=round(tp2_price, 8),
            tp2_pct=round(tp2_pct, 2),
            rr_tp1=round(tp1_pct / stop_abs, 2),
            rr_tp2=round(tp2_pct / stop_abs, 2),
            fib_anchor_low=round(float(swing['anchor_low']), 8),
            fib_anchor_high=round(float(swing['anchor_high']), 8),
            fib_0618=round(float(swing['fib_0618']), 8),
            fib_0786=round(float(swing['fib_0786']), 8),
            extension_basis=extension_basis,
        )

    def _passes_filters(self, current: float, swing: dict, zone: str, volume_ratio: float, overheated: bool, resistance_room_pct: float, risk: RiskPlan, market_ok: bool, breakout_confirmed: bool) -> tuple[bool, list[str]]:
        rejected: list[str] = []
        upper = max(float(swing['fib_0618']), float(swing['fib_0786']))
        if not market_ok:
            rejected.append('btc_market_regime_risk_off')
        if zone == 'out_zone':
            rejected.append('fib_zone_miss')
        if current > upper * (1.0 + settings.late_entry_buffer_pct / 100.0):
            rejected.append('late_entry_after_bounce')
        if volume_ratio < settings.min_volume_ratio:
            rejected.append('weak_volume')
        if overheated:
            rejected.append('overheated_after_recent_rally')
        if not breakout_confirmed:
            rejected.append('breakout_not_confirmed')
        if resistance_room_pct < settings.min_resistance_room_pct:
            rejected.append('too_close_to_resistance')
        if risk.stop_loss_pct >= 0:
            rejected.append('invalid_stop_structure')
        if abs(risk.stop_loss_pct) > settings.max_stop_pct:
            rejected.append('stop_too_wide')
        if abs(risk.stop_loss_pct) > settings.absolute_max_stop_pct:
            rejected.append('stop_above_absolute_limit')
        if risk.rr_tp1 < settings.min_rr_tp1:
            rejected.append('tp1_rr_too_low')
        if risk.rr_tp2 < settings.min_rr_tp2:
            rejected.append('tp2_rr_too_low')
        return len(rejected) == 0, rejected

    def _direct_action(self, passed: bool, rejected: list[str], risk: RiskPlan) -> DirectAction:
        if passed:
            return DirectAction(
                status='buyable',
                action='지금 진입 가능',
                message=(
                    f'진입가 {risk.entry_price}, 손절가 {risk.stop_price} ({risk.stop_loss_pct}%), '
                    f'1차 익절 {risk.tp1_price} ({risk.tp1_pct}%), 최종 익절 {risk.tp2_price} ({risk.tp2_pct}%).'
                ),
            )
        if 'fib_zone_miss' in rejected or 'late_entry_after_bounce' in rejected:
            return DirectAction(status='wait', action='대기', message='좋은 눌림 구간이 아니므로 추격매수 금지.')
        return DirectAction(status='reject', action='매수 금지', message='구조 또는 기대값이 부족해 이번 시나리오는 제외.')

    async def analyze_symbol(self, symbol: str, btc1h=None, btc4h=None) -> ScanSignal | None:
        try:
            df15, df1h, df4h = await self._frames(symbol)
            if min(len(df15), len(df1h), len(df4h)) < 120:
                return None
            if btc1h is None or btc4h is None:
                btc15, btc1h, btc4h = await self._frames('KRW-BTC')
                _ = btc15

            current = float(df1h['close'].iloc[-1])
            market_regime, market_ok = self._market_regime(btc1h, btc4h)
            volume_ratio = self._volume_ratio(df1h)
            overheated = self._overheated(df1h)
            rsi_1h = float(df1h['rsi'].iloc[-1])

            bull15 = detect_bullish(recent_pivot_lows(df15), settings.pivot_min_gap, settings.pivot_max_gap, settings.min_chain_span)
            bull1h = detect_bullish(recent_pivot_lows(df1h), settings.pivot_min_gap, settings.pivot_max_gap, settings.min_chain_span)
            if not (bull15['found'] or bull1h['found']):
                return None

            div = bull15 if bull15['found'] else bull1h
            div_tf = '15m' if bull15['found'] else '1h'
            swing = recent_bullish_swing(df1h, recent_pivot_highs(df1h), recent_pivot_lows(df1h))
            if swing is None:
                return None

            zone = zone_status(current, float(swing['fib_0618']), float(swing['fib_0786']), settings.fib_zone_tolerance_pct)
            resistance_room_pct = self._resistance_room(df1h, float(swing['anchor_high']))
            strength_profile, extension_basis = self._extension_profile(div.get('points', 0), volume_ratio, bool(swing['breakout_confirmed']), market_regime, rsi_1h)
            risk = self._risk_plan(current, swing, extension_basis, resistance_room_pct)
            passed, rejected = self._passes_filters(current, swing, zone, volume_ratio, overheated, resistance_room_pct, risk, market_ok, bool(swing['breakout_confirmed']))

            score = 0.0
            if div_tf == '15m':
                score += 18
            if bull1h['found']:
                score += 22 if bull1h['kind'] == 'chain' else 12
            if bull15['found']:
                score += 18 if bull15['kind'] == 'chain' else 10
            if zone == 'in_zone':
                score += 18
            elif zone == 'near_zone':
                score += 8
            if swing['breakout_confirmed']:
                score += 14
            if market_ok:
                score += 8
            if volume_ratio >= 1.15:
                score += 8
            elif volume_ratio >= 1.0:
                score += 4
            if risk.rr_tp2 >= 3.0:
                score += 8
            elif risk.rr_tp2 >= settings.min_rr_tp2:
                score += 4
            if abs(risk.stop_loss_pct) <= 5.0:
                score += 6
            elif abs(risk.stop_loss_pct) <= settings.max_stop_pct:
                score += 3

            grade = grade_from_score(score)
            direct = self._direct_action(passed, rejected, risk)
            reason_summary = ' | '.join([
                f'{div_tf} 상승 다이버전스 {div["kind"]}',
                f'직전고 돌파 {"확인" if swing["breakout_confirmed"] else "미확인"}',
                f'피보나치 {zone}',
                f'손절 {risk.stop_loss_pct}%',
                f'TP1 {risk.tp1_pct}%',
                f'TP2 {risk.tp2_pct}%',
                f'시장 {market_regime}',
                f'익절 기준 {extension_basis}',
            ])

            return ScanSignal(
                symbol=symbol,
                state='candidate' if passed else ('wait' if direct.status == 'wait' else 'reject'),
                grade=grade,
                score=round(score, 2),
                current_price=round(current, 8),
                entry_zone_status=zone,
                divergence_timeframe=div_tf,
                divergence_kind=div['kind'],
                chain_points=int(div.get('points', 0)),
                breakout_confirmed=bool(swing['breakout_confirmed']),
                market_regime=market_regime,
                volume_ratio=volume_ratio,
                resistance_room_pct=resistance_room_pct,
                strength_profile=strength_profile,
                risk=risk,
                direct=direct,
                filters_passed=passed,
                rejected_reasons=rejected,
                reason_summary=reason_summary,
            )
        except Exception:
            return None

    async def scan(self) -> ScanResponse:
        started = perf_counter()
        warnings: list[str] = []
        try:
            btc15, btc1h, btc4h = await self._frames('KRW-BTC')
            _ = btc15
        except Exception as exc:
            warnings.append(f'btc_regime_fetch_failed:{exc.__class__.__name__}')
            return ScanResponse(mode='final', scanned_symbols=0, matched_symbols=0, elapsed_seconds=round(perf_counter() - started, 2), top_picks=[], signals=[], warnings=warnings)

        try:
            markets = await self.client.top_markets(settings.scan_market_limit)
        except Exception as exc:
            warnings.append(f'upbit_market_fetch_failed:{exc.__class__.__name__}')
            return ScanResponse(mode='final', scanned_symbols=0, matched_symbols=0, elapsed_seconds=round(perf_counter() - started, 2), top_picks=[], signals=[], warnings=warnings)

        tasks = [self.analyze_symbol(m, btc1h=btc1h, btc4h=btc4h) for m in markets]
        analyzed = [x for x in await asyncio.gather(*tasks) if x is not None]
        analyzed.sort(key=lambda x: (x.filters_passed, x.score, x.risk.rr_tp2), reverse=True)
        passed = [x for x in analyzed if x.filters_passed]
        return ScanResponse(
            mode='final',
            scanned_symbols=len(markets),
            matched_symbols=len(passed),
            elapsed_seconds=round(perf_counter() - started, 2),
            top_picks=passed[:settings.top_pick_count],
            signals=analyzed[:max(20, settings.top_pick_count)],
            warnings=warnings,
        )

    def health(self) -> HealthResponse:
        return HealthResponse(status='ok', version=settings.app_version)

    def ready(self) -> ReadyResponse:
        return ReadyResponse(
            version=settings.app_version,
            scan_market_limit=settings.scan_market_limit,
            top_pick_count=settings.top_pick_count,
            max_stop_pct=settings.max_stop_pct,
            min_rr_tp1=settings.min_rr_tp1,
            min_rr_tp2=settings.min_rr_tp2,
        )
