from __future__ import annotations

"""
제이드 파동 스캐너 v4 — 핵심 엔진

매매 프로세스 (PDF 원칙 그대로):
  1. 1시간봉 기준으로 앞전 파동 방향 확인 (상승/하락/잔파동)
  2. 상승 다이버전스 연계(3꼭지) 탐지 (15m·1h 우선, 4h 보조)
  3. RSI 극값(과매도/과매수 경계) 확인 — 중간값 다이버전스는 신뢰도 낮음
  4. 피보나치 되돌림 0.618~0.786 구간 진입 여부 확인
  5. 직전 고점(파동 목적) 돌파 여부 확인
  6. BTC 시장 레짐(위험 선호/회피) 필터
  7. 손절 = 피보나치 1 (swing 저점)
  8. 1차 익절 = 피보나치 0 (swing 고점)
  9. 2차 익절 = 1.272 or 1.618 확장 (강도에 따라 동적 선택)
"""

import asyncio
from time import perf_counter

from app.core.config import settings
from app.core.schemas import (
    DirectAction,
    DivergenceDetail,
    HealthResponse,
    ReadyResponse,
    RiskPlan,
    ScanResponse,
    ScanSignal,
)
from app.services.divergence import detect_bullish
from app.services.fibonacci import recent_bullish_swing, zone_status
from app.services.indicators import enrich
from app.services.pivots import mark_pivots, recent_pivot_highs, recent_pivot_lows
from app.services.scoring import grade_from_score
from app.services.upbit_client import UpbitClient


class ScannerEngine:
    def __init__(self) -> None:
        self.client = UpbitClient()

    # ──────────────────────────────────────────────
    # 내부 헬퍼
    # ──────────────────────────────────────────────

    async def _frames(self, market: str):
        """15m·1h·4h 캔들 + 지표 + 피봇 마킹 묶음"""
        df15, df1h, df4h = await asyncio.gather(
            self.client.candles(market, 15, settings.candles_limit_15m),
            self.client.candles(market, 60, settings.candles_limit_1h),
            self.client.candles(market, 240, settings.candles_limit_4h),
        )
        kwargs = dict(
            rsi_period=settings.rsi_period,
            ema_fast=settings.ema_fast,
            ema_slow=settings.ema_slow,
            ema_regime=settings.ema_regime,
        )
        pivot_kwargs = dict(left=settings.pivot_left, right=settings.pivot_right)
        df15 = mark_pivots(enrich(df15, **kwargs), **pivot_kwargs)
        df1h = mark_pivots(enrich(df1h, **kwargs), **pivot_kwargs)
        df4h = mark_pivots(enrich(df4h, **kwargs), **pivot_kwargs)
        return df15, df1h, df4h

    def _volume_ratio(self, df) -> float:
        """vol_ma_5 / vol_ma_20 — 1.0 이상이면 거래량 증가 중"""
        row = df.iloc[-1]
        base = float(row.get('vol_ma_20', 0) or 0)
        if base <= 0:
            return 0.0
        return round(float(row.get('vol_ma_5', 0) or 0) / base, 2)

    def _market_regime(self, btc1h, btc4h) -> tuple[str, bool]:
        """
        BTC EMA200 기반 시장 레짐 판단.
        risk_on     : 진입 OK
        neutral_ok  : 진입 가능하지만 보수적으로
        risk_off    : 롱 진입 자제
        """
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
        """현재가 기준 저항(직전 고점)까지 여유 %"""
        current = float(df1h['close'].iloc[-1])
        recent_high = float(df1h['high'].tail(80).max())
        ref = max(anchor_high, recent_high)
        if current <= 0:
            return 0.0
        return round((ref / current - 1.0) * 100.0, 2)

    def _overheated(self, df1h) -> bool:
        """최근 20봉 저가 대비 상승률이 임계값 초과 → 과열"""
        val = float(df1h['pct_from_20_low'].iloc[-1])
        return val >= settings.overheated_pct_from_20_low

    def _extension_profile(
        self,
        chain_points: int,
        volume_ratio: float,
        breakout_confirmed: bool,
        market_regime: str,
        rsi_1h: float,
    ) -> tuple[str, str]:
        """
        강도(strength) 점수에 따라 익절 2차 레벨 선택.
        strong   → 1.618 확장 (강세 목표)
        standard → 1.272 확장 (보수 목표)
        """
        strength = 0
        if chain_points >= 3:
            strength += 1
        if volume_ratio >= 1.15:
            strength += 1
        if breakout_confirmed:
            strength += 1
        if market_regime == 'risk_on':
            strength += 1
        if 40 <= rsi_1h <= 65:
            strength += 1

        if strength >= 4:
            return 'strong', 'fib_1.618_extension'
        return 'standard', 'fib_1.272_extension'

    def _risk_plan(
        self,
        current: float,
        swing: dict,
        extension_basis: str,
        resistance_room_pct: float,
    ) -> RiskPlan:
        """
        손절·익절 가격 계산.
        - 손절 : 피보나치 1 (swing 저점) × (1 - stop_buffer)
        - 1차  : 피보나치 0 (swing 고점 = 직전 고점)
        - 2차  : 1.272 또는 1.618 확장; 저항까지 여유가 작으면 저항에 cap
        """
        stop_price = float(swing['fib_1']) * (1.0 - settings.stop_buffer_pct / 100.0)
        tp1_price = float(swing['fib_0'])
        raw_tp2 = float(
            swing['ext_1618'] if extension_basis == 'fib_1.618_extension' else swing['ext_1272']
        )
        if resistance_room_pct > 0:
            resistance_cap = current * (1.0 + resistance_room_pct / 100.0)
            tp2_price = min(raw_tp2, resistance_cap)
        else:
            tp2_price = raw_tp2

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

    def _passes_filters(
        self,
        current: float,
        swing: dict,
        zone: str,
        volume_ratio: float,
        overheated: bool,
        resistance_room_pct: float,
        risk: RiskPlan,
        market_ok: bool,
        breakout_confirmed: bool,
        div_kind: str,
        rsi_extreme: bool,
    ) -> tuple[bool, list[str]]:
        """
        모든 필터 통과 여부 판단.
        rejected_reasons 에 실패 사유 수집.
        """
        rejected: list[str] = []
        upper = max(float(swing['fib_0618']), float(swing['fib_0786']))

        # 1. 시장 레짐
        if not market_ok:
            rejected.append('btc_market_regime_risk_off')

        # 2. 피보나치 구간
        if zone == 'out_zone':
            rejected.append('fib_zone_miss')

        # 3. 늦은 진입 (피보 구간 위로 이미 크게 올라간 경우)
        if current > upper * (1.0 + settings.late_entry_buffer_pct / 100.0):
            rejected.append('late_entry_after_bounce')

        # 4. 거래량 부족
        if volume_ratio < settings.min_volume_ratio:
            rejected.append('weak_volume')

        # 5. 과열
        if overheated:
            rejected.append('overheated_after_recent_rally')

        # 6. 직전고 돌파 미확인 (파동 목적 미달성)
        if not breakout_confirmed:
            rejected.append('breakout_not_confirmed')

        # 7. 저항까지 여유 부족
        if resistance_room_pct < settings.min_resistance_room_pct:
            rejected.append('too_close_to_resistance')

        # 8. 손절 구조 이상
        if risk.stop_loss_pct >= 0:
            rejected.append('invalid_stop_structure')

        # 9. 손절 너무 넓음
        if abs(risk.stop_loss_pct) > settings.max_stop_pct:
            rejected.append('stop_too_wide')
        if abs(risk.stop_loss_pct) > settings.absolute_max_stop_pct:
            rejected.append('stop_above_absolute_limit')

        # 10. 수익비 부족
        if risk.rr_tp1 < settings.min_rr_tp1:
            rejected.append('tp1_rr_too_low')
        if risk.rr_tp2 < settings.min_rr_tp2:
            rejected.append('tp2_rr_too_low')

        # 11. RSI 극값 미확인 (중간값 다이버전스 — 신뢰도 낮음)
        if not rsi_extreme:
            rejected.append('rsi_not_at_extreme')

        return len(rejected) == 0, rejected

    def _direct_action(self, passed: bool, rejected: list[str], risk: RiskPlan) -> DirectAction:
        """GPT 연동용 즉각 행동 안내"""
        if passed:
            return DirectAction(
                status='buyable',
                action='진입 가능',
                message=(
                    f'진입가 {risk.entry_price:,} | '
                    f'손절 {risk.stop_price:,} ({risk.stop_loss_pct}%) | '
                    f'1차 익절 {risk.tp1_price:,} ({risk.tp1_pct}%) | '
                    f'최종 익절 {risk.tp2_price:,} ({risk.tp2_pct}%)'
                ),
            )
        if 'fib_zone_miss' in rejected or 'late_entry_after_bounce' in rejected:
            return DirectAction(
                status='wait',
                action='대기',
                message='피보나치 0.618~0.786 눌림 구간 미달. 추격 매수 금지. 되돌림 대기.',
            )
        return DirectAction(
            status='reject',
            action='매수 금지',
            message=f'필터 미통과: {", ".join(rejected[:3])}',
        )

    def _score(
        self,
        bull15: dict,
        bull1h: dict,
        bull4h: dict,
        zone: str,
        breakout_confirmed: bool,
        market_ok: bool,
        volume_ratio: float,
        risk: RiskPlan,
        rsi_extreme: bool,
    ) -> float:
        """
        신호 점수 계산.
        chain 다이버전스 > general > 없음
        """
        s = 0.0

        # 다이버전스 (15m 주도)
        if bull15['found']:
            s += 22 if bull15['kind'] == 'chain' else 12
        # 1h 다이버전스
        if bull1h['found']:
            s += 20 if bull1h['kind'] == 'chain' else 10
        # 4h 방향 보조
        if bull4h['found']:
            s += 12 if bull4h['kind'] == 'chain' else 6

        # RSI 극값 (점선 구간 컨펌)
        if rsi_extreme:
            s += 8

        # 피보나치 zone
        if zone == 'in_zone':
            s += 18
        elif zone == 'near_zone':
            s += 8

        # 직전 고점 돌파 (파동 목적 달성)
        if breakout_confirmed:
            s += 12

        # 시장 레짐
        if market_ok:
            s += 8

        # 거래량
        if volume_ratio >= 1.2:
            s += 8
        elif volume_ratio >= 1.0:
            s += 4

        # 손절 너비 (타이트할수록 좋음)
        stop_abs = abs(risk.stop_loss_pct)
        if stop_abs <= 4.0:
            s += 6
        elif stop_abs <= settings.max_stop_pct:
            s += 3

        # 수익비
        if risk.rr_tp2 >= 3.0:
            s += 8
        elif risk.rr_tp2 >= settings.min_rr_tp2:
            s += 4

        return round(s, 2)

    # ──────────────────────────────────────────────
    # 공개 API
    # ──────────────────────────────────────────────

    async def analyze_symbol(
        self,
        symbol: str,
        btc1h=None,
        btc4h=None,
    ) -> ScanSignal | None:
        try:
            df15, df1h, df4h = await self._frames(symbol)
            if min(len(df15), len(df1h), len(df4h)) < 120:
                return None

            # BTC 레짐 (개별 조회 시)
            if btc1h is None or btc4h is None:
                _, btc1h, btc4h = await self._frames('KRW-BTC')

            current = float(df1h['close'].iloc[-1])
            rsi_1h = float(df1h['rsi'].iloc[-1])
            rsi_15m = float(df15['rsi'].iloc[-1])

            market_regime, market_ok = self._market_regime(btc1h, btc4h)
            volume_ratio = self._volume_ratio(df1h)
            overheated = self._overheated(df1h)

            # 다이버전스 탐지 (15m·1h·4h)
            div_kwargs = dict(
                min_gap=settings.pivot_min_gap,
                max_gap=settings.pivot_max_gap,
                min_chain_span=settings.min_chain_span,
            )
            bull15 = detect_bullish(recent_pivot_lows(df15), **div_kwargs)
            bull1h = detect_bullish(recent_pivot_lows(df1h), **div_kwargs)
            bull4h = detect_bullish(recent_pivot_lows(df4h), **div_kwargs)

            # 최소 15m 또는 1h 에서 다이버전스 없으면 제외
            if not (bull15['found'] or bull1h['found']):
                return None

            # 주요 다이버전스 선택 (15m 우선)
            primary = bull15 if bull15['found'] else bull1h
            primary_tf = '15m' if bull15['found'] else '1h'
            rsi_extreme = bool(primary.get('rsi_extreme', False))

            # 피보나치 스윙 탐지
            swing = recent_bullish_swing(
                df1h,
                recent_pivot_highs(df1h),
                recent_pivot_lows(df1h),
            )
            if swing is None:
                return None

            # 피보나치 구간 확인
            zone = zone_status(
                current,
                float(swing['fib_0618']),
                float(swing['fib_0786']),
                settings.fib_zone_tolerance_pct,
            )

            resistance_room_pct = self._resistance_room(df1h, float(swing['anchor_high']))
            chain_pts = int(primary.get('points', 0))

            strength_profile, extension_basis = self._extension_profile(
                chain_pts, volume_ratio, bool(swing['breakout_confirmed']), market_regime, rsi_1h
            )

            risk = self._risk_plan(current, swing, extension_basis, resistance_room_pct)

            passed, rejected = self._passes_filters(
                current=current,
                swing=swing,
                zone=zone,
                volume_ratio=volume_ratio,
                overheated=overheated,
                resistance_room_pct=resistance_room_pct,
                risk=risk,
                market_ok=market_ok,
                breakout_confirmed=bool(swing['breakout_confirmed']),
                div_kind=primary['kind'],
                rsi_extreme=rsi_extreme,
            )

            score = self._score(
                bull15, bull1h, bull4h, zone,
                bool(swing['breakout_confirmed']), market_ok,
                volume_ratio, risk, rsi_extreme,
            )
            grade = grade_from_score(score)
            direct = self._direct_action(passed, rejected, risk)

            # 요약 사유 (reason_summary)
            reason_parts = [
                f'{primary_tf} 다이버전스 {primary["kind"]}',
                f'RSI극값 {"확인" if rsi_extreme else "미확인"}',
                f'직전고 돌파 {"확인" if swing["breakout_confirmed"] else "미확인"}',
                f'피보존 {zone}',
                f'손절 {risk.stop_loss_pct}%',
                f'TP1 {risk.tp1_pct}%',
                f'TP2 {risk.tp2_pct}%',
                f'시장 {market_regime}',
            ]
            if bull1h['found'] and primary_tf == '15m':
                reason_parts.append('1h 다이버전스 보조확인')
            if bull4h['found']:
                reason_parts.append('4h 방향 보조')

            state = 'candidate' if passed else ('wait' if direct.status == 'wait' else 'reject')

            return ScanSignal(
                symbol=symbol,
                state=state,
                grade=grade,
                score=score,
                current_price=round(current, 8),
                primary_divergence=DivergenceDetail(
                    found=bool(primary['found']),
                    kind=str(primary['kind']),
                    points=chain_pts,
                    timeframe=primary_tf,
                ),
                divergence_1h=DivergenceDetail(
                    found=bool(bull1h['found']),
                    kind=str(bull1h['kind']),
                    points=int(bull1h.get('points', 0)),
                    timeframe='1h',
                ),
                divergence_4h=DivergenceDetail(
                    found=bool(bull4h['found']),
                    kind=str(bull4h['kind']),
                    points=int(bull4h.get('points', 0)),
                    timeframe='4h',
                ),
                entry_zone_status=zone,
                breakout_confirmed=bool(swing['breakout_confirmed']),
                market_regime=market_regime,
                rsi_1h=round(rsi_1h, 2),
                rsi_15m=round(rsi_15m, 2),
                volume_ratio=volume_ratio,
                resistance_room_pct=resistance_room_pct,
                strength_profile=strength_profile,
                risk=risk,
                direct=direct,
                filters_passed=passed,
                rejected_reasons=rejected,
                reason_summary=' | '.join(reason_parts),
            )

        except Exception:
            return None

    async def scan(self) -> ScanResponse:
        started = perf_counter()
        warnings: list[str] = []

        # BTC 레짐 선조회
        try:
            _, btc1h, btc4h = await self._frames('KRW-BTC')
        except Exception as exc:
            warnings.append(f'btc_regime_fetch_failed:{exc.__class__.__name__}')
            return ScanResponse(
                scanned_symbols=0, matched_symbols=0,
                elapsed_seconds=round(perf_counter() - started, 2),
                top_picks=[], signals=[], warnings=warnings,
            )

        # 마켓 목록 조회
        try:
            markets = await self.client.top_markets(settings.scan_market_limit)
        except Exception as exc:
            warnings.append(f'upbit_market_fetch_failed:{exc.__class__.__name__}')
            return ScanResponse(
                scanned_symbols=0, matched_symbols=0,
                elapsed_seconds=round(perf_counter() - started, 2),
                top_picks=[], signals=[], warnings=warnings,
            )

        # 병렬 분석
        tasks = [self.analyze_symbol(m, btc1h=btc1h, btc4h=btc4h) for m in markets]
        analyzed = [x for x in await asyncio.gather(*tasks) if x is not None]

        # 정렬: 통과 여부 > 점수 > 수익비
        analyzed.sort(key=lambda x: (x.filters_passed, x.score, x.risk.rr_tp2), reverse=True)
        passed = [x for x in analyzed if x.filters_passed]

        return ScanResponse(
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
