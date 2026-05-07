# 제이드 파동 스캐너 v4

**제이드 파동 이론** 기반 업비트 KRW 마켓 자동 스캐너.  
RSI 다이버전스 연계 + 피보나치 되돌림 + 직전고 돌파를 모두 확인합니다.

---

## 매매 로직 요약 (PDF 원칙)

| 단계 | 내용 |
|------|------|
| 1 | 1시간봉 기준 앞전 파동 방향 확인 (상승/하락/잔파동) |
| 2 | **15m·1h 상승 다이버전스 연계(3꼭지)** 탐지 |
| 3 | RSI 극값 구간(≤42 / ≥58) 확인 — 중간값 다이버전스 제외 |
| 4 | **피보나치 0.618~0.786** 되돌림 구간 진입 여부 확인 |
| 5 | **직전 고점 돌파** 여부 확인 (파동의 목적 달성) |
| 6 | BTC EMA200 기반 시장 레짐(위험 선호/회피) 필터 |
| 7 | 손절 = **피보나치 1 (swing 저점)** |
| 8 | 1차 익절 = **피보나치 0 (직전 고점)** |
| 9 | 2차 익절 = **1.272 또는 1.618 확장** (강도에 따라 동적 선택) |

---

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/health` | 서버 상태 확인 |
| GET | `/ready` | 설정값 확인 |
| GET | `/scan` | 전체 마켓 스캔 |
| GET | `/scan/symbol/{symbol}` | 단일 종목 분석 (예: `BTC`, `ETH`, `KRW-SOL`) |

### `/scan` 응답 구조

```json
{
  "scanned_symbols": 80,
  "matched_symbols": 3,
  "elapsed_seconds": 12.4,
  "top_picks": [...],
  "signals": [...],
  "warnings": []
}
```

### `ScanSignal` 주요 필드

| 필드 | 설명 |
|------|------|
| `symbol` | 마켓 코드 (KRW-BTC 등) |
| `state` | `candidate` / `wait` / `reject` |
| `grade` | `S` / `A` / `B` / `C` / `D` |
| `score` | 0~100+ 점수 |
| `primary_divergence` | 주요 다이버전스 정보 (timeframe, kind, points) |
| `divergence_1h` | 1시간봉 다이버전스 |
| `divergence_4h` | 4시간봉 다이버전스 (방향 보조) |
| `entry_zone_status` | `in_zone` / `near_zone` / `out_zone` |
| `breakout_confirmed` | 직전 고점 돌파 여부 |
| `market_regime` | `risk_on` / `neutral_ok` / `risk_off` |
| `risk.stop_price` | 손절가 (피보나치 1) |
| `risk.tp1_price` | 1차 익절가 (직전 고점) |
| `risk.tp2_price` | 2차 익절가 (확장 레벨) |
| `risk.rr_tp2` | 최종 수익비 |
| `direct.status` | `buyable` / `wait` / `reject` |
| `direct.message` | GPT 연동용 한국어 행동 안내 |

---

## 점수 체계

| 항목 | 최대 점수 |
|------|-----------|
| 15m chain 다이버전스 | 22 |
| 1h chain 다이버전스 | 20 |
| RSI 극값 확인 | 8 |
| 피보나치 in_zone | 18 |
| 직전 고점 돌파 | 12 |
| 4h 방향 보조 | 12 |
| 시장 레짐 risk_on | 8 |
| 거래량 비율 | 8 |
| 손절 타이트 | 6 |
| 수익비 우수 | 8 |

| 등급 | 점수 |
|------|------|
| S | 90+ |
| A | 75+ |
| B | 60+ |
| C | 45+ |
| D | ~44 |

---

## 필터 통과 조건 (모두 만족해야 `candidate`)

1. BTC 시장 레짐 `risk_on` 또는 `neutral_ok`
2. 피보나치 zone `in_zone` 또는 `near_zone`
3. 늦은 진입 아님 (fib_0618 기준 +2.5% 이내)
4. 거래량 비율 ≥ 0.95
5. 과열 아님 (20봉 저가 대비 +35% 미만)
6. 직전 고점 돌파 확인
7. 저항까지 여유 ≥ 3%
8. 유효한 손절 구조 (stop < entry)
9. 손절 너비 ≤ 8% (절대 한도 12%)
10. TP1 수익비 ≥ 1.2
11. TP2 수익비 ≥ 2.0
12. RSI 극값 구간에서 발생한 다이버전스

---

## 환경변수 (Render 배포 시)

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `UPBIT_BASE_URL` | `https://api.upbit.com` | 업비트 API |
| `SCAN_MARKET_LIMIT` | `80` | 스캔할 최대 종목 수 |
| `TOP_PICK_COUNT` | `8` | 상위 픽 개수 |
| `MIN_DAILY_ACC_TRADE_PRICE_KRW` | `3000000000` | 일 거래대금 필터 (3십억) |
| `MAX_STOP_PCT` | `8.0` | 최대 손절 % |
| `MIN_RR_TP2` | `2.0` | 최소 최종 수익비 |
| `OVERHEATED_PCT_FROM_20_LOW` | `35.0` | 과열 판단 기준 % |
| `EXCLUDE_MARKETS` | `` | 제외 마켓 (쉼표 구분, 예: `KRW-SHIB,KRW-DOGE`) |

---

## 로컬 실행

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Swagger UI: http://localhost:8000/docs

---

## 버전 히스토리

| 버전 | 주요 변경 |
|------|-----------|
| v4.0.0 | RSI 극값 필터 추가, DivergenceDetail 스키마 분리, 15m/1h/4h 개별 노출, 확장 레벨 동적 선택 |
| v3.0.0 | BTC 레짐 필터, breakout_confirmed, 동적 익절 레벨 |
| v2.2.1 | main/sub 이중 모드, 업비트 전용 |
| v0.8.2 | 초기 손실 축소 버전 |
