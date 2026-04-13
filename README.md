# Kai Upbit Final Dynamic Scanner

업비트 원화마켓 롱 전용 최종본이다.

이번 버전은 말로만 정리한 전략이 아니라, 바로 GitHub에 올리고 Render에 배포할 수 있게 만든 FastAPI 스캐너다.
핵심은 세 가지다.

1. 진입은 한 번에 한다.
2. 손절은 한 번에 한다.
3. 손절과 익절은 종목마다 다르게 계산한다.

## 전략 최종본

### 진입
아래 조건을 모두 통과할 때만 진입 후보로 본다.

- 업비트 KRW 마켓
- 롱만
- 15분봉 또는 1시간봉 상승 다이버전스 확인
- 최근 1시간봉 상승 스윙이 직전고를 실제로 돌파
- 현재 가격이 0.618~0.786 눌림 구간이거나 그 근처
- 비트코인 시장 레짐이 risk_off 가 아님
- 거래량비, 저항 여유, 손절폭, RR 필터 통과

### 손절
- 손절가 = 해당 스윙의 피보나치 1 이탈값
- 종목마다 다르다
- 손절폭이 너무 크면 진입 자체를 금지한다

### 익절
- 1차 익절 = 직전 상승 스윙 고점(피보나치 0)
- 최종 익절 = 피보나치 1.272 또는 1.618 확장
- 다이버전스 구조가 강하면 1.618, 아니면 1.272
- 상단 저항이 더 가까우면 그 저항에서 최종 익절을 잘라서 과욕을 막는다

## 왜 이렇게 바꿨나
고정 8% / 15% / 22% 익절은 운영은 편하지만 구조를 무시한다.
반대로 실시간 다이버전스 추적 익절은 현재 GPT 운영 구조상 자동화가 어렵다.
그래서 이번 버전은 **진입 시점에 이미 종목별 손절가/익절가를 계산해서 바로 지시**하는 방식으로 고정했다.

## 응답에서 바로 봐야 할 값
- `risk.entry_price`
- `risk.stop_price`
- `risk.stop_loss_pct`
- `risk.tp1_price`
- `risk.tp1_pct`
- `risk.tp2_price`
- `risk.tp2_pct`
- `direct.action`
- `direct.message`

## 엔드포인트
- `GET /` : 버전 확인
- `GET /health` : 헬스체크
- `GET /ready` : 핵심 설정 확인
- `GET /scan/final` : 최종 스캔
- `GET /scan/symbol/KRW-BTC` : 특정 종목 단건 판정

## 로컬 실행
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Render 배포
1. 이 폴더를 GitHub 저장소 루트에 올린다.
2. Render에서 New Web Service 선택
3. 저장소 연결
4. Build Command: `pip install -r requirements.txt`
5. Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

또는 `render.yaml`을 그대로 사용하면 된다.

## 최종 철학
이 스캐너는 “좋아 보이는 종목 나열기”가 아니다.

- 직전고 돌파 없는 반등은 버린다.
- 0.618~0.786 눌림이 아니면 추격으로 본다.
- 피보나치 1 손절이 너무 멀면 구조가 좋아도 거래를 포기한다.
- 익절도 고정 숫자가 아니라 스윙 구조로 계산한다.

즉, 좋은 자리만 치기 위한 필터형 시스템이다.
