# 길수매매법 코인 검색기 v0.7.7 업비트 전용 롱 전용 완성판

이 프로젝트는 업로드된 PDF의 핵심 원칙을 바탕으로 만든 **업비트 KRW 마켓 전용 길수매매법 검색기**입니다.
이번 버전은 **구조 신호**와 **실전 체결 가능 신호**를 분리하는 방향으로 보완한 실전형 정합본입니다.

## 이번 버전의 기준
- 버전: `0.7.4`
- 거래소: **Upbit KRW market only**
- 방향: **bullish(롱) only**
- 출력 우선순위: **손절/익절 퍼센트 우선, 가격 보조**

## 냉정한 보완 포인트
기존 v0.7.3은 구조적으로 맞는 신호라도 아래와 같은 종목이 통과할 수 있었습니다.
- 손절폭이 지나치게 짧음
- 1차/2차 익절폭이 너무 작음
- RR이 좋아 보여도 손절이 과도하게 짧아 생긴 착시일 수 있음

즉, **다이버전스가 있느냐**와 **실제로 매매할 만하냐**를 분리할 필요가 있었습니다.

## v0.7.7 반영 내용
### 공통
- 업비트 현물 기준 **bullish(롱) 결과만 허용**
- 응답 JSON에서 `current_price`, `stop_loss_pct`, `tp1_pct`, `tp2_pct`를 가격보다 앞에 배치
- `metrics.risk_management.display_order` 추가
- `diagnostics.practical_thresholds` 추가

### 메인 실전 필터
- 최소 손절폭: **1.2% 이상**
- 최소 1차 익절폭: **3.0% 이상**
- 최소 2차 익절폭: **6.0% 이상**
- 최소 RR(TP2): **2.0 이상**

### 서브 실전 필터
- 최소 손절폭: **1.0% 이상**
- 최소 1차 익절폭: **2.5% 이상**
- 최소 2차 익절폭: **5.0% 이상**
- 최소 RR(TP2): **1.8 이상**

### Top Picks 보정
- Top Picks에서도 동일하게 너무 짧은 손절/익절 종목 제외
- 구조 점수만 높고 실전 여유가 없는 종목은 상단 추천에서 제거


## v0.7.7 추가 완성 포인트
- `results`와 `watchlist`를 분리해 A급 진입 신호와 B급 감시 신호를 따로 제공
- `success_count`처럼 혼동되는 카운트 제거 후 `analyzed_count`, `structural_candidate_count`, `practical_pass_count`, `watchlist_count`로 세분화
- watchlist는 구조는 맞지만 실전 기준 1~2개만 근접 미달한 종목만 허용
- 늦은 진입, 비정상 TP 구조, 비강세 신호는 watchlist에도 올리지 않음

## 엔드포인트
- `GET /` : 서비스 / 버전 확인
- `GET /health` : 헬스체크 + 버전
- `GET /ready` : 설정값 확인 + 버전
- `GET /scan/main` : 메인 검색
- `GET /scan/sub` : 서브 검색
- `GET /scan/sub/top` : 서브 Top Picks
- `GET /scan/symbol/{symbol}` : 특정 심볼 상세 판정

## 빠른 실행
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## 로컬 테스트
```bash
curl 'http://127.0.0.1:8000/'
curl 'http://127.0.0.1:8000/health'
curl 'http://127.0.0.1:8000/ready'
curl 'http://127.0.0.1:8000/scan/main'
curl 'http://127.0.0.1:8000/scan/sub'
curl 'http://127.0.0.1:8000/scan/sub/top'
curl 'http://127.0.0.1:8000/scan/symbol/FIL?mode=main'
```

## Render 배포
- GitHub push
- Render Web Service auto deploy 또는 manual deploy
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## 배포 후 검증 포인트
1. `/` 에서 `version = 0.7.4` 확인
2. `/ready` 에서 기본 설정값 확인
3. `/scan/main` 또는 `/scan/sub` diagnostics 에서 `practical_thresholds` 확인
4. `scan/symbol` 응답에서 퍼센트 필드가 가격 필드보다 앞에 오는지 확인

## 주의
이 버전은 **실전 필터 보완본**이다.
신호 수는 줄 수 있다. 대신 잡음 손절 가능성이 높은 종목과 먹을 구간이 지나치게 좁은 종목을 더 강하게 제거한다.
