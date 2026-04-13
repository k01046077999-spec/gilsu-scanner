# 길수매매법 코인 검색기 v0.7.3 업비트 전용 롱 전용 완성본

이 프로젝트는 업로드된 PDF의 핵심 원칙을 바탕으로 만든 **업비트 KRW 마켓 전용 길수매매법 검색기**입니다.
이번 패키지는 **README / 코드 버전 / 설정값 / API 확인값**을 일치시킨 정합본입니다.

## 이번 버전의 기준
- 버전: `0.7.3`
- 거래소: **Upbit KRW market only**
- 메인: 엄격 유지
- 서브: 탐색형으로 완화

## 핵심 원칙 반영
- 1시간봉 중심으로 해석
- 1시간 이하에서는 일반 다이버전스보다 **다이버전스 연계(최소 3개 꼭지점)** 우선
- 애매하면 30분봉으로 보조 확인
- 4시간봉은 일반 다이버전스로 보조 확인
- Fib `0.618 ~ 0.786` 핵심
- Fib `1.0` 이탈 시 무효
- RSI는 중간 구간보다 극단 구간의 신호를 더 높게 평가

## v0.7.3 반영 내용
### 공통

### v0.7.3 최종 보정
- 업비트 현물 기준 **bullish(롱) 결과만 허용**
- bearish 결과는 `non_bullish_filtered`로 자동 제외
- 서브 Top Picks는 bullish + 실전 필터 통과 종목만 선별
- 서브 Top Picks 최소 `tp2_pct` 기준을 완화해 후보가 실제 추천으로 올라올 수 있게 조정
- Upbit KRW 마켓 전용 유지
- 손절/익절 퍼센트 출력 유지
- 루트(`/`), `/health`, `/ready`, `/scan/main`, `/scan/sub`, `/scan/symbol/{symbol}`에서 버전 확인 가능
- 불필요한 `__pycache__`, `*.pyc` 제거

### 메인
- 엄격 유지
- 높은 품질의 정예 신호 선별용

### 서브
- universe size: `120`
- prefilter size: `50`
- threshold: `36.0`
- 실전 탐색형 후보 발굴 목적

## 엔드포인트
- `GET /` : 서비스 / 버전 확인
- `GET /health` : 헬스체크 + 버전
- `GET /ready` : 설정값 확인 + 버전
- `GET /scan/main` : 메인 검색
- `GET /scan/sub` : 서브 검색
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
curl 'http://127.0.0.1:8000/scan/symbol/BTC?mode=main'
```

## Render 배포
- GitHub push
- Render Web Service auto deploy 또는 manual deploy
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## 배포 후 검증 포인트
1. `/` 에서 `version = 0.7.3` 확인
2. `/ready` 에서 `universe_size = 120`, `prefilter_size = 50` 확인
3. `/scan/sub` diagnostics 에서 `version = 0.7.3` 확인

## 주의
이 버전은 **전략 정합성 정리본**이다.
수익률 검증은 별도의 백테스트로 확인해야 한다.
