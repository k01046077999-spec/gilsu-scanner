# 길수매매법 코인 검색기 patched

이 버전은 기존 MVP를 안정화하고, 피보나치 기반 손절/익절 라인을 같이 내려주도록 보강한 패치본이다.

## 추가된 핵심 변경
- HTTP 클라이언트 재사용
- 외부 API retry + backoff
- 스캔 동시성 제한
- 스캔 결과 캐시
- `/ready` 엔드포인트 추가
- 실패 심볼 / 소요시간 진단 정보 추가
- 각 결과에 `risk_management` 추가
  - `stop_loss`: fib 1 기준
  - `tp1`: 최근 스윙 고점/저점 기준
  - `tp2`: fib extension 1.272 기준
  - 각 라인의 퍼센트 및 RR 포함

## 엔드포인트
- `GET /health`
- `GET /ready`
- `GET /scan/main`
- `GET /scan/sub`
- `GET /scan/symbol/{symbol}`

## Render
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
