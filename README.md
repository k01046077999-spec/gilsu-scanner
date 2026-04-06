# 길수매매법 코인 검색기 MVP - 안정화 패치본

이 버전은 기존 MVP의 API 연동 불안정 문제를 줄이기 위한 안정화 버전이다.

## 주요 개선점
- `httpx.AsyncClient` 재사용 구조로 변경
- Binance 호출 retry / backoff 추가
- 스캔 동시성 제한 추가
- `/scan/main`, `/scan/sub` 결과 캐시 추가
- `failed_symbols`, `duration_ms` 등 진단 정보 응답 추가
- `/ready` 엔드포인트 추가
- 단일 심볼 실패와 전체 스캔 실패를 분리
- 기본 스캔 limit 및 동시 처리량 축소

## 엔드포인트
- `GET /health` : 헬스체크
- `GET /ready` : 준비 상태 및 주요 설정값 확인
- `GET /scan/main` : 메인 기준 추천 검색
- `GET /scan/sub` : 서브 기준 추천 검색
- `GET /scan/symbol/{symbol}` : 특정 심볼 상세 판정

## Render 권장
- Free 플랜은 cold start 영향이 크다.
- 실사용이면 유료 플랜 전환이 더 낫다.
- 그래도 코드 구조를 먼저 고치는 게 우선이다.

## Start command
```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```
