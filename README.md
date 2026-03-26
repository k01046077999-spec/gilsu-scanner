# 대통령매매법 v1.7 안정화판

핵심 개선:
- 요청 핸들러 내부에서 `asyncio.create_task()` 호출 제거
- 시작 시 백그라운드 갱신 루프 실행
- `/gpt/main`, `/gpt/sub`는 캐시 조회만 수행
- 캐시가 없으면 `refreshing` 상태를 정상 JSON으로 반환
- 2단계 구조 유지: 전체 선별 -> 후보 정밀판정

## 엔드포인트
- `/health`
- `/scan/main`
- `/scan/sub`
- `/scan/symbol/{symbol}?mode=main`
- `/gpt/main`
- `/gpt/sub`
- `/refresh/main`
- `/refresh/sub`
