# 길수매매법 코인 검색기 v0.7.9

이번 패치는 **필터 미세조정판이 아니라 안정화판**입니다.
핵심 목적은 업비트 호출 제한(429) 때문에 스캔 결과가 왜곡되는 문제를 줄이는 것입니다.

## 이번 버전 핵심
- 버전: `0.7.9`
- 거래소: **업비트 KRW 마켓 전용**
- 방향: **롱 전용**
- 출력: **손절/익절 퍼센트 우선**
- 안정화: **요청 간 간격 부여 + 재시도 + 백오프**

## v0.7.9 반영 내용
- 업비트 호출에 전역 pacing 적용
- 429 발생 시 재시도/backoff 강화
- 네트워크 timeout/연결 오류 재시도 강화
- scan concurrency 축소
- universe/prefilter 크기 축소로 요청량 완화
- diagnostics에 `stability_patch: rate_limit_backoff_v079` 추가

## 현재 실전 해석
이 버전은 신호를 더 많이 억지로 만드는 버전이 아닙니다.
먼저 **호출 안정성**을 올려서, `신호 없음`이 정말 전략 결과인지 API 제한 때문인지 구분하기 쉽게 만드는 버전입니다.

## 엔드포인트
- `GET /`
- `GET /health`
- `GET /ready`
- `GET /scan/main`
- `GET /scan/sub`
- `GET /scan/sub/top`
- `GET /scan/symbol/{symbol}`
