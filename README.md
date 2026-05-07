# Nongsa Scanner API v2.0 Final

국내주식 `농사매매법` 후보를 찾기 위한 FastAPI 기반 스캐너입니다.  
GitHub 업로드 후 Render에 바로 배포할 수 있게 구성했습니다.

## 핵심 구현

이 프로젝트는 특정 유튜버의 강의 내용을 그대로 복제하는 목적이 아니라, 공개적으로 알려진 농사매매식 차트 구조를 **기계 판정 가능한 기술적 조건**으로 일반화한 스캐너입니다.

### A타입: 메인 후보

- 224일선 아래 또는 224일선 근처
- 최근 100~180거래일 기준 장기간 224일선 아래 체류
- 쌍바닥 조건 통과
- 공구리 조건 통과
- 20일 평균 거래대금 기준 통과

### B타입: 탐색 후보

- 224일선 아래 또는 224일선 근처
- 장기간 224일선 아래 체류
- 쌍바닥 또는 공구리 중 하나만 통과
- 20일 평균 거래대금 기준 통과

## 쌍바닥 판정 로직

`detect_w_bottom()`은 아래를 계산합니다.

1. 최근 100거래일 내 로컬 저점 탐색
2. 두 저점 간격 10~65거래일
3. 두 저점 가격 차이 -12%~+15% 이내
4. 두 저점 사이 목선 반등폭 8% 이상
5. 2저점 이후 현재가 3% 이상 반등
6. 목선 대비 과도한 추격 구간 제외
7. 점수 55점 이상이면 쌍바닥 통과

응답에는 `low1_date`, `low2_date`, `neckline`, `rebound_from_low2_pct`, `score`가 포함됩니다.

## 공구리 판정 로직

`detect_gonguri()`는 아래를 계산합니다.

1. 최근 45거래일 내 돌파봉 탐색
2. 돌파 전 45~90거래일 구간의 피벗 고점/박스권 상단 산출
3. 돌파봉 종가가 기준선 1.5% 이상 상회 또는 고가가 3% 이상 상회
4. 돌파봉 거래량이 직전 20일 평균 대비 1.05배 이상
5. 돌파 이후 종가가 기준선 -5% 이내에서 버팀
6. 현재가가 기준선 대비 +25% 이상이면 과열 제외
7. 점수 58점 이상이면 공구리 통과

응답에는 `resistance`, `breakout_date`, `breakout_volume_ratio_20d`, `after_min_close_vs_resistance_pct`, `current_vs_resistance_pct`, `score`가 포함됩니다.

## 로컬 실행

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

브라우저에서 확인:

```text
http://127.0.0.1:8000/health
http://127.0.0.1:8000/scan/main?market=ALL&limit=120
http://127.0.0.1:8000/scan/all?market=ALL&limit=120
http://127.0.0.1:8000/analyze/005930
http://127.0.0.1:8000/debug/005930
```

## Render 배포

Build Command:

```bash
pip install -r requirements.txt
```

Start Command:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

환경변수 권장값:

```text
DEFAULT_MARKET=ALL
SCAN_LIMIT=120
MAX_RESULTS=30
MIN_TRADING_VALUE=1500000000
CACHE_TTL_SEC=900
PYKRX_SLEEP_SEC=0.03
```

Render Free에서는 `limit=80~120` 권장. 300개 이상은 pykrx 조회 지연으로 502/timeout 가능성이 있습니다.

## Custom GPT 연결

`custom_gpt_schema.yaml`에서 아래 부분만 본인 Render 주소로 바꾸세요.

```yaml
servers:
  - url: https://YOUR-RENDER-SERVICE.onrender.com
```

## 한계

- 이 스캐너는 투자 추천이 아니라 조건 검색기입니다.
- 쌍바닥/공구리는 사람의 차트 해석 영역이 포함되므로, 최종 매수 전 차트 육안 검토가 필요합니다.
- 관리종목, 감사의견, 자본잠식, 거래정지 등 재무/공시 위험은 별도 데이터 연동 전까지 완전 자동 제외되지 않습니다.
- 현재 버전은 pykrx 기반이라 장중 실시간 데이터가 아니라 KRX 일봉 데이터 기준입니다.
