# JADE 롱 스캐너 🟢

업비트 KRW 전체 종목 대상 **롱 신호 스캐너**  
파동의 심화 전략 — RSI 상승 다이버전스 연계 + 피보나치 되돌림

---

## 배포 방법 (GitHub + Render, 무료)

### 1단계 — GitHub에 올리기

```bash
cd jade-scanner
git init
git add .
git commit -m "JADE 롱 스캐너 초기 배포"
```

GitHub에서 새 Repository 만들고 (이름: `jade-scanner`):

```bash
git remote add origin https://github.com/본인아이디/jade-scanner.git
git branch -M main
git push -u origin main
```

### 2단계 — Render에 배포

1. [render.com](https://render.com) 접속 → 회원가입 (GitHub 연동)
2. **New → Web Service** 클릭
3. 방금 만든 `jade-scanner` 레포 선택
4. 설정:
   - **Name**: jade-scanner (원하는 이름)
   - **Environment**: Node
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Plan**: Free
5. **Deploy** 클릭

배포 완료되면 `https://jade-scanner-xxxx.onrender.com` 같은 URL이 생김  
→ 그 URL로 어디서든 접속 가능!

---

## 로컬에서 테스트

```bash
npm install
npm start
```

→ http://localhost:3000 접속

---

## 사용법

1. 봉 주기 선택 (기본: 1H)
2. **▲ 전체 스캔** 클릭
3. 업비트 KRW 전체 종목 순차 분석 (약 4~5분)
4. 롱 신호 목록에서 점수 높은 종목 확인
5. **상세 ▸** 클릭 → 피보나치 레벨 + 손절가 + 익절 목표 확인
6. 트레이딩뷰에서 최종 확인 후 진입

---

## 신호 등급

| 등급 | 조건 |
|------|------|
| `▲ LONG ★` | 80점 이상 — 즉시 확인 필요 |
| `▲ LONG` | 60~79점 — 진입 검토 |
| `↑ 대기` | 연계 미충족 — 관찰 중 |

---

## 점수 산정

- 다이버전스 3연계 충족: +30점
- RSI 과매도 컨펌: +20점  
- 피보 0.618~0.786 구간: +20점
- 상승 파동 특성 유지: +15점
- 추가 연계: +5점

---

> ⚠ 손절(역지정) 없이는 절대 진입하지 마세요. 피보 1.0이 최후의 선.
