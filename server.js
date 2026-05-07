const express = require('express');
const axios = require('axios');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// CORS 허용
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

// 정적 파일 (스캐너 HTML)
app.use(express.static(path.join(__dirname, 'static')));

// ── 업비트 프록시 라우트 ──
const UPBIT = 'https://api.upbit.com/v1';

// 마켓 목록
app.get('/api/markets', async (req, res) => {
  try {
    const r = await axios.get(`${UPBIT}/market/all?isDetails=false`, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    const krw = r.data.filter(m => m.market.startsWith('KRW-'));
    res.json(krw);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 캔들 (분봉/일봉/주봉)
app.get('/api/candles/*', async (req, res) => {
  try {
    const tf = req.params[0]; // e.g. minutes/60, days, weeks
    const { market, count = 110 } = req.query;
    const url = `${UPBIT}/candles/${tf}?market=${market}&count=${count}`;
    const r = await axios.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    res.json(r.data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 티커 (여러 종목 한번에)
app.get('/api/ticker', async (req, res) => {
  try {
    const { markets } = req.query;
    const r = await axios.get(`${UPBIT}/ticker?markets=${markets}`, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    res.json(r.data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 루트 → index.html
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'static', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`JADE Scanner 서버 실행중: http://localhost:${PORT}`);
});
