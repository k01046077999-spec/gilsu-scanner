from __future__ import annotations

import os

APP_VERSION = "2.0.0-final"
DEFAULT_MARKET = os.getenv("DEFAULT_MARKET", "ALL").upper()
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "120"))
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "30"))

# 농사매매 기본 필터
MA_LONG = int(os.getenv("MA_LONG", "224"))
MA_MID = int(os.getenv("MA_MID", "112"))
MIN_TRADING_VALUE = int(os.getenv("MIN_TRADING_VALUE", "1500000000"))  # 20일 평균 거래대금 15억
MIN_PRICE = int(os.getenv("MIN_PRICE", "500"))
MAX_PRICE = int(os.getenv("MAX_PRICE", "300000"))

# Render Free 안정화용
OHLCV_DAYS = int(os.getenv("OHLCV_DAYS", "620"))
PYKRX_SLEEP_SEC = float(os.getenv("PYKRX_SLEEP_SEC", "0.03"))
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "900"))
PYTHONHASHSEED = os.getenv("PYTHONHASHSEED", "0")
