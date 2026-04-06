
from pydantic import BaseModel


class Settings(BaseModel):
    default_symbols: list[str] = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "BNBUSDT",
        "DOGEUSDT",
        "ADAUSDT",
        "AVAXUSDT",
        "LINKUSDT",
        "SUIUSDT",
        "WIFUSDT",
        "PEPEUSDT",
    ]
    default_limit: int = 250
    rsi_period: int = 14
    swing_window: int = 3
    max_symbols_per_scan: int = 20
    scan_concurrency: int = 3
    request_timeout_seconds: float = 12.0
    request_retries: int = 3
    cache_ttl_seconds: int = 120

    # Practical trade filters
    main_min_rr_tp2: float = 1.2
    sub_min_rr_tp2: float = 0.9
    main_min_tp1_pct: float = 1.0
    sub_min_tp1_pct: float = 1.0
    allow_short_signals: bool = True


settings = Settings()
