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
    default_limit: int = 220
    rsi_period: int = 14
    swing_window: int = 3
    max_symbols_per_scan: int = 12
    scan_concurrency: int = 3
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    pool_timeout: float = 5.0
    api_retries: int = 3
    retry_backoff_seconds: float = 0.8
    cache_ttl_main_seconds: int = 90
    cache_ttl_sub_seconds: int = 120


settings = Settings()
