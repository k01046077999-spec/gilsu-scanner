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


settings = Settings()
