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
    prefilter_limit: int = 160
    rsi_period: int = 14
    swing_window: int = 3
    max_symbols_per_scan: int = 40
    universe_size: int = 100
    prefilter_size: int = 30
    scan_concurrency: int = 6
    request_timeout: float = 15.0


settings = Settings()
