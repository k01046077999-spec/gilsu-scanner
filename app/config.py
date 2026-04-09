from pydantic import BaseModel


class Settings(BaseModel):
    default_symbols: list[str] = [
        "KRW-BTC",
        "KRW-ETH",
        "KRW-XRP",
        "KRW-SOL",
        "KRW-DOGE",
        "KRW-ADA",
        "KRW-SUI",
        "KRW-LINK",
        "KRW-AVAX",
        "KRW-TRX",
        "KRW-HBAR",
        "KRW-XLM",
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
    top_pick_count: int = 3
    main_threshold: float = 62.0
    sub_threshold: float = 42.0


settings = Settings()
