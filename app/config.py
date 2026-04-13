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
    default_limit: int = 240
    prefilter_limit: int = 160
    rsi_period: int = 14
    swing_window: int = 2
    max_symbols_per_scan: int = 45
    universe_size: int = 90
    prefilter_size: int = 45
    scan_concurrency: int = 2
    request_timeout: float = 20.0
    top_pick_count: int = 3
    main_threshold: float = 52.0
    sub_threshold: float = 30.0


settings = Settings()
