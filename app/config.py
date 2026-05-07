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
    max_symbols_per_scan: int = 50
    universe_size: int = 100
    prefilter_size: int = 50
    scan_concurrency: int = 2
    request_timeout: float = 20.0
    top_pick_count: int = 3

    full_analysis_main_limit: int = 12
    full_analysis_sub_limit: int = 16
    quick_score_main_floor: float = 22.0
    quick_score_sub_floor: float = 16.0
    main_threshold: float = 50.0
    sub_threshold: float = 28.0


settings = Settings()
