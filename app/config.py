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


settings = Settings()
