from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'Kai Upbit Final Dynamic Scanner'
    app_version: str = '3.0.0-final-dynamic'
    upbit_base_url: str = 'https://api.upbit.com'

    rsi_period: int = 14
    ema_fast: int = 20
    ema_slow: int = 50
    ema_regime: int = 200

    pivot_left: int = 3
    pivot_right: int = 3
    pivot_min_gap: int = 5
    pivot_max_gap: int = 80
    min_chain_span: int = 12

    candles_limit_15m: int = 220
    candles_limit_1h: int = 280
    candles_limit_4h: int = 220

    scan_market_limit: int = 80
    top_pick_count: int = 8

    fib_zone_tolerance_pct: float = 1.8
    late_entry_buffer_pct: float = 2.2
    min_volume_ratio: float = 0.95
    min_daily_acc_trade_price_krw: float = 3_000_000_000
    max_stop_pct: float = 8.0
    absolute_max_stop_pct: float = 10.0
    min_rr_tp1: float = 1.2
    min_rr_tp2: float = 2.2
    min_resistance_room_pct: float = 3.0
    overheated_pct_from_20_low: float = 35.0
    stop_buffer_pct: float = 0.0
    exclude_markets: str = ''


settings = Settings()
