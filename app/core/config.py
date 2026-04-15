from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = '제이드 파동 스캐너 v4'
    app_version: str = '4.0.0-jade-unified'
    upbit_base_url: str = 'https://api.upbit.com'

    # RSI / EMA
    rsi_period: int = 14
    ema_fast: int = 20
    ema_slow: int = 50
    ema_regime: int = 200

    # 피봇 설정
    pivot_left: int = 3
    pivot_right: int = 3
    pivot_min_gap: int = 5
    pivot_max_gap: int = 80
    min_chain_span: int = 12

    # 캔들 수
    candles_limit_15m: int = 240
    candles_limit_1h: int = 300
    candles_limit_4h: int = 240

    # 스캔 범위
    scan_market_limit: int = 80
    top_pick_count: int = 8

    # 피보나치 zone 허용 오차 (%)
    fib_zone_tolerance_pct: float = 1.8
    # 늦은 진입 허용 버퍼 (%) - fib_0618 위에서도 이 이내면 watch
    late_entry_buffer_pct: float = 2.5

    # 손절 제한
    max_stop_pct: float = 8.0
    absolute_max_stop_pct: float = 12.0
    stop_buffer_pct: float = 0.0   # 피보 1 아래로 약간 여유를 줄 때 사용

    # 수익비
    min_rr_tp1: float = 1.2
    min_rr_tp2: float = 2.0

    # 볼륨
    min_volume_ratio: float = 0.95

    # 저항까지 최소 여유 (%)
    min_resistance_room_pct: float = 3.0

    # 과열 기준 (20봉 저점 대비 상승 %)
    overheated_pct_from_20_low: float = 35.0

    # 일 거래대금 필터 (원)
    min_daily_acc_trade_price_krw: float = 3_000_000_000

    # 제외 마켓 (쉼표 구분)
    exclude_markets: str = ''


settings = Settings()
