# src/infrastructure/config/settings.py

from pathlib import Path
from typing import Dict
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field as PydanticField, computed_field
from asynch import Connection 

class ClickHouseSettings(BaseSettings):
    host: str = "localhost"
    port_tcp: int = 9000
    port_http: int = 8123
    login: str = "default"
    user: str = "default"
    password: str = ""
    
    db_name: str 
    table_kline_archive: str = "kline_archive"
    table_kline_archive_datetime: str = "kline_archive_datetime"
    table_orderbook_snapshots: str = "orderbook_snapshots"
    table_orderbook_archive_filename: str = "orderbook_archive_filename"
    table_trade_signals: str = "trade_signals"
    table_trade_results: str = "trade_results"
    table_positions: str = "trade_positions"

    @computed_field
    def db_name_testnet(self) -> str:
        return self.db_name + "_testnet"
    
    @computed_field
    def testnet_tables(self) -> Dict[str, str]:
        table_fields = [
            'table_kline_archive',
            'table_kline_archive_datetime',
            'table_orderbook_snapshots',
            'table_orderbook_archive_filename',
            'table_trade_signals',
            'table_trade_results',
            'table_positions'
        ]
        return {field: getattr(self, field) + "_testnet" for field in table_fields}

    async def connect(self, use_db: bool=True) -> Connection:
        return Connection(
            host=self.host,
            port=self.port_tcp,
            user=self.login,
            password=self.password,
            database=self.db_name if use_db else "default" 
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CLICKHOUSE_",
        extra="ignore"
    )

class BybitSettings(BaseSettings):
    name: str = "bybit"
    api_key: str = ""
    api_secret: str = ""
    base_url: str = ""
    ws_url: str = ""
    kline_url: str = ""
    orderbook_url: str = ""

    test_api_key: str = ""
    test_api_secret: str = ""
    test_base_url: str = ""
    test_ws_url: str = ""
    test_kline_url: str = ""
    test_orderbook_url: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="BYBIT_",
        extra="ignore"
    )

class BinanceSettings(BaseSettings):
    name: str = "binance"
    api_key: str = ""
    api_secret: str = ""
    base_url: str = ""
    kline_url: str = ""

    test_api_key: str = ""
    test_api_secret: str = ""
    test_base_url: str = ""
    test_kline_url: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="BINANCE_",
        extra="ignore"
    )


CONFIG_DIR = Path(__file__).parent

class BacktestingSettings(BaseSettings):
    name: str = "backtesting"
    default_file_settings: str = "default_strategy_settings.json"
    best_file_settings: str = "best_strategy_settings.json"
    custom_file_settings: str = "custom_strategy_settings.json"
    cash: int = 100000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="BACKTESTING_",
        extra="ignore"
    )

    @computed_field
    @property
    def default_path(self) -> Path:
        return CONFIG_DIR / self.default_file_settings

    @computed_field
    @property
    def best_path(self) -> Path:
        return CONFIG_DIR / self.best_file_settings

    @computed_field
    @property
    def custom_path(self) -> Path:
        return CONFIG_DIR / self.custom_file_settings


class StreamSettings(BaseSettings):
    name: str = "stream"
    snapshots_count: int = 10
    snapshots_interval_sec: int = 60
    snapshots_orderbook_depth: int = 20
    duration: int = 120
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="STREAM_",
        extra="ignore"
    )

class KlineSettings(BaseSettings):
    
    interval: str = "1" # Интервал свечей указывается в минутах 1,3,5,15,30,60,120,240,360,720,D,W,M    
    count_candles: int = 5 # количество свечей в одном запросе для получения сигнала по стриму  
    snapshots_count: int = 200 
    timer_iteration: int = 15 # как часто опрашиваем для получения свечи

    # размер батчей для сохранения в базу данных 
    db_batch_size: int = 1 # Размер батча для сохранения свечей в БД
    db_batch_timer: float = 2.0  # Таймаут для батча свечей в БД

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="KLINE_",
        extra="ignore"
    )

class OrderbookSettings(BaseSettings):
    
    # размер батчей для сохранения в базу данных 
    db_batch_size: int = 10 # Размер батча для сохранения ордербука в БД
    db_batch_timer: float = 3.0  # Таймаут для батча ордербука в БД
    timer_iteration: float = 0.1 # как часто отбора ордербука

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ORDERBOOK_",
        extra="ignore"
    )

class Settings(BaseSettings):
    logger_level: str = PydanticField(default="INFO") 

    clickhouse: ClickHouseSettings = PydanticField(default_factory=lambda: ClickHouseSettings(db_name="default"))
    bybit: BybitSettings = PydanticField(default_factory=lambda: BybitSettings())
    binance: BinanceSettings = PydanticField(default_factory=lambda: BinanceSettings(
        api_key="",
        api_secret="",
        base_url="",
        kline_url="",
        test_api_key="",
        test_api_secret="",
        test_base_url="",
        test_kline_url=""
    ))
    backtesting: BacktestingSettings = PydanticField(
        default_factory=lambda: BacktestingSettings(
            default_file_settings="default_strategy_settings.json",
            best_file_settings="best_strategy_settings.json",
            custom_file_settings="custom_strategy_settings.json"
        )
    )
    streaming: StreamSettings = PydanticField(default_factory=lambda: StreamSettings())
    kline: KlineSettings = PydanticField(default_factory=lambda: KlineSettings())
    orderbook: OrderbookSettings = PydanticField(default_factory=lambda: OrderbookSettings())


    # Modes parameters
    testnet: bool

    # archive_modes 
    archive_mode: bool = False
    archive_source: bool = False
    stream_source: bool = False

    # stream_modes
    stream_mode: bool = False
    use_ws: bool = False # True - использовать вебсокеты, False - использовать API


    # kline_interval: str = "1" # minutes
    folder_report: str = "reports"
    datasets_dir: str = "datasets"
    pair_tokens: str = "ETHUSDT"
    start_time: str = "2025-06-20"
    end_time: str = "2025-06-21"

    # Параметры стратегии, включая для backtesting
    best_strategy_settings_path: str = "best_strategy_settings.json"
    custom_strategy_settings_path: str = "custom_strategy_settings.json"
    default_strategy_settings_path: str = "default_strategy_settings.json" 

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="SETTINGS_",
        extra="ignore"
    )


settings = Settings(testnet=False)
