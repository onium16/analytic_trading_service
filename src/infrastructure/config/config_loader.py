# src/infrastructure/config/config_loader.py

from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__, level=settings.logger_level)

def apply_environment_settings(current_settings, is_test_net: bool):
    """
    Модифицирует settings в зависимости от режима is_test_net (True = testnet).
    """
    if is_test_net:
        # Модифицируем настройки ClickHouse
        current_settings.clickhouse.db_name += "_testnet"
        current_settings.clickhouse.table_kline_archive = current_settings.clickhouse.table_kline_archive_testnet
        current_settings.clickhouse.table_kline_archive_datetime = current_settings.clickhouse.table_kline_archive_datetime_testnet
        current_settings.clickhouse.table_orderbook_snapshots = current_settings.clickhouse.table_orderbook_snapshots_testnet
        current_settings.clickhouse.table_orderbook_archive_filename = current_settings.clickhouse.table_orderbook_archive_filename_testnet
        current_settings.clickhouse.table_trade_results = current_settings.clickhouse.table_trade_results_testnet
        current_settings.clickhouse.table_positions = current_settings.clickhouse.table_positions_testnet
        
        # --- БЛОК ДЛЯ BINANCE ---
        current_settings.binance.api_key = current_settings.binance.test_api_key
        current_settings.binance.api_secret = current_settings.binance.test_api_secret
        current_settings.binance.base_url = current_settings.binance.test_base_url
        current_settings.binance.kline_url = current_settings.binance.test_kline_url

        # --- БЛОК ДЛЯ BYBIT ---
        current_settings.bybit.api_key = current_settings.bybit.test_api_key
        current_settings.bybit.api_secret = current_settings.bybit.test_api_secret
        current_settings.bybit.base_url = current_settings.bybit.test_base_url
        current_settings.bybit.kline_url = current_settings.bybit.test_kline_url
        current_settings.bybit.orderbook_url = current_settings.bybit.test_orderbook_url
        current_settings.bybit.ws_url = current_settings.bybit.test_ws_url

    logger.debug(f"Applied settings for test net: {is_test_net}")
    logger.debug(f"Binance KLine URL after apply_environment_settings: {current_settings.binance.kline_url}")
