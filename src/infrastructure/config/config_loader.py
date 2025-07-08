# src/infrastructure/config/config_loader.py

def apply_environment_settings(settings, is_test_net: bool):
    """
    Модифицирует settings в зависимости от режима is_test_net (True = testnet).
    """
    if is_test_net:
        settings.clickhouse.db_name = settings.clickhouse.db_name_testnet
        # присваеваем тестовые таблицы к основным 
        for field, testnet_name in settings.clickhouse.testnet_tables.items():
            setattr(settings.clickhouse, field, testnet_name)

        settings.bybit.api_key = settings.bybit.test_api_key
        settings.bybit.api_secret = settings.bybit.test_api_secret
        settings.bybit.base_url = settings.bybit.test_base_url
        settings.bybit.kline_url = settings.bybit.test_kline_url
        settings.bybit.orderbook_url = settings.bybit.test_orderbook_url
        settings.bybit.ws_url = settings.bybit.test_ws_url

        settings.binance.api_key = settings.binance.test_api_key
        settings.binance.api_secret = settings.binance.test_api_secret
        settings.binance.base_url = settings.binance.test_base_url
        settings.binance.kline_url = settings.binance.test_kline_url
