from datetime import datetime
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
import pytest
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock
from main import run_stream_application, initialisation_storage # Импорт основных функций
from application.services.event_publisher import EventPublisher
from application.stream_strategy_processor import StreamStrategyProcessor
from application.trade_processor import TradingProcessor
from application.services.data_pipeline_service import DataPipelineService
from domain.events.data_events import KlineDataReceivedWsEvent, OrderbookSnapshotReceivedWsEvent
from infrastructure.config.settings import settings
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord, OrderbookSnapshotModel, TradeSignal, TradeResult

# Фикстура для настройки ClickHouse для этого теста
@pytest.fixture(scope="module")
async def setup_clickhouse_for_full_pipeline():
    test_db_name = f"test_full_pipeline_db_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Save original settings
    original_db_name = settings.clickhouse.db_name
    original_kline_table = settings.clickhouse.table_kline_archive
    original_snapshots_table = settings.clickhouse.table_orderbook_snapshots
    original_trade_signals_table = settings.clickhouse.table_trade_signals
    original_trade_results_table = settings.clickhouse.table_trade_results

    # Set test settings
    settings.clickhouse.db_name = test_db_name
    settings.clickhouse.table_kline_archive = "kline_full_pipeline"
    settings.clickhouse.table_orderbook_snapshots = "orderbook_full_pipeline"
    settings.clickhouse.table_trade_signals = "trade_signals_full_pipeline"
    settings.clickhouse.table_trade_results = "trade_results_full_pipeline"

    client = await settings.clickhouse.connect()
    initializer = StorageInitializer(settings, MagicMock(), client)
    
    try:
        await initializer.create_database(test_db_name)
        await initializer.initialize([
            (KlineRecord, settings.clickhouse.table_kline_archive),
            (OrderbookSnapshotModel, settings.clickhouse.table_orderbook_snapshots),
            (TradeSignal, settings.clickhouse.table_trade_signals),
            (TradeResult, settings.clickhouse.table_trade_results),
        ])
        yield  # Run tests
    finally:
        await client.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        await client.close()
        # Restore original settings
        settings.clickhouse.db_name = original_db_name
        settings.clickhouse.table_kline_archive = original_kline_table
        settings.clickhouse.table_orderbook_snapshots = original_snapshots_table
        settings.clickhouse.table_trade_signals = original_trade_signals_table
        settings.clickhouse.table_trade_results = original_trade_results_table

# Мок для WebSocket соединения
@pytest.fixture
async def mock_ws_connection():
    ws = AsyncMock()
    ws.receive_json.side_effect = [
        {"op": "pong"},
        {"data": [{"k": {"s": "ETHUSDT", "t": 1720000000000, "o": "2000", "h": "2010", "l": "1990", "c": "2005", "v": "100"}}], "topic": "kline.1.ETHUSDT", "type": "snapshot"},
        {"data": {"s": "ETHUSDT", "b": [["2000.0", "50.0"]], "a": [["2001.0", "60.0"]]}, "topic": "orderbook.1.ETHUSDT", "type": "snapshot"},
        asyncio.CancelledError # Останавливаем поток после получения тестовых данных
    ]
    yield ws

@pytest.fixture
async def mock_aiohttp_session(mock_ws_connection):
    session = AsyncMock(spec=aiohttp.ClientSession)
    session.ws_connect.return_value.__aenter__.return_value = mock_ws_connection
    yield session

@pytest.mark.asyncio
async def test_full_stream_pipeline_integration(setup_clickhouse_for_full_pipeline, mock_aiohttp_session):
    # Мокируем зависимости, которые не хотим тестировать в этом интеграционном тесте,
    # но которые вызываются в run_stream_application
    with patch('application.stream_strategy_processor.StreamStrategyProcessor.process_data', AsyncMock()) as mock_process_strategy_data, \
         patch('application.trade_processor.TradingProcessor.process_trade', AsyncMock()) as mock_process_trade:

        # Настраиваем длительность потока на короткий срок, чтобы тест завершился
        original_duration = settings.streaming.duration
        settings.streaming.duration = 1 # Сек.
        
        try:
            # Запускаем основное приложение в режиме STREAM_MODE с WebSocket
            # initialisation_storage уже выполнена в setup_clickhouse_for_full_pipeline
            await run_stream_application(use_ws=True)
        finally:
            settings.streaming.duration = original_duration # Восстанавливаем настройки

        # Проверяем, что Kline и Orderbook данные были получены и обработаны
        # (Проверяем через мокирование ws_connection и event_publisher)
        mock_aiohttp_session.ws_connect.assert_called_once()
        
        # Проверяем, что StreamStrategyProcessor и TradingProcessor были вызваны
        # (Это указывает на то, что данные прошли через пайплайн)
        assert mock_process_strategy_data.called
        assert mock_process_trade.called

        # Проверяем, что данные были сохранены в ClickHouse
        kline_repo = ClickHouseRepository(KlineRecord, settings.clickhouse.db_name, settings.clickhouse.table_kline_archive)
        orderbook_repo = ClickHouseRepository(OrderbookSnapshotModel, settings.clickhouse.db_name, settings.clickhouse.table_orderbook_snapshots)
        trade_signal_repo = ClickHouseRepository(TradeSignal, settings.clickhouse.db_name, settings.clickhouse.table_trade_signals)
        trade_result_repo = ClickHouseRepository(TradeResult, settings.clickhouse.db_name, settings.clickhouse.table_trade_results)

        # Обратите внимание: DataPipelineService сохраняет данные в БД.
        # Мокируем сохранение на уровне репозитория, если не хотим зависеть от ClickHouse здесь.
        # Для *интеграционного* теста мы хотим проверить реальное сохранение.
        # Поэтому, проверяем, что данные *есть* в БД после запуска.

        # DataPipelineService сохраняет Klines и Orderbooks
        saved_klines = await kline_repo.get_all()
        saved_orderbooks = await orderbook_repo.get_all()
        
        assert len(saved_klines) >= 1
        assert len(saved_orderbooks) >= 1
        
        # Если стратегии генерируют сигналы и результаты, их также можно проверить:
        # saved_signals = await trade_signal_repo.get_all()
        # saved_results = await trade_result_repo.get_all()
        # assert len(saved_signals) >= 1 # Если ожидается генерация сигналов
        # assert len(saved_results) >= 1 # Если ожидается совершение сделок