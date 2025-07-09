# tests/application/services/test_data_pipeline_service.py
import asyncio
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from collections import deque
from datetime import datetime

from application.services.data_pipeline_service import DataPipelineService
from domain.events.data_events import (
    KlineDataReceivedEvent,
    OrderbookSnapshotReceivedEvent,
    KlineDataReceivedWsEvent,
    OrderbookSnapshotReceivedWsEvent,
)
from infrastructure.storage.schemas import KlineRecord, OrderbookSnapshotModel
from infrastructure.config.settings import settings

# Подготавливаем фикстуры и моки для тестов
@pytest.fixture
def mock_trade_processor():
    return AsyncMock()

@pytest.fixture
def mock_stream_strategy_processor():
    return AsyncMock()

@pytest.fixture
def mock_clickhouse_repository():
    repo = AsyncMock()
    repo.ensure_table = AsyncMock()
    repo.update = AsyncMock()
    return repo

@pytest.fixture
def mock_delta_analyzer():
    analyzer = MagicMock()
    analyzer.archive_analyze = MagicMock(return_value=pd.DataFrame([{
        's': 'BTCUSDT', 'ts': 1697059200000, 'u': 1, 'seq': 1, 'cts': 1697059200000,
        'uuid': 123, 'num_bids': 10, 'num_asks': 10
    }]))
    return analyzer

@pytest.fixture
def mock_prepare_backtest_data():
    with patch('application.services.data_pipeline_service.prepare_backtest_data') as mock:
        mock.return_value = pd.DataFrame([{
            'timestamp': pd.to_datetime(1697059200000, unit='ms'),
            'symbol': 'BTCUSDT',
            'close': 28000.0
        }])
        yield mock

@pytest.fixture
def data_pipeline_service(mock_trade_processor, mock_stream_strategy_processor, mock_clickhouse_repository, mock_delta_analyzer):
    # Патчим ClickHouseRepository, чтобы возвращать замоканный репозиторий
    with patch('application.services.data_pipeline_service.ClickHouseRepository', return_value=mock_clickhouse_repository):
        # Патчим DeltaAnalyzerArchive
        with patch('application.services.data_pipeline_service.DeltaAnalyzerArchive', return_value=mock_delta_analyzer):
            service = DataPipelineService(
                trade_processor=mock_trade_processor,
                stream_strategy_processor=mock_stream_strategy_processor
            )
            # Устанавливаем тестовые значения для настроек
            settings.kline.db_batch_size = 2
            settings.kline.db_batch_timer = 5
            settings.orderbook.db_batch_size = 2
            settings.orderbook.db_batch_timer = 5
            settings.kline.count_candles = 2
            return service

@pytest.mark.asyncio
async def test_initialize(data_pipeline_service, mock_clickhouse_repository):
    """Тестируем метод initialize."""
    await data_pipeline_service.initialize()
    
    # Проверяем, что таблицы созданы
    assert mock_clickhouse_repository.ensure_table.call_count == 2
    # Проверяем, что фоновые задачи созданы
    assert data_pipeline_service.kline_save_task is not None
    assert data_pipeline_service.orderbook_save_task is not None

@pytest.mark.asyncio
async def test_handle_kline_data(data_pipeline_service):
    """Тестируем обработку события KlineDataReceivedEvent."""
    kline_data = {
        'symbol': 'BTCUSDT',
        'timestamp': 1697059200000,
        'interval': '1m',
        'open': 27000.0,
        'close': 28000.0,
        'high': 28500.0,
        'low': 26500.0,
        'volume': 100.0
    }
    event = KlineDataReceivedEvent(kline_data=kline_data)
    
    await data_pipeline_service.handle_kline_data(event)
    
    # Проверяем, что данные добавлены в буфер
    assert 'BTCUSDT' in data_pipeline_service.kline_buffer
    assert len(data_pipeline_service.kline_buffer['BTCUSDT']) == 1
    assert data_pipeline_service.kline_buffer['BTCUSDT'][0] == kline_data
    
    # Проверяем, что данные добавлены в батч для БД
    assert 'BTCUSDT' in data_pipeline_service.kline_current_batch_db
    assert len(data_pipeline_service.kline_current_batch_db['BTCUSDT']) == 1

@pytest.mark.asyncio
async def test_handle_orderbook_snapshot(data_pipeline_service):
    """Тестируем обработку события OrderbookSnapshotReceivedEvent."""
    orderbook_data = {
        'symbol': 'BTCUSDT',
        'ts': 1697059200000,
        'bids': [[27000.0, 1.0]],
        'asks': [[28000.0, 1.0]]
    }
    event = OrderbookSnapshotReceivedEvent(orderbook_data=orderbook_data)
    
    await data_pipeline_service.handle_orderbook_snapshot(event)
    
    # Проверяем, что данные добавлены в батч ордербуков
    assert 'BTCUSDT' in data_pipeline_service.orderbook_current_batch
    assert len(data_pipeline_service.orderbook_current_batch['BTCUSDT']) == 1
    assert data_pipeline_service.orderbook_current_batch['BTCUSDT'][0] == orderbook_data


@pytest.mark.asyncio
async def test_process_combined_data(data_pipeline_service, mock_prepare_backtest_data):
    """Тестируем обработку комбинированных данных."""
    symbol = 'BTCUSDT'
    kline_data = {
        'symbol': symbol,
        'timestamp': 1697059200000,
        'interval': '1m',
        'open': 27000.0,
        'close': 28000.0,
        'high': 28500.0,
        'low': 26500.0,
        'volume': 100.0
    }
    orderbook_data = {
        's': symbol,
        'ts': 1697059200000,
        'bids': [[27000.0, 1.0]],
        'asks': [[28000.0, 1.0]]
    }
    
    # Наполняем буферы
    data_pipeline_service.kline_buffer[symbol] = [kline_data, kline_data]
    data_pipeline_service.orderbook_buffer_for_strategy[symbol] = [orderbook_data]
    
    # Мокаем StrategyManager.run_all
    with patch.object(data_pipeline_service.strategy_manager, 'run_all', new=AsyncMock(return_value={'signal': 'buy'})) as mock_run_all:
        await data_pipeline_service._process_combined_data(symbol)
        
        # Проверяем, что prepare_backtest_data вызван
        mock_prepare_backtest_data.assert_called_once()
        
        # Проверяем, что стратегия вызвана
        mock_run_all.assert_called_once()
        
        # Проверяем, что торговый процессор получил сигнал
        data_pipeline_service.trade_processor.process_signal.assert_called_once_with({'signal': 'buy'})
        
        # Проверяем, что буферы очищены
        assert len(data_pipeline_service.kline_buffer[symbol]) == 0
        assert len(data_pipeline_service.orderbook_buffer_for_strategy[symbol]) == 0
