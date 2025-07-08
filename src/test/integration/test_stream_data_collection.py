import pytest
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock
from application.services.kline_data_collector import KlineDataCollector
from application.services.orderbook_data_collector import OrderbookDataCollector
from application.commands.collect_kline_api_data import CollectKlineApiDataCommand
from application.commands.collect_orderbook_api_data import CollectOrderbookApiDataCommand
from application.commands.collect_kline_ws_data import CollectKlineWsDataCommand
from application.commands.collect_orderbook_ws_data import CollectOrderbookWsDataCommand
from application.handlers.collect_kline_api_data_handler import CollectKlineApiDataHandler
from application.handlers.collect_kline_ws_data_handler import CollectKlineWsDataHandler
from application.handlers.collect_orderbook_api_data_handler import CollectOrderbookApiDataHandler
from application.handlers.collect_orderbook_ws_data_handler import CollectOrderbookWsDataHandler
from application.services.event_publisher import EventPublisher
from domain.events.data_events import KlineDataReceivedEvent, OrderbookSnapshotReceivedEvent, \
                                    KlineDataReceivedWsEvent, OrderbookSnapshotReceivedWsEvent
from infrastructure.config.settings import settings

@pytest.fixture
def mock_event_publisher():
    return AsyncMock(spec=EventPublisher)

@pytest.fixture
async def mock_aiohttp_session():
    # Мок aiohttp.ClientSession
    session = AsyncMock(spec=aiohttp.ClientSession)
    session.get.return_value.__aenter__.return_value.json.return_value = {
        "retCode": 0, "result": {"list": []} # Базовый ответ для API
    }
    # Мок websocket
    session.ws_connect.return_value.__aenter__.return_value = MagicMock()
    session.ws_connect.return_value.__aenter__.return_value.receive_json.side_effect = [
        {"op": "pong"}, # Ответ на пинг
        {"data": [{"k": {"s": "ETHUSDT", "t": 123, "o": "100", "h": "101", "l": "99", "c": "100.5", "v": "10"}}], "topic": "kline.1.ETHUSDT", "type": "snapshot"}, # Kline WS data
        asyncio.CancelledError # Останавливаем receive_json после первой порции данных
    ]
    return session

@pytest.mark.asyncio
async def test_kline_collection_api_flow(mock_aiohttp_session, mock_event_publisher):
    api_handler = CollectKlineApiDataHandler(mock_aiohttp_session, mock_event_publisher)
    collector = KlineDataCollector(api_handler=api_handler, ws_handler=None, use_ws=False)
    command = CollectKlineApiDataCommand(symbol="ETHUSDT", interval="1m", duration=1, interval_iteration=1)

    # Используем asyncio.wait с таймаутом, чтобы избежать бесконечного цикла,
    # так как коллектор в реальной жизни работает постоянно.
    task = asyncio.create_task(collector.collect(command))
    try:
        await asyncio.wait_for(task, timeout=2) # Даем немного времени на выполнение
    except asyncio.TimeoutError:
        pass # Ожидаем таймаут, чтобы остановить бесконечный цикл
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True) # Ждем отмены

    mock_aiohttp_session.get.assert_called_once() # Проверяем, что был вызов API
    # Проверяем, что событие было опубликовано
    mock_event_publisher.publish.assert_called_with(
        KlineDataReceivedEvent, # Убедитесь, что это правильный тип события
        # Здесь можно добавить проверку содержимого аргументов, если это критично для теста
        MagicMock() # Приблизительная проверка, что был передан какой-то объект данных
    )

@pytest.mark.asyncio
async def test_orderbook_collection_ws_flow(mock_aiohttp_session, mock_event_publisher):
    # Мокаем receive_json для ордербука
    mock_aiohttp_session.ws_connect.return_value.__aenter__.return_value.receive_json.side_effect = [
        {"op": "pong"},
        {"data": {"s": "ETHUSDT", "b": [["100.0", "10.0"]], "a": [["100.1", "5.0"]]}, "topic": "orderbook.1.ETHUSDT", "type": "snapshot"}, # Orderbook WS data
        asyncio.CancelledError
    ]

    ws_handler = CollectOrderbookWsDataHandler(settings.bybit.ws_url, mock_event_publisher)
    collector = OrderbookDataCollector(api_handler=None, ws_handler=ws_handler, use_ws=True)
    command = CollectOrderbookWsDataCommand(symbol="ETHUSDT", duration=1, depth=1)

    task = asyncio.create_task(collector.collect(command))
    try:
        await asyncio.wait_for(task, timeout=2)
    except asyncio.TimeoutError:
        pass
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    mock_aiohttp_session.ws_connect.assert_called_once() # Проверяем, что было установлено WS соединение
    mock_event_publisher.publish.assert_called_with(
        OrderbookSnapshotReceivedWsEvent, # Убедитесь, что это правильный тип события
        MagicMock() # Проверка аргументов
    )