import asyncio
import aiohttp
from application.commands.collect_orderbook_api_data import CollectOrderbookApiDataCommand
from application.contracts import Handler
from infrastructure.adapters.bybit_api_client import BybitClient # Изменен импорт
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger
from application.services.event_publisher import EventPublisher # Импорт EventPublisher
from domain.events.data_events import OrderbookSnapshotReceivedEvent # Импорт события

logger = setup_logger(__name__)

class CollectOrderbookApiDataHandler(Handler):
    def __init__(self, session: aiohttp.ClientSession, event_publisher: EventPublisher):
        self.session = session
        self.event_publisher = event_publisher
        # BybitClient будет создан с этой же сессией
        self.bybit_client = BybitClient(session)

    async def handle(self, command: CollectOrderbookApiDataCommand):
        logger.info(f"Начало сбора срезов ордербука для {command.symbol} (глубина: {command.limit}, "
                    f"длительность сбора: {command.duration}с, частота запросов: {command.interval_iteration}с)")

        try:
            params_orderbook = {
                "symbol": command.symbol,
                "limit": command.limit,
            }

            async for snapshot_data in self.bybit_client.get_multiple_snapshots_universal(
                func=self.bybit_client.get_orderbook_snapshot,
                duration=command.duration,
                interval_iteration=command.interval_iteration, # interval_iteration is in seconds now
                **params_orderbook
            ):
                result = snapshot_data.get("result", {})
                
                processed_orderbook = {
                    "s": result.get("s"),
                    "topic": f"orderbook.{command.limit}.{command.symbol}",
                    "b": result.get("b", []), # list of [price, qty] strings
                    "a": result.get("a", []), # list of [price, qty] strings
                    "ts": result.get("ts"), # Bybit v5 orderbook timestamp in milliseconds
                    "u": result.get("u"), 
                    "seq": result.get("seq"),
                    "cts": result.get("cts"), 
                    "type": "snapshot_stream", 
                    "uuid": 0 # UUID  generated later
                }


                if processed_orderbook["b"] and processed_orderbook["a"]: # Проверяем, что есть хоть какие-то данные
                    event = OrderbookSnapshotReceivedEvent(orderbook_data=processed_orderbook)
                    await self.event_publisher.publish(event)
                    logger.info(f"[ORDERBOOK] Снимок для публикации: {processed_orderbook['s']}@{processed_orderbook['ts']}")
                else:
                    logger.warning(f"Получен пустой снимок ордербука для {command.symbol}. Пропуск.")

        except asyncio.TimeoutError:
            logger.info("Время выполнения сбора ордербука истекло (asyncio.timeout).")
        except Exception as e:
            logger.error(f"Ошибка при получении/обработке данных ордербука: {e}", exc_info=True)

        logger.info(f"Завершён цикл сбора срезов ордербука для {command.symbol}.")
