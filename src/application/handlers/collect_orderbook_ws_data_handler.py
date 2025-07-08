import asyncio
import time
import json
from typing import AsyncGenerator, Dict, List

from application.contracts import HandlerWs
from infrastructure.adapters.bybit_ws_client import BybitWebSocketClient
from application.commands.collect_orderbook_ws_data import CollectOrderbookWsDataCommand
from application.services.event_publisher import EventPublisher
from domain.events.data_events import OrderbookSnapshotReceivedWsEvent
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__, level=settings.logger_level)


class CollectOrderbookWsDataHandler(HandlerWs):
    def __init__(self, ws_url: str, event_publisher: EventPublisher):
        self.ws_url = ws_url
        self.event_publisher = event_publisher

    async def handle(self, command: CollectOrderbookWsDataCommand):
        logger.info(f"[WS][ORDERBOOK] Старт сбора данных по {command.symbol}")

        try:
            async for snapshot in self.listen(command):
                if snapshot["b"] and snapshot["a"]:
                    event = OrderbookSnapshotReceivedWsEvent(orderbook_data=snapshot)
                    await self.event_publisher.publish(event)
                    logger.info(f"[WS][ORDERBOOK] Снимок опубликован: {snapshot['s']} @ {snapshot['ts']}")
                else:
                    logger.warning(f"[WS][ORDERBOOK] Пустой снимок по {command.symbol} — пропущен.")
        except asyncio.CancelledError:
            logger.info(f"[WS][ORDERBOOK] Обработка отменена: {command.symbol}")
        except Exception as e:
            logger.error(f"[WS][ORDERBOOK] Ошибка обработки: {e}", exc_info=True)

    async def listen(self, command: CollectOrderbookWsDataCommand) -> AsyncGenerator[dict, None]:
        async with BybitWebSocketClient(self.ws_url) as client:
            await client.subscribe_to_orderbook(command.symbol, command.depth)

            start_time = time.time()
            while True:
                if command.duration and (time.time() - start_time) > command.duration:
                    logger.info(f"[WS][ORDERBOOK] Время сбора истекло ({command.duration}s)")
                    break

                try:
                    msg = await asyncio.wait_for(client._ws.recv(), timeout=command.duration)
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8")

                    snapshot = self._parse_orderbook_message(msg)
                    if snapshot:
                        yield snapshot
                except asyncio.TimeoutError:
                    logger.info(f"[WS][ORDERBOOK] Таймаут ожидания сообщения.")
                    return
                except Exception as e:
                    logger.error(f"[WS][ORDERBOOK] Ошибка при получении данных: {e}", exc_info=True)

    def _parse_orderbook_message(self, msg: str) -> dict | None:
        try:
            data = json.loads(msg)
            if "data" not in data:
                logger.debug(f"[WS][ORDERBOOK] Пропущено сообщение без 'data': {msg}")
                return None

            d = data["data"]
            result = {
                "s": d.get("s"),
                "topic": data.get("topic", ""),
                "b": d.get("b", []),
                "a": d.get("a", []),
                "ts": data.get("ts"),       # WS-level timestamp
                "u": d.get("u"),
                "seq": d.get("seq"),
                "cts": data.get("cts"),
                "type": "snapshot_stream",
                "uuid": 0  # может быть заменён в pipeline или оставлен как есть
            }
            return result

        except json.JSONDecodeError as e:
            logger.error(f"[WS][ORDERBOOK] Ошибка JSON: {e}")
        except Exception as e:
            logger.error(f"[WS][ORDERBOOK] Ошибка разбора сообщения: {e}", exc_info=True)

        return None