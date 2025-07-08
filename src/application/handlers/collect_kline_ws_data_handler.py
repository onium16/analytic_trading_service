import asyncio
import json
from typing import Optional
from application.contracts import Handler
from application.services.event_publisher import EventPublisher
from domain.utilits.interval import _validate_interval
from infrastructure.adapters.bybit_ws_client import BybitWebSocketClient
from application.commands.collect_kline_ws_data import CollectKlineWsDataCommand
from domain.events.data_events import KlineDataReceivedWsEvent
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__)

class CollectKlineWsDataHandler(Handler):
    def __init__(self, ws_url: str, event_publisher: EventPublisher):
        self.ws_url = ws_url
        self.event_publisher = event_publisher

    async def handle(self, command: CollectKlineWsDataCommand):
        command.interval = _validate_interval(command.interval)
        seen_timestamps = set()
        start_time = asyncio.get_event_loop().time()

        async with BybitWebSocketClient(self.ws_url) as client:
            await client.subscribe_to_kline(symbol=command.symbol, interval=command.interval)

            while True:
                try:
                    msg = await asyncio.wait_for(client._ws.recv(), timeout=command.duration)
                except asyncio.TimeoutError:
                    logger.info(f"Timeout {command.duration}s reached, stopping listen")
                    break

                msg = json.loads(msg)
                logger.debug(f"RAW MSG: {msg}")

                if not msg.get("topic", "").startswith("kline") or "data" not in msg:
                    continue

                for kline in msg["data"]:
                    if not kline.get("confirm"):
                        # Только подтвержденные свечи
                        continue

                    ts = kline["start"]
                    if ts in seen_timestamps:
                        logger.debug(f"Duplicate kline with timestamp {ts} skipped")
                        continue

                    seen_timestamps.add(ts)

                    parsed_kline = {
                        "timestamp": ts,
                        "open": float(kline["open"]),
                        "high": float(kline["high"]),
                        "low": float(kline["low"]),
                        "close": float(kline["close"]),
                        "volume": float(kline["volume"]),
                        "interval": command.interval,
                        "symbol": command.symbol,
                    }

                    logger.info(f"[KLINE] Свеча для публикации: {parsed_kline['symbol']}@{parsed_kline['timestamp']}")

                    event = KlineDataReceivedWsEvent(kline_data=parsed_kline)
                    await self.event_publisher.publish(event)

                # Проверка длительности работы
                elapsed = asyncio.get_event_loop().time() - start_time
                if command.duration is not None and command.duration > 0 and elapsed >= command.duration:
                    logger.info(f"Duration {command.duration}s reached, stopping listen")
                    break
