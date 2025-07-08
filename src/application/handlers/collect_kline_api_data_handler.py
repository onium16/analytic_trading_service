import asyncio
import aiohttp
from typing import Any, Dict, Optional

from application.commands.collect_kline_api_data import CollectKlineApiDataCommand
from application.contracts import Handler
from domain.utilits.interval import _validate_interval
from infrastructure.adapters.bybit_api_client import BybitClient # Изменен импорт
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger
from application.services.event_publisher import EventPublisher # Импорт EventPublisher
from domain.events.data_events import KlineDataReceivedEvent # Импорт события

logger = setup_logger(__name__)


def _interval_to_number(interval: str) -> str: # Возвращаем str, как в вашей схеме KlineRecord
    """Converts interval like '1' to '1' or 'D' to 'D'."""
    # Bybit API expects "1", "60", "D" etc., which are strings.
    # So, we return string representation.
    return str(interval)


def _get_interval_duration_seconds(interval: str) -> int:
    """Возвращает длительность интервала в секундах."""
    interval_upper = interval.upper()
    if interval_upper in {"D", "W", "M"}:
        if interval_upper == "D":
            return 24 * 60 * 60
        elif interval_upper == "W":
            return 7 * 24 * 60 * 60
        elif interval_upper == "M":
            return 30 * 24 * 60 * 60 # Приближенно
    try:
        return int(interval) * 60 # For numerical intervals like "1", "5", "60" etc.
    except ValueError:
        logger.warning(f"Неизвестный интервал: {interval}. Используем 60 секунд по умолчанию.")
        return 60


class CollectKlineApiDataHandler(Handler): # Убрали Handler, если это не базовый класс
    def __init__(self, session: aiohttp.ClientSession, event_publisher: EventPublisher):
        self.session = session
        self.event_publisher = event_publisher
        # BybitClient будет создан с этой же сессией
        self.bybit_client = BybitClient(session) 

    async def handle(self, command: CollectKlineApiDataCommand):
        last_timestamp = None

        try:
            params_kline = {
                "symbol": command.symbol,
                "kline_interval": _validate_interval(command.interval),
            }
            # Длительность свечи в миллисекундах (для проверки закрытия)
            interval_duration_ms = _get_interval_duration_seconds(command.interval) * 1000 

            logger.info(f"Начало сбора свечей для {command.symbol} (интервал: {command.interval}, "
                        f"длительность сбора: {command.duration}с, частота запросов: {command.interval_iteration}с)")
            
            async for snapshot_data in self.bybit_client.get_multiple_snapshots_universal(
                func=self.bybit_client.get_kline_snapshot,
                duration=command.duration,
                interval_iteration=command.interval_iteration,
                **params_kline
            ):
                result_list = snapshot_data.get("result", {}).get("list", [])

                if not result_list:
                    logger.debug("Нет новых данных в ответе API для свечей.")
                    continue

                # Bybit возвращает свечи в обратном порядке (самые новые первыми)
                # Берем самую старую (первую в отсортированном по времени списке) свечу из двух полученных,
                # чтобы убедиться, что она закрыта.
                # Свеча [0] - самая новая (текущая незавершенная), [1] - предыдущая (завершенная).
                # Мы интересуемся ЗАВЕРШЕННЫМИ свечами.
                
                # result_list[0] - последняя свеча (скорее всего, незавершенная)
                # result_list[1] - предпоследняя свеча (скорее всего, завершенная)
                
                # Проходим по списку, чтобы убедиться, что берем завершенную свечу
                processed_kline_data: Optional[Dict[str, Any]] = None
                
                # Bybit API docs usually say list[0] is most recent, list[1] is previous, etc.
                # If you request limit=2, list[1] is the one that just closed (or closed an interval ago).
                if len(result_list) >= 2:
                    kline = result_list[1] # Consider the second-to-last kline as the "closed" one

                    current_ts = int(kline[0]) # Timestamp of the kline close/open (depends on API)
                    
                    # Проверка на дубликаты и "незакрытость"
                    # Если Bybit возвращает свечу с timestamp 'T' и интервалом '1m',
                    # то она "закрылась" в T + 1m - 1ms.
                    # Для простоты, мы будем считать, что kline[1] - это уже закрытая свеча
                    # (если она не дубликат).
                    if last_timestamp is not None and current_ts <= last_timestamp:
                        logger.debug(f"Пропущена дублирующая свеча: timestamp={current_ts}")
                        continue
                    
                    # Парсинг данных
                    parsed_kline = {
                        "timestamp": current_ts,
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "interval": _interval_to_number(command.interval),
                        "symbol": command.symbol
                    }
                    processed_kline_data = parsed_kline
                    last_timestamp = current_ts
                    logger.info(f"[KLINE] Свеча для публикации: {parsed_kline['symbol']}@{parsed_kline['timestamp']}")

                if processed_kline_data:
                    event = KlineDataReceivedEvent(kline_data=processed_kline_data)
                    await self.event_publisher.publish(event)
                else:
                    logger.debug(f"Не удалось получить готовую свечу для {command.symbol}.")

        except asyncio.TimeoutError:
            logger.info("Время выполнения сбора свечей истекло (asyncio.timeout).")
        except Exception as e:
            logger.error(f"Ошибка при получении/обработке данных свечей: {e}", exc_info=True)

        logger.info(f"Завершён цикл сбора свечей для {command.symbol}.")

    # close метод больше не нужен, сессия управляется в main