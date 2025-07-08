# src/infrastructure/adapters/bybit_client.py

import asyncio
import aiohttp
from typing import AsyncGenerator, Awaitable, Callable, Dict, Any, Optional

from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__)

class BybitClient:
    """Клиент Bybit для взаимодействия с REST API Bybit."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        """
        Инициализирует клиент Bybit, используя переданную сессию aiohttp.
        """
        self._session = session 

    async def get_orderbook_snapshot(self, symbol: str, limit: int = 200) -> dict:
        url = settings.bybit.orderbook_url
        params: Dict[str, str] = {
            "category": "linear",  
            "symbol": str(symbol),
            "limit": str(limit)
        }

        async with self._session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            else:
                text = await resp.text()
                raise Exception(f"Error fetching orderbook snapshot: {resp.status} {text}")
            
    async def get_kline_snapshot(self, symbol: str, kline_interval: str = "1") -> dict: # kline_interval as str
        """
        Метод для получения среза свечи.
        """
        url = settings.bybit.kline_url
        params: Dict[str, str] = {
            "category": "linear",
            "symbol": str(symbol),
            "interval": str(kline_interval), # kline_interval as str
            "limit": str(2)  # Запрашиваем 2 свечи, чтобы получить закрытую свечу
        }

        async with self._session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            else:
                text = await resp.text()
                raise Exception(f"Error fetching kline snapshot: {resp.status} {text}")

    async def get_multiple_snapshots_universal(
        self,
        func: Callable[..., Awaitable[Dict[str, Any]]],
        *,
        duration: int,
        interval_iteration: float = 1.0,
        **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Универсальный асинхронный генератор: вызывает переданную функцию `func`
        с интервалом времени `interval_iteration` секунд в течение `duration` секунд.
        Завершает выполнение строго через `duration` секунд.
        """
        start_time = asyncio.get_event_loop().time()
        end_time = start_time + duration
        logger.info(f"Начало цикла: start_time={start_time:.2f}, end_time={end_time:.2f}, duration={duration}, interval_iteration={interval_iteration}")

        iteration = 0
        while True:
            now = asyncio.get_event_loop().time()
            logger.debug(f"Итерация {iteration}: текущее время={now:.2f}, end_time={end_time:.2f}")

            if now >= end_time:
                logger.info(f"Цикл завершён: now={now:.2f} >= end_time={end_time:.2f}")
                break

            try:
                snapshot = await func(**kwargs)
                yield snapshot
            except Exception as e:
                logger.error(f"Ошибка при получении данных в get_multiple_snapshots_universal: {e}", exc_info=True)
                # Возможно, стоит пропустить текущую итерацию и продолжить
                
            iteration += 1

            # Проверяем время перед сном
            now = asyncio.get_event_loop().time()
            if now >= end_time:
                logger.info(f"Цикл завершён перед сном: now={now:.2f} >= end_time={end_time:.2f}")
                break

            next_call = start_time + iteration * interval_iteration
            sleep_time = max(0, next_call - now)
            logger.debug(f"Итерация {iteration}: sleep_time={sleep_time:.2f}")
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
