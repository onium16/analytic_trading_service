import asyncio
import time
import pandas as pd
from typing import Optional

from application.commands.collect_kline_ws_data import CollectKlineWsDataCommand
from application.handlers.collect_kline_ws_data_handler import CollectKlineWsDataHandler
from application.commands.collect_orderbook_ws_data import CollectOrderbookWsDataCommand
from application.handlers.collect_orderbook_ws_data_handler import CollectOrderbookWsDataHandler

from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord, OrderbookSnapshotModel
from domain.delta_analyzer import DeltaAnalyzerArchive
from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__, level=settings.logger_level)


class StreamProcessorWs:
    def __init__(self):
        self.symbol = settings.pair_tokens
        self.queue_orderbook = asyncio.Queue()
        self.queue_kline = asyncio.Queue()
        self.stop_event = asyncio.Event()

        self.repo_orderbook = ClickHouseRepository(
            schema=OrderbookSnapshotModel,
            table_name=settings.clickhouse.table_orderbook_snapshots,
            db=settings.clickhouse.db_name
        )
        self.repo_kline = ClickHouseRepository(
            schema=KlineRecord,
            table_name=settings.clickhouse.table_kline_archive,
            db=settings.clickhouse.db_name
        )

        self.analyzer = DeltaAnalyzerArchive()
        self.last_row_orderbook = None

        # Таймаут работы, если None — бесконечно
        self.runtime_timer = settings.streaming.timer  # например, 300 сек или None



    async def stream_orderbook_data(self, ws_url: str, process_function):
        command = CollectOrderbookWsDataCommand(
            symbol=self.symbol,
            duration=self.runtime_timer,
            depth=settings.streaming.snapshots_orderbook_depth,
        )
        handler = CollectOrderbookWsDataHandler(ws_url)

        async for snapshot in handler.listen(command):
            if self.stop_event.is_set():
                break
            await process_function(snapshot)

    async def stream_kline_data(self, ws_url: str, process_function):
        command = CollectKlineWsDataCommand(
            symbol=self.symbol,
            duration=self.runtime_timer,
            interval=settings.kline_interval
        )
        handler = CollectKlineWsDataHandler(ws_url)

        async for kline in handler.listen(command):
            if self.stop_event.is_set():
                break
            await process_function(kline)

    async def produce_orderbook(self):
        ws_url = settings.bybit.ws_url

        async def process(snapshot):
            await self.queue_orderbook.put(snapshot)

        try:
            await self.stream_orderbook_data(ws_url, process)
        except Exception as e:
            logger.error(f"[ORDERBOOK] Ошибка в produce_orderbook: {e}")
        finally:
            await self.queue_orderbook.put(None)
            logger.info("[ORDERBOOK] Продюсер завершён")

    async def produce_kline(self):
        ws_url = settings.bybit.ws_url

        async def process(kline):
            await self.queue_kline.put(kline)

        try:
            await self.stream_kline_data(ws_url, process)
        except Exception as e:
            logger.error(f"[KLINE] Ошибка в produce_kline: {e}")
        finally:
            await self.queue_kline.put(None)
            logger.info("[KLINE] Продюсер завершён")

    async def consume_archive_orderbook(self, batch_size: int = 10, timer: float = 5.0):
        batch: list = []
        start = asyncio.get_running_loop().time()

        # Если runtime_timer задан, то фиксируем время окончания
        end_time = None
        if self.runtime_timer is not None:
            end_time = start + self.runtime_timer

        while not self.stop_event.is_set():
            # Если есть ограничение по времени и оно достигнуто — прерываем цикл
            if end_time is not None and asyncio.get_running_loop().time() >= end_time:
                logger.info("[ORDERBOOK] Таймаут работы достигнут — завершаем консьюмер.")
                break

            try:
                item = await asyncio.wait_for(self.queue_orderbook.get(), timeout=timer)
                if item is None:
                    if batch:
                        df = pd.DataFrame(batch)
                        analyzed = self.analyzer.archive_analyze(df)
                        await self.repo_orderbook.update(analyzed)
                        logger.info(f"[ORDERBOOK] Сохранено {len(batch)} записей (финальный батч)")
                    logger.info("[ORDERBOOK] Консьюмер завершён")
                    break

                batch.append(item)
                now = asyncio.get_running_loop().time()

                if len(batch) >= batch_size or (now - start) > timer:
                    df = pd.DataFrame(batch)
                    if self.last_row_orderbook is not None:
                        df = pd.concat([pd.DataFrame([self.last_row_orderbook]), df], ignore_index=True)

                    analyzed = self.analyzer.archive_analyze(df)
                    self.last_row_orderbook = df.iloc[-1].to_dict()

                    to_save = analyzed.iloc[1:] if len(analyzed) > 1 else pd.DataFrame()
                    if not to_save.empty:
                        await self.repo_orderbook.update(to_save)
                        logger.info(f"[ORDERBOOK] Сохранено {len(to_save)} записей")

                    batch.clear()
                    start = now

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[ORDERBOOK] Ошибка в consume_orderbook: {e}")
                break

        logger.info("[ORDERBOOK] Консьюмер выходит")

    async def consume_archive_kline(self):
        last_saved_time = None
        interval_seconds = StreamProcessorWs._convert_interval_to_seconds(settings.kline_interval)
        end_time = None
        if self.runtime_timer is not None:
            end_time = asyncio.get_running_loop().time() + self.runtime_timer

        while not self.stop_event.is_set():
            if end_time is not None and asyncio.get_running_loop().time() >= end_time:
                logger.info("[KLINE] Таймаут работы достигнут — завершаем консьюмер.")
                break

            try:
                item = await asyncio.wait_for(self.queue_kline.get(), timeout=5.0)
                
                if item is None:
                    logger.info("[KLINE] Получен сигнал завершения")
                    break

                current_time = item.get("timestamp")
                if not current_time:
                    logger.warning("[KLINE] Нет timestamp в данных свечи, пропуск")
                    continue

                # Переводим timestamp в секунды, если это миллисекунды
                current_time = current_time / 1000 if current_time > 1e10 else current_time

                # Сохраняем только если нет last_saved_time или прошло достаточно времени
                if last_saved_time is None or (current_time - last_saved_time) >= interval_seconds:
                    df = pd.DataFrame([item])
                    await self.repo_kline.update(df)
                    logger.info(f"[KLINE] Сохранена 1 свеча: {item}")
                    last_saved_time = current_time
                else:
                    logger.debug(f"[KLINE] Пропуск сохранения, слишком рано: {current_time - last_saved_time} сек. с последнего сохранения")


            except asyncio.TimeoutError:
                logger.debug("[KLINE] Таймаут ожидания данных свечи")
                continue
            except Exception as e:
                logger.error(f"[KLINE] Ошибка в consume_kline: {e}")
                break

        logger.info("[KLINE] Консьюмер завершён")

    @staticmethod
    def _convert_interval_to_seconds(interval: str) -> int:
        unit = interval[-1].lower()
        value = int(interval[:-1])
        if unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            raise ValueError(f"Unsupported interval unit: {unit}")

    async def run(self):
        await self.repo_orderbook.ensure_table()
        await self.repo_kline.ensure_table()

        produce_orderbook_task = asyncio.create_task(self.produce_orderbook())
        produce_kline_task = asyncio.create_task(self.produce_kline())
        consume_orderbook_task = asyncio.create_task(self.consume_orderbook())
        consume_kline_task = asyncio.create_task(self.consume_kline())

        tasks = [
            produce_orderbook_task,
            produce_kline_task,
            consume_orderbook_task,
            consume_kline_task
        ]

        try:
            if settings.streaming.timer is not None:
                # Если задан таймер — завершаем после его истечения
                await asyncio.wait_for(self.stop_event.wait(), timeout=settings.streaming.timer)
            else:
                # Если None — ждём бесконечно внешнего сигнала остановки
                await self.stop_event.wait()
        except asyncio.TimeoutError:
            logger.info(f"Таймаут {settings.streaming.timer} секунд истёк — останавливаем поток")
        except asyncio.CancelledError:
            logger.info("Задача run была отменена")
        except Exception as e:
            logger.error(f"Ошибка в run: {e}")
        finally:
            logger.info("Получен сигнал остановки. Отмена задач...")

            self.stop_event.set()  # Убедимся, что флаг установлен

            for task in tasks:
                task.cancel()

            # Ждём отмены, игнорируем исключения
            await asyncio.gather(*tasks, return_exceptions=True)

            logger.info("Завершение работы процессора.")



if __name__ == "__main__":
    processor = StreamProcessorWs()
    asyncio.run(processor.run())
