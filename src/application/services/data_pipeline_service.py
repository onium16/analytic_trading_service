# src/application/services/data_pipeline_service.py
import asyncio
import time
from domain.strategies.stream_signal import StrategyManager
import pandas as pd
from collections import deque
from typing import Any, Dict, List, Optional, Union

from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord, OrderbookSnapshotModel 

from domain.delta_analyzer import DeltaAnalyzerArchive 

from domain.events.data_events import (KlineDataReceivedEvent, 
                                       OrderbookSnapshotReceivedEvent, 
                                       KlineDataReceivedWsEvent,
                                       OrderbookSnapshotReceivedWsEvent 
                                       )
from application.stream_strategy_processor import StreamStrategyProcessor
from application.trade_processor import TradingProcessor
from application._backtest_data import prepare_backtest_data 

from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__, level=settings.logger_level)


class DataPipelineService:
    def __init__(self,
                 trade_processor: TradingProcessor,
                 stream_strategy_processor: StreamStrategyProcessor):
        logger.info("Инициализация DataPipelineService (REST API)")

        self.trade_processor = trade_processor
        self.stream_strategy_processor = stream_strategy_processor
        self.strategy_manager = StrategyManager()
        
        self.orderbook_buffer_for_strategy: Dict[str, List[Dict[str, Any]]] = {}

        self.repo_orderbook_init = ClickHouseRepository(
            schema=OrderbookSnapshotModel,
            table_name=settings.clickhouse.table_orderbook_snapshots,
            db=settings.clickhouse.db_name
        )
        self.repo_kline_init = ClickHouseRepository(
            schema=KlineRecord,
            table_name=settings.clickhouse.table_kline_archive,
            db=settings.clickhouse.db_name
        )
        
        # Анализатор для ордербуков
        self.analyzer = DeltaAnalyzerArchive()

        # Буферы для агрегации данных перед формированием DataFrame/батчей для стратегии
        self.kline_buffer: Dict[str, List[Dict[str, Any]]] = {}
        self.count_candles = settings.kline.count_candles # Пример: для 5-минутного DF из 1-минутных свечей

        # Для батчинга сырых ордербуков перед анализом и сохранением в БД
        self.orderbook_current_batch: Dict[str, List[Dict[str, Any]]] = {}
        self.orderbook_batch_size = settings.orderbook.db_batch_size  # Размер батча для ордербуков
        self.orderbook_batch_timer = settings.orderbook.db_batch_timer # Таймаут для батча ордербуков
        self.orderbook_batch_start_time: Dict[str, float] = {}

        # Добавлены поля для батчинга kline данных перед сохранением в БД
        self.kline_current_batch_db: Dict[str, List[Dict[str, Any]]] = {}
        self.kline_db_batch_size =  settings.kline.db_batch_size # Размер батча для сохранения свечей в БД
        self.kline_db_batch_timer = settings.kline.db_batch_timer # Таймаут для батча свечей в БД
        self.kline_db_batch_start_time: Dict[str, float] = {}


    async def initialize(self):
        """Метод для асинхронной инициализации репозиториев и запуска фоновых задач."""
        # Use the initial repo instances just to ensure tables exist
        await self.repo_orderbook_init.ensure_table()
        await self.repo_kline_init.ensure_table()
        logger.info("Таблицы ClickHouse проверены/созданы.")
        
        # Запускаем фоновые задачи для периодического сохранения батчей в БД
        self.kline_save_task = asyncio.create_task(self._kline_batch_saver_loop())
        self.orderbook_save_task = asyncio.create_task(self._orderbook_batch_saver_loop())
        logger.info("Фоновые задачи сохранения данных запущены.")


    async def _kline_batch_saver_loop(self):
        """Фоновый цикл для периодического сохранения батчей свечей в БД по таймеру."""
        while True:
            await asyncio.sleep(1) # Проверяем каждую секунду
            now = asyncio.get_running_loop().time() 
            symbols_to_process = list(self.kline_current_batch_db.keys()) 
            
            for symbol in symbols_to_process:
                if symbol in self.kline_current_batch_db and self.kline_current_batch_db[symbol]:
                    if len(self.kline_current_batch_db[symbol]) >= self.kline_db_batch_size or \
                       (now - self.kline_db_batch_start_time.get(symbol, now)) > self.kline_db_batch_timer:
                        await self._save_kline_batch_to_db(symbol)

    async def _save_kline_batch_to_db(self, symbol: str):
        """
        Сохраняет накопленный батч свечей в БД.
        Uses a NEW ClickHouseRepository instance for each call to avoid concurrency issues.
        """
        if not self.kline_current_batch_db.get(symbol):
            return

        batch_to_save = self.kline_current_batch_db[symbol]
        
        records_for_db = []
        for kline_data in batch_to_save:
            kline_for_db = kline_data.copy()
            kline_for_db['timestamp'] = pd.to_datetime(kline_for_db['timestamp'], unit='ms')
            
            try:
                kline_record = KlineRecord(**{k: v for k, v in kline_for_db.items() if k in KlineRecord.model_fields})
                records_for_db.append(kline_record.model_dump())
            except Exception as e:
                logger.error(f"[KLINE DB] Ошибка валидации KlineRecord для {symbol}: {e}. Данные: {kline_for_db}", exc_info=True)
        
        if records_for_db:
            temp_repo_kline = ClickHouseRepository(
                schema=KlineRecord,
                table_name=settings.clickhouse.table_kline_archive,
                db=settings.clickhouse.db_name
            )
            try:
                await temp_repo_kline.update(pd.DataFrame(records_for_db))
                logger.info(f"[KLINE DB] Сохранено {len(records_for_db)} свечей в БД для {symbol}.")
            except Exception as e:
                logger.error(f"[KLINE DB] Ошибка при сохранении батча свечей для {symbol}: {e}", exc_info=True)
            finally:
                self.kline_current_batch_db[symbol].clear()
                self.kline_db_batch_start_time[symbol] = asyncio.get_running_loop().time()


    async def _orderbook_batch_saver_loop(self):
        """Фоновый цикл для периодического сохранения батчей ордербуков в БД по таймеру."""
        while True:
            await asyncio.sleep(1) # Проверяем каждую секунду
            now = asyncio.get_running_loop().time() 
            symbols_to_process = list(self.orderbook_current_batch.keys()) 
            
            for symbol in symbols_to_process:
                if symbol in self.orderbook_current_batch and self.orderbook_current_batch[symbol]:
                    if len(self.orderbook_current_batch[symbol]) >= self.orderbook_batch_size or \
                       (now - self.orderbook_batch_start_time.get(symbol, now)) > self.orderbook_batch_timer:
                        await self._process_and_save_orderbook_batch(symbol)


    async def _process_and_save_orderbook_batch(self, symbol: str):
        """
        Обрабатывает и сохраняет накопленный батч ордербуков в БД.
        Добавляет анализированные данные в orderbook_buffer_for_strategy, но НЕ очищает этот буфер.
        Uses a NEW ClickHouseRepository instance for each call to avoid concurrency issues.
        """
        if not self.orderbook_current_batch.get(symbol):
            return

        batch_to_process = self.orderbook_current_batch[symbol]
        df = pd.DataFrame(batch_to_process)

        if 'ts' not in df.columns and 'timestamp' in df.columns:
            df['ts'] = df['timestamp']
        if 's' not in df.columns and 'symbol' in df.columns:
            df['s'] = df['symbol']
        
        if 's' not in df.columns or df['s'].isnull().all():
            logger.error(f"Не удалось определить символ ('s') в батче ордербуков для анализа. Пропускаем батч.")
            self.orderbook_current_batch[symbol].clear()
            self.orderbook_batch_start_time[symbol] = asyncio.get_running_loop().time()
            return

        analyzed_df = self.analyzer.archive_analyze(df)
        
        if not analyzed_df.empty:
            records_to_save_dicts = analyzed_df.to_dict(orient="records")
            
            valid_records = []
            for record_dict in records_to_save_dicts:
                try:
                    if 's' not in record_dict or record_dict['s'] is None:
                        logger.warning(f"Символ 's' отсутствует в записи ордербука для сохранения: {record_dict}. Пропускаем эту запись.")
                        continue
                    if 'ts' in record_dict and not isinstance(record_dict['ts'], int):
                        record_dict['ts'] = int(record_dict['ts'])
                    if 'u' in record_dict and not isinstance(record_dict['u'], int):
                        record_dict['u'] = int(record_dict['u'])
                    if 'seq' in record_dict and not isinstance(record_dict['seq'], int):
                        record_dict['seq'] = int(record_dict['seq'])
                    if 'cts' in record_dict and not isinstance(record_dict['cts'], int):
                        record_dict['cts'] = int(record_dict['cts'])
                    if 'uuid' in record_dict and not isinstance(record_dict['uuid'], int):
                        record_dict['uuid'] = int(record_dict['uuid'])
                    if 'num_bids' in record_dict and not isinstance(record_dict['num_bids'], int):
                        record_dict['num_bids'] = int(record_dict['num_bids'])
                    if 'num_asks' in record_dict and not isinstance(record_dict['num_asks'], int):
                        record_dict['num_asks'] = int(record_dict['num_asks'])

                    valid_records.append(OrderbookSnapshotModel(**record_dict).model_dump())
                except Exception as e:
                    logger.error(f"Ошибка валидации OrderbookSnapshotModel: {e}. Дан данные: {record_dict}", exc_info=True)
            
            if valid_records:

                temp_repo_orderbook = ClickHouseRepository(
                    schema=OrderbookSnapshotModel,
                    table_name=settings.clickhouse.table_orderbook_snapshots,
                    db=settings.clickhouse.db_name
                )
                try:
                    await temp_repo_orderbook.update(pd.DataFrame(valid_records))
                    logger.info(f"[ORDERBOOK DB] Сохранено {len(valid_records)} анализированных записей в БД для {symbol}.")
                except Exception as e:
                    logger.error(f"[ORDERBOOK DB] Ошибка при сохранении батча ордербуков для {symbol}: {e}", exc_info=True)
                finally:
                    # Добавляем анализированные данные в буфер для стратегии
                    if symbol not in self.orderbook_buffer_for_strategy:
                        self.orderbook_buffer_for_strategy[symbol] = []
                    self.orderbook_buffer_for_strategy[symbol].extend(analyzed_df.to_dict(orient="records"))
                    logger.debug(f"[ORDERBOOK] Добавлено {len(analyzed_df)} записей в orderbook_buffer_for_strategy для {symbol}. Текущий размер: {len(self.orderbook_buffer_for_strategy[symbol])}")

                    self.orderbook_current_batch[symbol].clear()
                    self.orderbook_batch_start_time[symbol] = asyncio.get_running_loop().time()
            else:
                logger.warning(f"[ORDERBOOK DB] Нет валидных записей для сохранения в БД для {symbol}.")
        else:
            logger.warning(f"[ORDERBOOK] analyze_df пуст для {symbol}. Нечего сохранять или передавать стратегии.")
            self.orderbook_current_batch[symbol].clear()
            self.orderbook_batch_start_time[symbol] = asyncio.get_running_loop().time()
            
    async def handle_kline_data(self, event: Union[KlineDataReceivedEvent | KlineDataReceivedWsEvent]):
        """Обрабатывает полученные данные свечи."""
        kline = event.kline_data
        symbol = kline['symbol']
        if symbol is None:
            logger.error(f"Получена свеча без символа: {kline}. Пропускаем.")
            return

        timestamp_ms = kline['timestamp'] 
        logger.debug(f"DataPipelineService: Получена свеча {symbol} {kline['interval']} @ {timestamp_ms}")

        if symbol not in self.kline_current_batch_db:
            self.kline_current_batch_db[symbol] = []
            self.kline_db_batch_start_time[symbol] = asyncio.get_running_loop().time()
        
        self.kline_current_batch_db[symbol].append(kline)

        if len(self.kline_current_batch_db[symbol]) >= self.kline_db_batch_size:
            await self._save_kline_batch_to_db(symbol)
        
        if symbol not in self.kline_buffer:
            self.kline_buffer[symbol] = []
        
        self.kline_buffer[symbol].append(kline) 

        logger.debug(f"[KLINE] Буфер свечей для стратегии ({symbol}): {len(self.kline_buffer[symbol])}/{self.count_candles}")
        
        if len(self.kline_buffer[symbol]) >= self.count_candles and \
           symbol in self.orderbook_buffer_for_strategy and self.orderbook_buffer_for_strategy[symbol]:
            
            logger.info(f"Достаточно свечей ({len(self.kline_buffer[symbol])}) и есть ордербуки для {symbol}. Вызов _process_combined_data.")
            await self._process_combined_data(symbol)
        else:
            logger.debug(f"Условия для _process_combined_data не выполнены для {symbol}. "
                         f"Kline buffer size: {len(self.kline_buffer[symbol])}/{self.count_candles}. "
                         f"Orderbook buffer empty: {not (symbol in self.orderbook_buffer_for_strategy and self.orderbook_buffer_for_strategy[symbol])}")

    async def handle_orderbook_snapshot(self, event: Union[OrderbookSnapshotReceivedEvent | OrderbookSnapshotReceivedWsEvent]):
        """Обрабатывает полученный снимок стакана."""
        orderbook = event.orderbook_data
        symbol = orderbook.get('symbol')
        if symbol is None: 
            symbol = orderbook.get('s')
        
        if symbol is None:
            logger.error(f"Не удалось определить символ для ордербука: {orderbook}. Пропускаем снимок.")
            return

        timestamp_ms = orderbook.get('ts')
        logger.debug(f"DataPipelineService: Получен снимок стакана для {symbol} @ {timestamp_ms}")

        if symbol not in self.orderbook_current_batch:
            self.orderbook_current_batch[symbol] = []
            self.orderbook_batch_start_time[symbol] = asyncio.get_running_loop().time()

        self.orderbook_current_batch[symbol].append(orderbook)
        
        now = asyncio.get_running_loop().time() 
        if len(self.orderbook_current_batch[symbol]) >= self.orderbook_batch_size: 
            await self._process_and_save_orderbook_batch(symbol)
        elif (now - self.orderbook_batch_start_time.get(symbol, now)) > self.orderbook_batch_timer:
            await self._process_and_save_orderbook_batch(symbol)
        else:
            logger.debug(f"Накопление ордербуков для {symbol}. Текущий размер: {len(self.orderbook_current_batch[symbol])}")

    async def _process_combined_data(self, symbol: str):
        """
        Метод для объединения последних актуальных данных (свечи и ордербук)
        и передачи их в стратегию и торговый процессор.
        Этот метод вызывается, когда накоплено достаточно свечей.
        """
        if symbol not in self.kline_buffer or len(self.kline_buffer[symbol]) < self.count_candles:
            logger.debug(f"В _process_combined_data: Недостаточно свечей ({len(self.kline_buffer.get(symbol, []))}/{self.count_candles}) для {symbol}. Пропускаем.")
            return
        
        if symbol not in self.orderbook_buffer_for_strategy or not self.orderbook_buffer_for_strategy[symbol]:
            logger.debug(f"В _process_combined_data: Буфер анализированных ордербуков пуст для {symbol}. Пропускаем.")
            return

        logger.info(f"Начинаем объединение данных для стратегии для {symbol}. "
                    f"Kline buffer size: {len(self.kline_buffer[symbol])}, "
                    f"Orderbook buffer size: {len(self.orderbook_buffer_for_strategy[symbol])}")
        
        kline_df_for_strategy = pd.DataFrame(self.kline_buffer[symbol])
        kline_df_for_strategy['timestamp'] = pd.to_datetime(kline_df_for_strategy['timestamp'], unit='ms')

        orderbook_df_for_strategy = pd.DataFrame(self.orderbook_buffer_for_strategy[symbol])

        if 'ts' not in orderbook_df_for_strategy.columns and 'timestamp' in orderbook_df_for_strategy.columns:
            orderbook_df_for_strategy['ts'] = orderbook_df_for_strategy['timestamp']

        if not kline_df_for_strategy.empty and not orderbook_df_for_strategy.empty:
            start_time_df = kline_df_for_strategy['timestamp'].min()
            end_time_df = kline_df_for_strategy['timestamp'].max()

            logger.debug(f"Вызов prepare_backtest_data для {symbol} с потоковыми данными. "
                        f"Kline DF shape: {kline_df_for_strategy.shape}, "
                        f"Orderbook DF shape: {orderbook_df_for_strategy.shape}. "
                        f"Время: {start_time_df} - {end_time_df}")

            combined_df = await prepare_backtest_data(
                repo_kline=self.repo_kline_init, # Use the initial repo for _backtest_data as it's not a write operation
                repo_orderbook=self.repo_orderbook_init, # Same here
                start_time=start_time_df,
                end_time=end_time_df,
                database_mode=False,
                kline_df=kline_df_for_strategy,
                orderbook_df=orderbook_df_for_strategy
            )

            if not combined_df.empty:
                logger.info(f"Сформирован объединенный DataFrame для {symbol} ({len(combined_df)} строк).")

                signal = await self.strategy_manager.run_all(
                    combined_df
                )
                logger.debug(f"Завершено обработка стратегий для {symbol} получены сигналы {signal}.")
                if signal:
                    logger.info(f"Получен торговый сигнал: {signal}")
                    await self.trade_processor.process_signal(signal)

                self.kline_buffer[symbol].clear()
                self.orderbook_buffer_for_strategy[symbol].clear()
                logger.info(f"Буферы kline и orderbook очищены для {symbol} после успешной обработки стратегией.")
            else:
                logger.warning(f"prepare_backtest_data вернул пустой DataFrame для {symbol}.")
        else:
            logger.debug(f"Недостаточно данных (свечей или ордербука) для формирования объединенного DataFrame для {symbol}. "
                        f"Kline buffer size: {len(self.kline_buffer.get(symbol, []))}, "
                        f"Orderbook buffer size: {len(self.orderbook_buffer_for_strategy.get(symbol, []))}")

    @staticmethod
    def _convert_interval_to_seconds(interval: str) -> int:
        """Вспомогательная функция для конвертации интервала kline в секунды."""
        interval_upper = interval.upper()
        if interval_upper.endswith("M") and interval_upper[:-1].isdigit(): 
            return int(interval_upper[:-1]) * 60
        elif interval_upper.isdigit(): 
            return int(interval_upper) * 60
        elif interval_upper == "D":
            return 24 * 60 * 60
        elif interval_upper == "W":
            return 7 * 24 * 60 * 60
        elif interval_upper == "M": 
            return 30 * 24 * 60 * 60
        else:
            logger.warning(f"Unsupported interval unit for conversion: {interval}. Defaulting to 60 seconds.")
            return 60