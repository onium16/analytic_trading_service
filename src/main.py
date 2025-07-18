import asyncio
import sys

import aiohttp

from application.archive_processor import ArchiveProcessor
from application.backtest_runner import BacktestRunner
from application.commands.collect_kline_api_data import CollectKlineApiDataCommand
from application.commands.collect_kline_ws_data import CollectKlineWsDataCommand
from application.commands.collect_orderbook_api_data import CollectOrderbookApiDataCommand
from application.commands.collect_orderbook_ws_data import CollectOrderbookWsDataCommand
from application.handlers.collect_kline_api_data_handler import CollectKlineApiDataHandler
from application.handlers.collect_kline_ws_data_handler import CollectKlineWsDataHandler
from application.handlers.collect_orderbook_api_data_handler import CollectOrderbookApiDataHandler
from application.handlers.collect_orderbook_ws_data_handler import CollectOrderbookWsDataHandler
from application.services.data_pipeline_service import DataPipelineService
from application.services.event_publisher import EventPublisher
from application.services.kline_data_collector import KlineDataCollector
from application.services.orderbook_data_collector import OrderbookDataCollector
from application.stream_strategy_processor import StreamStrategyProcessor
from application.trade_processor import TradingProcessor
from domain.events.data_events import KlineDataReceivedEvent, KlineDataReceivedWsEvent, OrderbookSnapshotReceivedEvent, OrderbookSnapshotReceivedWsEvent
from infrastructure.config.config_loader import apply_environment_settings
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.storage.schemas import KlineRecord
from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

from infrastructure.adapters.archive_kline_parser import KlineParser

logger = setup_logger(__name__, level=settings.logger_level)

# Выбрать тестовую сеть мы можем только при работе св режиме стриминга
settings.testnet = False

# Collect archive data (zip, parsing)
settings.archive_mode = True

# Backtest strategies on archive data or stream data 
settings.backtest_mode= True

# Collect archive data and backtest data
settings.archive_source = True
settings.stream_source = False

# Stream data + Air BackTesting + Trading
settings.stream_mode = False
settings.use_ws = True # API/WebSocket)

settings.start_time = "2025-06-16"
settings.end_time = "2025-06-21"

async def run_basktest_application():
    """
    Функция запуска процессора для тестового режима стратегий на архивных данных из архивных или стриминговых данных.
    Включает также получение лучших настроек для стратегий. и сохранение их для использования в стриминговом режиме (трейдинге).
    """
    logger.info("Запуск  режима тестирования стратегий...")

    # Backtesting в BACKTEST_MODE работает на архивных данных из указанных источников. 
    # Проверяет наличие данных за указанный период. Если есть то работаем если нет то отказ.
    if not settings.archive_source and not settings.stream_source:
        logger.error("Тестовый режим не может быть запущен без источников данных.")
        sys.exit(1)

    if settings.archive_source and settings.stream_source:
        # flag snapshot_stream + delta по данным из всех источников делает поиск
        logger.info("Запуск оценки стратегий... По архивным и стриминговым данным совместно. Обновление параметров стратегий.")
    if settings.archive_source :
        logger.info("Запуск оценки стратегий... По архивным данным. Обновление параметров стратегий.")
    if settings.stream_source:
        logger.info("Запуск оценки стратегий... По архивным стриминговым данным...")
    
    backtest_runner = BacktestRunner(
            archive_mode=True,
            stream_mode=False,
            archive_source=settings.archive_source,
            stream_source=settings.stream_source
        )
        
    await backtest_runner.run_backtest() 

    logger.info("Режим тестирования стратегий завершен.")

async def run_archive_application():
    
    """
    Функция запуска процессора для работы с архивными данными
    
    """  

    logger.info("Запуск парсинга архивных данных свечей по дате ...")
    # Запускаем парсер свечей по дате и токену.
    kline_repo = ClickHouseRepository(
        schema=KlineRecord,
        db=settings.clickhouse.db_name,
        table_name=settings.clickhouse.table_kline_archive
    )
    kline_parser = KlineParser(repository=kline_repo, symbol=settings.pair_tokens, interval=settings.kline.interval)
    await kline_parser.collect_kline_range(settings.start_time, settings.end_time)
    logger.info("Обработка архивных данных свечей по дате завершена...")

    # запуск процессора для архивных данных
    logger.info("Запуск обработки архивных данных (архивы)...")

    # Обработчик архивных данных файлов zip из папки datasets (settings.datasets_dir)               
    archive_processor = ArchiveProcessor()
    await archive_processor.process_all_archives()
    logger.info("Обработка архивных данных (архивы) завершена...")

    logger.info("Обработка архивных данных в режиме ARCHIVE_SOURCE завершена.")
    logger.info("Режим ARCHIVE_MODE завершил работу.")

async def run_stream_application(use_ws: bool = False):
    logger.info("Запуск приложения...")

    event_publisher = EventPublisher()
    stream_strategy_processor = StreamStrategyProcessor()
    trade_processor = TradingProcessor()

    data_pipeline_service = DataPipelineService(
        trade_processor=trade_processor,
        stream_strategy_processor=stream_strategy_processor
    )
    await data_pipeline_service.initialize()

    # === Подписываемся на события
    event_publisher.subscribe(KlineDataReceivedEvent, data_pipeline_service.handle_kline_data)
    event_publisher.subscribe(KlineDataReceivedWsEvent, data_pipeline_service.handle_kline_data)
    event_publisher.subscribe(OrderbookSnapshotReceivedEvent, data_pipeline_service.handle_orderbook_snapshot)
    event_publisher.subscribe(OrderbookSnapshotReceivedWsEvent, data_pipeline_service.handle_orderbook_snapshot)

    async with aiohttp.ClientSession() as session:
        # === Инициализируем обработчики
        kline_collector = KlineDataCollector(
            api_handler=CollectKlineApiDataHandler(session, event_publisher),
            ws_handler=CollectKlineWsDataHandler(settings.bybit.ws_url, event_publisher),
            use_ws=use_ws
        )

        orderbook_collector = OrderbookDataCollector(
            api_handler=CollectOrderbookApiDataHandler(session, event_publisher),
            ws_handler=CollectOrderbookWsDataHandler(settings.bybit.ws_url, event_publisher),
            use_ws=use_ws
        )

        if not use_ws:
        # === Команды API
            kline_command = (
                
                CollectKlineApiDataCommand
            )(
                symbol=settings.pair_tokens,
                interval=settings.kline.interval,
                duration=settings.streaming.duration or 3600 * 24 * 365,
                interval_iteration=settings.kline.timer_iteration
            ) 

            orderbook_command = (
                CollectOrderbookApiDataCommand
            )(
                symbol=settings.pair_tokens,
                limit=settings.streaming.snapshots_orderbook_depth,
                duration=settings.streaming.duration or 3600 * 24 * 365,
                interval_iteration=1
            )

        if use_ws:
            # === Команды WS
            kline_command = (
                CollectKlineWsDataCommand 
            )(
                symbol=settings.pair_tokens,
                interval=settings.kline.interval,
                duration=settings.streaming.duration or 3600 * 24 * 365,
            )

            orderbook_command = (
                CollectOrderbookWsDataCommand
            )(
                symbol=settings.pair_tokens,
                duration=settings.streaming.duration or 3600 * 24 * 365,
                depth=settings.streaming.snapshots_orderbook_depth,
            )


        logger.info(f"Используется источник данных: {'WebSocket' if use_ws else 'REST API'}")

        # === Запуск задач
        kline_task = asyncio.create_task(kline_collector.collect(kline_command))
        orderbook_task = asyncio.create_task(orderbook_collector.collect(orderbook_command))

        tasks = [kline_task, orderbook_task]

        try:
            if settings.streaming.duration is not None:
                logger.info(f"Приложение будет работать {settings.streaming.duration} сек.")
                done, pending = await asyncio.wait(
                    tasks, timeout=settings.streaming.duration, return_when=asyncio.ALL_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if pending:
                    logger.warning(f"{len(pending)} задач были отменены по таймауту.")
            else:
                logger.info("Приложение будет работать бессрочно (Ctrl+C для остановки).")
                await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Сбор данных отменён.")
        except Exception as e:
            logger.exception(f"Ошибка в run_application: {e}")
        finally:
            logger.info("Завершение всех задач...")
            for task in tasks:
                if not task.done():
                    task.cancel()
            logger.info("Все задачи завершены.")

    logger.info("Завершение приложения.")

async def main():
    if settings.testnet:
        logger.info("Запуск приложения в режиме TEST_NET...")
    else:
        logger.info("Запуск приложения в режиме PRODUCTION...")

    if not settings.archive_mode and not settings.backtest_mode and not settings.stream_source:
        logger.error("Ни один режим не выбран. Выберите хотя бы один режим для начала работыю.") 
    

    try:
        # --- ШАГ 1: Применяем настройки окружения (тестнет/боевая) ПЕРЕД ВСЕМИ ИНИЦИАЛИЗАЦИЯМИ ---
        # Это гарантирует, что все последующие объекты будут использовать актуальные настройки.
        apply_environment_settings(settings, settings.testnet)
        logger.info(f"Настройки среды применены. Используется TEST_NET: {settings.testnet}")
        # После этой строки, settings.clickhouse.db_name и другие параметры уже должны быть обновлены.

        # --- ШАГ 2: Подключение к ClickHouse и инициализация хранилища ---
        # Теперь client будет использовать обновленные настройки из 'settings'
        client = await settings.clickhouse.connect(use_db=False)
        initializer = StorageInitializer(settings, logger, client)
        await initializer.initialisation_storage()
        logger.info("Хранилище ClickHouse инициализировано.")

        # --- ШАГ 3: Запуск режимов в соответствии с флагами ---
        if settings.archive_mode and not settings.testnet:
            logger.info("Запуск режима ARCHIVE_MODE.")
            await run_archive_application()
        elif settings.archive_mode and settings.testnet:
            logger.warning("Режим ARCHIVE_MODE не запускается в тестовой сети (settings.testnet = True).")

        if settings.backtest_mode:
            logger.info("Запуск режима BACKTEST_MODE.")
            await run_basktest_application()
        
        # Условие для стриминга: просто проверяем флаг STREAM_MODE
        if settings.stream_source:
            logger.info(f"Запуск режима STREAM_MODE. Использование WebSocket: {settings.use_ws}")
            await run_stream_application(use_ws=settings.use_ws)
        else:
            logger.info("Режим STREAM_MODE отключен.")

        logger.info("Программа завершена.")
    except Exception as e:
        logger.exception(f"Произошла непредвиденная ошибка при запуске приложения: {e}")
        return 1
    return 0

    

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
