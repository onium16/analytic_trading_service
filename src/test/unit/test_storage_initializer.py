import asyncio
import logging
from copy import deepcopy

# Предполагаемые импорты из вашей структуры проекта
# Убедитесь, что пути к этим файлам корректны
from infrastructure.config.settings import settings
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.storage.schemas import (
    KlineRecord,
    KlineRecordDatetime,
    OrderBookDelta,
    OrderBookFilenameModel,
    OrderBookSnapshot,
    OrderbookSnapshotModel,
    TradeResult,
    TradeSignal,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Определяем тестовые имена для базы данных и таблиц
TEST_DB_NAME = "test_app_db"
TEST_TABLE_PREFIX = "test_"

# Создаем копию настроек и модифицируем их для тестовой базы данных
test_settings = deepcopy(settings)
test_settings.clickhouse.db_name = TEST_DB_NAME
# Модифицируем имена таблиц для теста, добавляя префикс
test_settings.clickhouse.table_orderbook_snapshots = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_orderbook_snapshots}"
test_settings.clickhouse.table_orderbook_archive_filename = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_orderbook_archive_filename}"
test_settings.clickhouse.table_kline_archive = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_kline_archive}"
test_settings.clickhouse.table_kline_archive_datetime = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_kline_archive_datetime}"
test_settings.clickhouse.table_trade_signals = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_trade_signals}"
test_settings.clickhouse.table_trade_results = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_trade_results}"
test_settings.clickhouse.table_positions = f"{TEST_TABLE_PREFIX}{settings.clickhouse.table_positions}"


# Список всех тестовых таблиц для операций
ALL_TEST_TABLES = [
    (OrderbookSnapshotModel, test_settings.clickhouse.table_orderbook_snapshots),
    (OrderBookFilenameModel, test_settings.clickhouse.table_orderbook_archive_filename),
    (KlineRecord, test_settings.clickhouse.table_kline_archive),
    (KlineRecordDatetime, test_settings.clickhouse.table_kline_archive_datetime),
    (TradeSignal, test_settings.clickhouse.table_trade_signals),
    (TradeResult, test_settings.clickhouse.table_trade_results),
    (OrderBookDelta, test_settings.clickhouse.table_positions),
]

async def check_table_exists(client, db_name: str, table_name: str) -> bool:
    """Проверяет, существует ли таблица в базе данных ClickHouse."""
    cur = client.cursor()
    async with cur:
        query = f"EXISTS TABLE {db_name}.{table_name}"
        result = await cur.execute(query)
        # execute возвращает список списков, например [[1]] или [[0]]
        return result[0][0] == 1


async def drop_table(table_name: str):
    """Удаляет указанную таблицу из тестовой базы данных."""
    logger.info(f"Удаление таблицы '{table_name}' из базы данных '{TEST_DB_NAME}'...")
    repo = ClickHouseRepository(
        schema=None, # Схема не нужна для DROP TABLE
        db=TEST_DB_NAME,
        table_name=table_name
    )

    try:
        # Для удаления напрямую через cursor:
        client = await test_settings.clickhouse.connect()
        cur = client.cursor()
        async with cur:
            await cur.execute(f"DROP TABLE IF EXISTS {TEST_DB_NAME}.{table_name}")
        await client.close() # Закрываем временное соединение
        logger.info(f"Таблица '{table_name}' успешно удалена.")
    except Exception as e:
        logger.error(f"Ошибка при удалении таблицы '{table_name}': {e}", exc_info=True)


async def drop_database(db_name: str):
    """Удаляет указанную базу данных ClickHouse."""
    logger.info(f"Удаление базы данных '{db_name}'...")
    # Для удаления базы данных нужно подключиться к 'default' или любой другой существующей
    tmp_settings = deepcopy(test_settings.clickhouse)
    tmp_settings.db_name = "default" # Подключаемся к default для удаления другой базы

    client = None
    try:
        client = await tmp_settings.connect()
        cur = client.cursor()
        async with cur:
            await cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
        logger.info(f"База данных '{db_name}' успешно удалена.")
    except Exception as e:
        logger.error(f"Ошибка при удалении базы данных '{db_name}': {e}", exc_info=True)
    finally:
        if client:
            await client.close()


async def run_test_scenario():
    """Главный сценарий тестирования операций с ClickHouse."""
    logger.info("--- Начинаем тестовый сценарий операций с ClickHouse ---")

    # 1. Очистка перед началом (на случай предыдущего незавершенного теста)
    logger.info("Попытка предварительной очистки: удаление тестовых таблиц и базы данных...")
    for _, table_name in ALL_TEST_TABLES:
        await drop_table(table_name)
    await drop_database(TEST_DB_NAME)
    logger.info("Предварительная очистка завершена.")

    # 2. Инициализация (создание БД и таблиц)
    logger.info(f"Инициализация хранилища для тестовой базы данных '{TEST_DB_NAME}'...")
    initializer = StorageInitializer(settings=test_settings, logger=logger)
    try:
        await initializer.initialisation_storage()
        logger.info("Инициализация тестового хранилища успешно завершена.")
    finally:
        if initializer.client:
            await initializer.client.close() # Закрываем основное соединение инициализатора

    # 3. Проверка наличия таблиц после создания
    logger.info("Проверка наличия созданных таблиц...")
    main_client_for_checks = await test_settings.clickhouse.connect()
    try:
        all_tables_exist = True
        for schema, table_name in ALL_TEST_TABLES:
            exists = await check_table_exists(main_client_for_checks, TEST_DB_NAME, table_name)
            if exists:
                logger.info(f"Таблица '{table_name}' СУЩЕСТВУЕТ в '{TEST_DB_NAME}'.")
            else:
                logger.error(f"Ошибка: Таблица '{table_name}' НЕ СУЩЕСТВУЕТ в '{TEST_DB_NAME}'.")
                all_tables_exist = False
        if all_tables_exist:
            logger.info("Все тестовые таблицы успешно проверены и существуют.")
        else:
            logger.error("Некоторые тестовые таблицы отсутствуют после инициализации.")
    finally:
        if main_client_for_checks:
            await main_client_for_checks.close()


    # 4. Удаление всех тестовых таблиц
    logger.info("Начинаем удаление всех тестовых таблиц...")
    for _, table_name in ALL_TEST_TABLES:
        await drop_table(table_name)
    logger.info("Все тестовые таблицы удалены.")

    # 5. Проверка отсутствия таблиц после удаления
    logger.info("Проверка отсутствия таблиц после удаления...")
    main_client_for_checks_after_drop = await test_settings.clickhouse.connect()
    try:
        all_tables_dropped = True
        for schema, table_name in ALL_TEST_TABLES:
            exists = await check_table_exists(main_client_for_checks_after_drop, TEST_DB_NAME, table_name)
            if exists:
                logger.error(f"Ошибка: Таблица '{table_name}' СУЩЕСТВУЕТ после удаления.")
                all_tables_dropped = False
            else:
                logger.info(f"Таблица '{table_name}' ОТСУТСТВУЕТ после удаления.")
        if all_tables_dropped:
            logger.info("Все тестовые таблицы успешно проверены и отсутствуют.")
        else:
            logger.error("Некоторые тестовые таблицы не были удалены.")
    finally:
        if main_client_for_checks_after_drop:
            await main_client_for_checks_after_drop.close()


    # 6. Удаление тестовой базы данных
    await drop_database(TEST_DB_NAME)
    logger.info("Тестовая база данных удалена.")

    logger.info("--- Тестовый сценарий операций с ClickHouse завершен ---")


if __name__ == "__main__":
    asyncio.run(run_test_scenario())