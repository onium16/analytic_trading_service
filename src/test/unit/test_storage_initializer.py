import pytest
import asyncio
import uuid
from datetime import datetime, timezone
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError, DatabaseError

from infrastructure.config.settings import settings
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.storage.schemas import (
    OrderbookSnapshotModel,
    KlineRecord,
    KlineRecordDatetime,
    OrderBookFilenameModel,
    TradeResult,
    TradeSignal,
    OrderBookDelta
)
from infrastructure.logging_config import setup_logger
import pytest_asyncio

logger = setup_logger(__name__)

# --- Фикстура для уникальной тестовой базы данных ---
@pytest_asyncio.fixture(scope="function")
async def setup_unique_test_db():
    unique_db_name = f"test_db_{uuid.uuid4().hex[:8]}_{datetime.now(tz=timezone.utc).strftime('%H%M%S')}"
    
    # Параметры подключения к ClickHouse для временного клиента
    ch_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    # Убедитесь, что CLICKHOUSE_PORT_TCP корректно установлен в CI
    ch_port = int(os.getenv('CLICKHOUSE_PORT_TCP', '9000')) 
    ch_user = os.getenv('CLICKHOUSE_USER', 'default')
    ch_password = os.getenv('CLICKHOUSE_PASSWORD', 'testpass') # Используем 'testpass'

    temp_client = None
    try:
        # 1. Создаем временный клиент, подключенный к базе данных по умолчанию (без указания test_db)
        # Это позволяет выполнять команды DDL, такие как CREATE DATABASE.
        temp_client = get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database='default' # Подключаемся к default, чтобы создать новую БД
        )
        
        # Проверяем соединение
        await asyncio.get_running_loop().run_in_executor(None, temp_client.ping)
        logger.info(f"Временное подключение к ClickHouse для создания БД: {ch_host}:{ch_port}/default")

        # 2. Создаем уникальную базу данных для теста
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: temp_client.command(f"CREATE DATABASE IF NOT EXISTS {unique_db_name}")
        )
        logger.info(f"Уникальная тестовая база данных '{unique_db_name}' создана.")

        # 3. Возвращаем имя уникальной базы данных для использования в тестах
        yield unique_db_name

    except (OperationalError, DatabaseError) as e:
        logger.error(f"Ошибка при настройке уникальной тестовой базы данных: {e}", exc_info=True)
        pytest.fail(f"Не удалось настроить уникальную тестовую базу данных: {e}")
    finally:
        # 4. Удаляем уникальную базу данных после завершения теста
        if temp_client:
            try:
                # Сначала закрываем все соединения к этой БД, если они есть
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: temp_client.command(f"KILL DATABASE {unique_db_name} SYNC")
                )
                # Затем удаляем базу данных
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: temp_client.command(f"DROP DATABASE IF EXISTS {unique_db_name}")
                )
                logger.info(f"Уникальная тестовая база данных '{unique_db_name}' успешно удалена.")
            except Exception as e:
                logger.error(f"Ошибка при удалении уникальной тестовой базы данных '{unique_db_name}': {e}", exc_info=True)
            finally:
                await asyncio.get_running_loop().run_in_executor(None, temp_client.close)


@pytest_asyncio.fixture(scope="function")
async def temp_clickhouse_repo(setup_unique_test_db):
   
    unique_db_name = setup_unique_test_db
    repo = ClickHouseRepository(
        schema=OrderbookSnapshotModel,
        table_name="test_orderbook_snapshots",
        db=unique_db_name
    )
    yield repo
    await repo.close() # Закрываем соединение после теста

# --- Тесты ---

@pytest.mark.asyncio
async def test_initialisation_storage_integration(temp_clickhouse_repo):
    # Используем temp_clickhouse_repo, который уже подключен к уникальной БД
    # и эта БД уже создана.
    repo = temp_clickhouse_repo
    
    # Проверяем, что база данных существует
    assert await repo.database_exists()
    
    # Проверяем, что таблица не существует до инициализации
    assert not await repo.table_exists()

    tables_to_init = [
        (OrderbookSnapshotModel, repo.table_name), # Используем имя таблицы из фикстуры
        (KlineRecord, settings.clickhouse.table_kline_archive_testnet),
        (KlineRecordDatetime, settings.clickhouse.table_kline_archive_datetime_testnet),
        (OrderBookFilenameModel, settings.clickhouse.table_orderbook_archive_filename_testnet),
        (TradeResult, settings.clickhouse.table_trade_results_testnet),
        (TradeSignal, settings.clickhouse.table_trade_signals), # Возможно, здесь тоже нужен testnet вариант
        (OrderBookDelta, settings.clickhouse.table_positions_testnet),
    ]


    initializer_client = get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT_TCP', '9000')),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'testpass'),
        database=repo.db # Подключаемся к уникальной БД
    )
    initializer = StorageInitializer(settings, logger, initializer_client)
    await initializer.initialize(tables_to_init)
    await initializer_client.close() # Закрываем клиент инициализатора

    # Проверяем, что таблица была создана
    assert await repo.table_exists()

    # Проверяем, что можно сохранить данные
    test_data = OrderbookSnapshotModel(
        symbol="BTCUSDT",
        timestamp=datetime.now(timezone.utc),
        bids=[[100.0, 1.0]],
        asks=[[101.0, 1.0]]
    )
    await repo.save(test_data)

    # Проверяем, что данные были сохранены
    retrieved_data = await repo.get_all()
    assert len(retrieved_data) == 1
    assert retrieved_data[0].symbol == "BTCUSDT"

    # Очищаем таблицу
    await repo.delete_all()
    retrieved_data_after_delete = await repo.get_all()
    assert len(retrieved_data_after_delete) == 0

# --- Другие тесты (если есть) ---
# @pytest.mark.asyncio
# async def test_another_repo_method(temp_clickhouse_repo):
#    repo = temp_clickhouse_repo
#    # Ваш тест здесь
