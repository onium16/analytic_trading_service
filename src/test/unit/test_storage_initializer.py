import asyncio
from datetime import datetime, timezone
import uuid
# from clickhouse_connect import get_client # Если get_client не используется напрямую в тестах, его можно удалить
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from main import initialisation_storage # Импортируем тестируемую функцию
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.config.settings import settings
from infrastructure.storage.schemas import KlineRecord, OrderbookSnapshotModel, OrderBookFilenameModel, KlineRecordDatetime, TradeSignal, TradeResult, OrderBookDelta
from infrastructure.logging_config import setup_logger
import pytest_asyncio


test_logger = setup_logger("test_"+__name__, level=settings.logger_level)



@pytest.fixture
def mock_clickhouse_client():
    mock_client = MagicMock()

    mock_cursor = MagicMock()
    
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=None)
    
    mock_cursor.execute = AsyncMock(return_value=None)

    mock_client.cursor.return_value = mock_cursor

    return mock_client

@pytest.fixture
def storage_initializer(mock_clickhouse_client):
    mock_logger = MagicMock()
    mock_logger.info = MagicMock()
    mock_logger.debug = MagicMock()
    mock_logger.error = MagicMock()
    
    mock_settings = MagicMock()
    mock_settings.clickhouse = MagicMock()
    mock_settings.clickhouse.db_name = "mock_test_db" # Устанавливаем заглушку для db_name

    return StorageInitializer(mock_settings, mock_logger, mock_clickhouse_client)

@pytest.mark.asyncio
async def test_create_database(storage_initializer, mock_clickhouse_client):
    db_name = "default"
    
    await storage_initializer.create_database(db_name)
    
    mock_clickhouse_client.cursor.assert_called_once()
    
    mock_cursor = mock_clickhouse_client.cursor.return_value
    
    mock_cursor.execute.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    
    storage_initializer.logger.info.assert_called_once_with(f"Инициализация базы данных {db_name} ClickHouse...")

@pytest.mark.asyncio
@patch("infrastructure.storage.repositories.storage_initializer.ClickHouseRepository")
async def test_initialize_tables(mock_repo_class, storage_initializer):
    mock_repo = AsyncMock()
    mock_repo_class.return_value = mock_repo

    tables_to_init = [
        (KlineRecord, "test_kline_table"),
        (OrderbookSnapshotModel, "test_orderbook_table")
    ]

    await storage_initializer.initialize(tables_to_init)

    assert mock_repo_class.call_count == len(tables_to_init)
    
    assert mock_repo.ensure_table.await_count == len(tables_to_init)
    
    storage_initializer.logger.info.assert_called_with("Все переданные таблицы ClickHouse успешно инициализированы.")

@pytest.mark.asyncio
@patch("infrastructure.storage.repositories.storage_initializer.ClickHouseRepository")
async def test_initialization_failure(mock_repo_class, storage_initializer):
    mock_repo = AsyncMock()
    mock_repo.ensure_table.side_effect = Exception("DB error")
    mock_repo_class.return_value = mock_repo

    tables_to_init = [(KlineRecord, "test_kline_table")]

    with pytest.raises(Exception, match="DB error"):
        await storage_initializer.initialize(tables_to_init)

    storage_initializer.logger.error.assert_called_once_with(
        f"Ошибка при инициализации таблицы {tables_to_init[0][1]}: DB error"
    )


@pytest_asyncio.fixture(scope="function")
async def setup_unique_test_db():
    unique_db_name = f"test_db_{uuid.uuid4().hex[:8]}_{datetime.now(tz=timezone.utc).strftime('%H%M%S')}"
    
    temp_repo = ClickHouseRepository(
        schema=OrderbookSnapshotModel, # Схема здесь не важна, т.к. мы управляем БД
        table_name="temp_table_for_db_ops",
        db=unique_db_name, # Указываем нашу тестовую базу
        port=settings.clickhouse.port_http # Используем порт из настроек
    )

    try:
        test_logger.info(f"Начинаем интеграционный тест: Проверяем/создаем базу данных {unique_db_name}...")
        
        try:
            temp_repo.client.command("SELECT 1") 
            test_logger.info("Успешно подключились к ClickHouse.")
        except Exception as e:
            pytest.fail(f"Не удалось подключиться к ClickHouse для интеграционного теста. Проверьте, запущен ли ClickHouse. Ошибка: {e}")

        temp_repo.client.command(f"DROP DATABASE IF EXISTS {unique_db_name}")
        test_logger.info(f"Предварительная очистка: База данных {unique_db_name} удалена, если существовала.")

        yield unique_db_name 

    finally:
        test_logger.info(f"Завершение интеграционного теста: Удаляем базу данных {unique_db_name}...")
        try:
            temp_repo.client.command(f"DROP DATABASE IF EXISTS {unique_db_name}")
            test_logger.info(f"База данных {unique_db_name} успешно удалена.")
        except Exception as e:
            test_logger.error(f"Ошибка при удалении базы данных {unique_db_name}: {e}")

@pytest.mark.asyncio
async def test_initialisation_storage_integration(setup_unique_test_db):
    test_db_name = setup_unique_test_db 
    
    original_db_name = settings.clickhouse.db_name 
    
    try:
        settings.clickhouse.db_name = test_db_name

        test_logger.info(f"Запуск initialisation_storage для базы: {test_db_name}")
        await initialisation_storage(testnet=False)

        temp_checker_repo = ClickHouseRepository(
            schema=OrderbookSnapshotModel,
            table_name="temp_check_table",
            db=test_db_name,
            port=settings.clickhouse.port_http
        )
        
        # Проверяем, что база данных была создана
        exists_db = temp_checker_repo.client.command(f"EXISTS DATABASE {test_db_name}")
        assert bool(int(exists_db)) is True, f"База данных '{test_db_name}' не была создана функцией initialisation_storage."

        expected_tables = [
            settings.clickhouse.table_orderbook_snapshots,
            settings.clickhouse.table_orderbook_archive_filename,
            settings.clickhouse.table_kline_archive,
            settings.clickhouse.table_kline_archive_datetime,
            settings.clickhouse.table_trade_signals,
            settings.clickhouse.table_trade_results,
            settings.clickhouse.table_positions,
        ]

        for table_name in expected_tables:
            exists_table = temp_checker_repo.client.command(f"EXISTS TABLE {test_db_name}.{table_name}")
            assert bool(int(exists_table)) is True, f"Таблица '{table_name}' в базе '{test_db_name}' не была создана."
            test_logger.info(f"Таблица '{test_db_name}.{table_name}' успешно проверена.")

        test_logger.info(f"Функция initialisation_storage успешно инициализировала базу данных {test_db_name} и все таблицы.")

    finally:
        settings.clickhouse.db_name = original_db_name 