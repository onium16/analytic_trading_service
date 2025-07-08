import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.config.settings import settings
from infrastructure.storage.schemas import KlineRecord, OrderbookSnapshotModel


@pytest.fixture
def mock_clickhouse_client():
    mock_client = MagicMock()

    # cursor должен быть асинхронным, если в коде await self.client.cursor()
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None
    mock_cursor.execute.return_value = None

    # cursor() должен возвращать корутину, т.е. быть AsyncMock
    mock_client.cursor = AsyncMock(return_value=mock_cursor)

    return mock_client



@pytest.fixture
def storage_initializer(mock_clickhouse_client):
    mock_logger = MagicMock()
    return StorageInitializer(settings, mock_logger, mock_clickhouse_client)


@pytest.mark.asyncio
async def test_create_database(storage_initializer, mock_clickhouse_client):
    db_name = "test_db"

    # ВАЖНО: здесь нужно await
    mock_cursor = await mock_clickhouse_client.cursor()

    await storage_initializer.create_database(db_name)

    mock_cursor.execute.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    storage_initializer.logger.info.assert_called_once()




@pytest.mark.asyncio
@patch("infrastructure.storage.repositories.storage_initializer.ClickHouseRepository")
async def test_initialize_tables(mock_repo_class, storage_initializer):
    # Мокаем репозиторий и метод ensure_table
    mock_repo = AsyncMock()
    mock_repo_class.return_value = mock_repo

    tables_to_init = [
        (KlineRecord, "test_kline_table"),
        (OrderbookSnapshotModel, "test_orderbook_table")
    ]

    await storage_initializer.initialize(tables_to_init)

    assert mock_repo.ensure_table.await_count == 2
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

    # Убери эту строку, если ты не логируешь ошибку
    # storage_initializer.logger.error.assert_called()
