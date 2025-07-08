import pytest
import asyncio
import os
import zipfile
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock
from application.archive_processor import ArchiveProcessor
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import OrderbookSnapshotModel, OrderBookFilenameModel
from infrastructure.config.settings import settings
from domain.delta_analyzer import DeltaAnalyzerArchive
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger

logger = setup_logger("test_archive_processing_flow", level=settings.logger_level)

# Временная директория для датасетов
@pytest.fixture(scope="module")
def temp_datasets_dir(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("test_datasets")
    original_datasets_dir = settings.datasets_dir
    settings.datasets_dir = str(temp_dir)
    yield temp_dir
    settings.datasets_dir = original_datasets_dir # Восстанавливаем оригинальный путь

# Создаем фиктивный ZIP-файл
@pytest.fixture
def create_mock_zip_file(temp_datasets_dir):
    def _create_mock_zip_file(filename="test_archive.zip"):
        zip_path = temp_datasets_dir / filename
        with zipfile.ZipFile(zip_path, 'w') as zf:
            # JSON Lines (ndjson) для корректной работы pd.read_json(..., lines=True)
            jsonl_content = (
                '{"ts":1678886400000,"data":{"topic":"orderbook.400.B", "type":"update", "s":"ETHUSDT", "u":12345, "seq":1, "uuid":111, "b": [["100.0", "1000.0"]], "a": [["100.1", "800.0"]]}}\n'
                '{"ts":1678886401000,"data":{"topic":"orderbook.400.B", "type":"update", "s":"ETHUSDT", "u":12346, "seq":2, "uuid":222, "b": [["99.9", "500.0"]], "a": [["100.2", "400.0"]]}}\n'
            )
            zf.writestr("data.json", jsonl_content)  # Имя файла изменено на data.json
        return str(zip_path), filename
    return _create_mock_zip_file

@pytest.fixture(scope="module")
async def setup_clickhouse_for_archive_test():
    test_db_name = f"test_archive_db_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    original_db_name = settings.clickhouse.db_name
    original_snapshots_table = settings.clickhouse.table_orderbook_snapshots
    original_filenames_table = settings.clickhouse.table_orderbook_archive_filename

    settings.clickhouse.db_name = test_db_name
    settings.clickhouse.table_orderbook_snapshots = "orderbook_snapshots_test"
    settings.clickhouse.table_orderbook_archive_filename = "orderbook_filenames_test"

    client = await settings.clickhouse.connect()
    initializer = StorageInitializer(settings, MagicMock(), client)
    
    try:
        async with asyncio.timeout(10):  # Тайм-аут 10 секунд
            await initializer.create_database(test_db_name)
            await initializer.initialize([
                (OrderbookSnapshotModel, settings.clickhouse.table_orderbook_snapshots),
                (OrderBookFilenameModel, settings.clickhouse.table_orderbook_archive_filename)
            ])
        yield  # Выполнение тестов
    finally:
        try:
            async with asyncio.timeout(5):  # Тайм-аут 5 секунд для очистки
                await client.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        except asyncio.TimeoutError:
            logger.warning(f"Warning: Timeout while dropping database {test_db_name}")
        finally:
            await client.close()
        settings.clickhouse.db_name = original_db_name
        settings.clickhouse.table_orderbook_snapshots = original_snapshots_table
        settings.clickhouse.table_orderbook_archive_filename = original_filenames_table

@pytest.mark.asyncio
async def test_full_archive_processing_flow(temp_datasets_dir, create_mock_zip_file, setup_clickhouse_for_archive_test):
    zip_full_path, zip_filename = create_mock_zip_file("test_orderbook.zip")

    processor = ArchiveProcessor()
    
    # Мокаем DeltaAnalyzerArchive, чтобы избежать реальной обработки
    with patch("application.archive_processor.DeltaAnalyzerArchive") as MockDeltaAnalyzer:
        mock_analyzer = MockDeltaAnalyzer.return_value
        mock_analyzer.analyze.return_value = pd.DataFrame([
            {"timestamp": 1678886400000, "symbol": "ETHUSDT", "bid_price": 100.0, "bid_volume": 1000.0, "ask_price": 100.1, "ask_volume": 800.0},
            {"timestamp": 1678886401000, "symbol": "ETHUSDT", "bid_price": 99.9, "bid_volume": 500.0, "ask_price": 100.2, "ask_volume": 400.0}
        ])

        async with asyncio.timeout(10):  # Тайм-аут 10 секунд
            await processor.process_all_archives()

    # Проверяем, что снимки были сохранены в БД
    repo_snapshots = ClickHouseRepository(
        schema=OrderbookSnapshotModel,
        table_name=settings.clickhouse.table_orderbook_snapshots,
        db=settings.clickhouse.db_name
    )
    saved_snapshots = await repo_snapshots.get_all()
    assert len(saved_snapshots) >= 2  # Ожидаем две записи из фиктивного JSON

    # Проверяем, что имя файла было отмечено как обработанное
    repo_filenames = ClickHouseRepository(
        schema=OrderBookFilenameModel,
        table_name=settings.clickhouse.table_orderbook_archive_filename,
        db=settings.clickhouse.db_name
    )
    processed_filenames = await repo_filenames.get_all()
    assert len(processed_filenames) == 1
    assert processed_filenames[0].filename == zip_filename

    # Проверяем, что повторная обработка того же файла пропускается
    processor.logger = MagicMock()
    async with asyncio.timeout(5):  # Тайм-аут 5 секунд
        await processor.process_all_archives()
    processor.logger.info.assert_called_with(f"Файл {zip_filename} уже обработан, пропуск.")