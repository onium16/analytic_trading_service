import pytest
import asyncio
import os
import zipfile
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from application.archive_processor import ArchiveProcessor
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import OrderbookSnapshotModel, OrderBookFilenameModel
from infrastructure.config.settings import settings
from domain.delta_analyzer import DeltaAnalyzerArchive
from infrastructure.logging_config import setup_logger
import pytest_asyncio
from src.test.test_data import MOCK_ORDERBOOK_SNAPSHOT_DATA

test_logger = setup_logger("test_clickhouse_setup", level=settings.logger_level)


# Временная директория для датасетов
@pytest.fixture(scope="module")
def temp_datasets_dir(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("test_datasets")
    original_datasets_dir = settings.datasets_dir
    settings.datasets_dir = str(temp_dir)
    yield temp_dir
    settings.datasets_dir = original_datasets_dir

# Создаем фиктивный ZIP-файл
@pytest.fixture
def create_mock_zip_file(temp_datasets_dir):
    def _create_mock_zip_file(filename="test_archive.zip"):
        zip_path = temp_datasets_dir / filename
        with zipfile.ZipFile(zip_path, 'w') as zf:
            jsonl_content = (
                '{"ts":1678886400000,"data":{"topic":"orderbook.400.B", "type":"update", "s":"ETHUSDT", "u":12345, "seq":1, "uuid":111, "b": [["100.0", "1000.0"]], "a": [["100.1", "800.0"]]}}\n'
                '{"ts":1678886401000,"data":{"topic":"orderbook.400.B", "type":"update", "s":"ETHUSDT", "u":12346, "seq":2, "uuid":222, "b": [["99.9", "500.0"]], "a": [["100.2", "400.0"]]}}\n'
            )
            zf.writestr("data.json", jsonl_content)
        return str(zip_path), filename
    return _create_mock_zip_file

# Фикстура для настройки ClickHouse
# !!! ОБЯЗАТЕЛЬНО ИСПОЛЬЗУЕМ @pytest_asyncio.fixture для асинхронных фикстур !!!
@pytest_asyncio.fixture(scope="module")
async def setup_clickhouse_for_archive_test():
    test_db_name = f"test_archive_db_{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
    original_db_name = settings.clickhouse.db_name
    original_snapshots_table = settings.clickhouse.table_orderbook_snapshots
    original_filenames_table = settings.clickhouse.table_orderbook_archive_filename

    settings.clickhouse.db_name = test_db_name
    settings.clickhouse.table_orderbook_snapshots = "orderbook_snapshots_test"
    settings.clickhouse.table_orderbook_archive_filename = "orderbook_filenames_test"

    # Создаём репозитории, которые будем использовать для инициализации и очистки
    # Это важно: каждый репозиторий имеет свой внутренний клиент ClickHouse
    snapshots_repo_for_init = ClickHouseRepository(
        schema=OrderbookSnapshotModel,
        table_name=settings.clickhouse.table_orderbook_snapshots,
        db=test_db_name,
        port=settings.clickhouse.port_http
    )
    filenames_repo_for_init = ClickHouseRepository(
        schema=OrderBookFilenameModel,
        table_name=settings.clickhouse.table_orderbook_archive_filename,
        db=test_db_name,
        port=settings.clickhouse.port_http
    )

    try:
        test_logger.info(f"Начинаем настройку ClickHouse для теста. База: {test_db_name}")
        
        # Создаём базу данных
        async with asyncio.timeout(30): # Увеличим таймаут для setup
            await snapshots_repo_for_init.create_database() # Создаст базу данных
            test_logger.info(f"База данных {test_db_name} создана.")

            # Инициализируем таблицы
            await snapshots_repo_for_init.ensure_table()
            await filenames_repo_for_init.ensure_table()
            test_logger.info("Таблицы успешно инициализированы.")
            
        yield # Тест выполняется здесь
        
    finally:
        test_logger.info(f"Начинаем очистку ClickHouse: удаляем базу данных {test_db_name}...")
        try:
            async with asyncio.timeout(30): # Таймаут для очистки
                # Сначала удаляем таблицы, чтобы избежать проблем с блокировками базы
                await snapshots_repo_for_init.drop_table()
                await filenames_repo_for_init.drop_table()
                test_logger.info("Таблицы удалены.")

                # После удаления таблиц, удаляем саму базу данных
                # Для удаления базы данных мы можем использовать временный клиент, не привязанный к конкретной базе
                temp_client_for_drop_db = ClickHouseRepository(
                    schema=OrderbookSnapshotModel, # Схема не имеет значения
                    table_name="temp_name", # Имя таблицы не имеет значения
                    db=test_db_name,
                    port=settings.clickhouse.port_http
                )
                try:
                    await temp_client_for_drop_db.drop_database() # Добавляем этот метод в ClickHouseRepository
                    test_logger.info(f"База данных {test_db_name} удалена.")
                except Exception as e:
                    test_logger.error(f"Ошибка при удалении базы данных {test_db_name}: {e}")
                finally:
                    await temp_client_for_drop_db.close() # Закрываем временный клиент для удаления базы


        except asyncio.TimeoutError:
            test_logger.error(f"Внимание: Таймаут при очистке базы данных {test_db_name}. Возможно, потребуется ручная очистка.")
        except Exception as e:
            test_logger.error(f"Ошибка во время очистки ClickHouse: {e}", exc_info=True)
        finally:
            # ОБЯЗАТЕЛЬНО закрываем все репозитории, созданные в фикстуре
            await snapshots_repo_for_init.close()
            await filenames_repo_for_init.close()
            
            test_logger.info("Очистка фикстуры завершена. Восстанавливаем настройки ClickHouse.")
            # Возвращаем оригинальные настройки
            settings.clickhouse.db_name = original_db_name
            settings.clickhouse.table_orderbook_snapshots = original_snapshots_table
            settings.clickhouse.table_orderbook_archive_filename = original_filenames_table

@pytest.mark.asyncio
async def test_full_archive_processing_flow(temp_datasets_dir, create_mock_zip_file, setup_clickhouse_for_archive_test):
    zip_full_path, zip_filename = create_mock_zip_file("test_orderbook.zip")

    processor = ArchiveProcessor()
    
    with patch("application.archive_processor.DeltaAnalyzerArchive") as MockDeltaAnalyzer:
        mock_analyzer = MockDeltaAnalyzer.return_value
        # Используем данные из импортированного файла
        mock_analyzer.archive_analyze.return_value = MOCK_ORDERBOOK_SNAPSHOT_DATA 
        
        async with asyncio.timeout(60):
            await processor.process_all_archives()

    # После завершения обработки, создаём новые репозитории для проверки данных.
    # Важно: эти репозитории также должны быть закрыты после использования!
    repo_snapshots = ClickHouseRepository(
        schema=OrderbookSnapshotModel,
        table_name=settings.clickhouse.table_orderbook_snapshots,
        db=settings.clickhouse.db_name,
        port=settings.clickhouse.port_http
    )
    repo_filenames = ClickHouseRepository(
        schema=OrderBookFilenameModel,
        table_name=settings.clickhouse.table_orderbook_archive_filename,
        db=settings.clickhouse.db_name,
        port=settings.clickhouse.port_http
    )
    try:
        saved_snapshots = await repo_snapshots.get_all()
        assert len(saved_snapshots) >= 2
        # Дополнительная проверка на содержимое
        assert any(s.s == "ETHUSDT" for s in saved_snapshots)

        processed_filenames = await repo_filenames.get_all()
        assert len(processed_filenames) == 1
        assert processed_filenames[0].filename == zip_filename

        processor.logger = MagicMock()
        async with asyncio.timeout(10):
            await processor.process_all_archives()
        processor.logger.info.assert_called_with(f"Файл {zip_filename} уже обработан, пропуск.")
    finally:
        await repo_snapshots.close()
        await repo_filenames.close()