import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.adapters.archive_kline_parser import KlineParser
from infrastructure.config.settings import settings
from infrastructure.storage.schemas import KlineRecord, KlineRecordDatetime
import requests # Для мокирования реальных запросов

# Фикстура для временной настройки ClickHouse для тестов
# Это должно быть выполнено один раз для всех интеграционных тестов
# или для каждого теста, если вы хотите изоляцию.
# Для простоты, здесь предполагается, что ClickHouse доступен локально.
@pytest.fixture(scope="module")
async def setup_clickhouse_for_integration_tests():
    # Настраиваем ClickHouse для тестов
    test_db_name = f"test_db_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    original_db_name = settings.clickhouse.db_name
    original_kline_table = settings.clickhouse.table_kline_archive
    original_kline_datetime_table = settings.clickhouse.table_kline_archive_datetime

    settings.clickhouse.db_name = test_db_name
    settings.clickhouse.table_kline_archive = "kline_archive_test"
    settings.clickhouse.table_kline_archive_datetime = "kline_datetime_test"

    client = await settings.clickhouse.connect()
    initializer = StorageInitializer(settings, MagicMock(), client)
    
    try:
        await initializer.create_database(test_db_name)
        await initializer.initialize([
            (KlineRecord, settings.clickhouse.table_kline_archive),
            (KlineRecordDatetime, settings.clickhouse.table_kline_archive_datetime)
        ])
        yield # Выполнение тестов
    finally:
        # Очистка: удалить тестовую базу данных
        await client.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        await client.close()
        # Восстанавливаем оригинальные настройки
        settings.clickhouse.db_name = original_db_name
        settings.clickhouse.table_kline_archive = original_kline_table
        settings.clickhouse.table_kline_archive_datetime = original_kline_datetime_table

@pytest.mark.asyncio
async def test_kline_collection_and_storage(setup_clickhouse_for_integration_tests):
    # Мокаем внешний API Binance, чтобы не зависеть от сети
    with patch('requests.get') as mock_requests_get:
        mock_response_data = [
            [1719772800000, "100.0", "101.0", "99.0", "100.5", "10.0", 1719772859999, "...", 10, "...", "...", "0"]
        ] # Пример данных для 2024-07-01 00:00:00 UTC
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_requests_get.return_value = mock_response

        # Инициализируем репозитории с тестовыми настройками
        kline_repo = ClickHouseRepository(
            schema=KlineRecord,
            db=settings.clickhouse.db_name,
            table_name=settings.clickhouse.table_kline_archive
        )
        
        parser = KlineParser(
            repository=kline_repo,
            symbol=settings.pair_tokens, # Используем реальные настройки проекта
            interval=settings.kline.interval
        )

        start_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Проверяем, что дата уже не обработана перед запуском
        parser.repo_save_date = ClickHouseRepository(
            KlineRecordDatetime,
            db=settings.clickhouse.db_name,
            table_name=settings.clickhouse.table_kline_archive_datetime
        )
        await parser.repo_save_date.ensure_table()
        
        # Запускаем сбор данных
        await parser.collect_kline_range(start_date, end_date)

        # Проверяем, что данные были сохранены
        saved_kline_records = await kline_repo.get_all()
        assert len(saved_kline_records) >= 1 # Ожидаем, что хотя бы одна запись будет сохранена

        # Проверяем, что метка об обработке даты была сохранена
        processed_dates = await parser.repo_save_date.get_all()
        assert len(processed_dates) >= 1
        assert any(start_date in record.data for record in processed_dates)

        # Дополнительная проверка содержимого сохраненной записи
        first_record = saved_kline_records[0]
        assert first_record.symbol == settings.pair_tokens
        assert first_record.open == 100.0