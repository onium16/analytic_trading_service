import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from application.backtest_runner import BacktestRunner
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord
from infrastructure.config.settings import settings
from backtesting import Backtest, Strategy # Убедимся, что они импортируются для теста

# Пример простейшей стратегии для Backtesting.py
class DummyStrategy(Strategy):
    def init(self):
        self.set_entry_price = self.I(lambda: 0)
        self.set_exit_price = self.I(lambda: 0)

    def next(self):
        if self.data.Close[-1] > self.data.Open[-1]:
            self.buy()
        elif self.data.Close[-1] < self.data.Open[-1]:
            self.sell()

# Фикстура для настройки ClickHouse и заполнения его данными для бэктеста
@pytest.fixture(scope="module")
async def setup_clickhouse_for_backtest_data():
    test_db_name = f"test_backtest_db_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    original_db_name = settings.clickhouse.db_name
    original_kline_table = settings.clickhouse.table_kline_archive

    settings.clickhouse.db_name = test_db_name
    settings.clickhouse.table_kline_archive = "kline_for_backtest"

    client = await settings.clickhouse.connect()
    initializer = StorageInitializer(settings, MagicMock(), client)
    
    try:
        await initializer.create_database(test_db_name)
        await initializer.initialize([
            (KlineRecord, settings.clickhouse.table_kline_archive)
        ])

        # Заполняем тестовыми Kline данными
        kline_repo = ClickHouseRepository(
            schema=KlineRecord,
            db=settings.clickhouse.db_name,
            table_name=settings.clickhouse.table_kline_archive
        )
        test_kline_data = []
        for i in range(100): # 100 свечей
            timestamp = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=i)
            test_kline_data.append(KlineRecord(
                symbol="ETHUSDT",
                timestamp=timestamp,
                open=100.0 + i,
                high=101.0 + i,
                low=99.0 + i,
                close=100.5 + i + (0.5 if i % 2 == 0 else -0.5), # Для имитации движения
                volume=10.0 + i,
                interval="1m"
            ))
        await kline_repo.save_batch(test_kline_data)

        yield # Выполнение тестов
    finally:
        await client.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        await client.close()
        settings.clickhouse.db_name = original_db_name
        settings.clickhouse.table_kline_archive = original_kline_table

@pytest.mark.asyncio
async def test_backtest_runner_loads_from_db_and_runs(setup_clickhouse_for_backtest_data):
    # Мокируем стратегии, чтобы использовать DummyStrategy
    with patch('application.backtest_runner.BacktestRunner.strategies_to_test', [DummyStrategy]):
        # Мокируем функцию prepare_backtest_data, чтобы она возвращала наш DataFrame
        # В реальном интеграционном тесте prepare_backtest_data будет взаимодействовать с базой
        # Но для этого теста мы убедимся, что он возвращает данные в правильном формате.
        # В setup_clickhouse_for_backtest_data мы УЖЕ заполнили базу,
        # так что prepare_backtest_data должен их сам вытянуть.
        
        runner = BacktestRunner(
            archive_mode=True, # Используем архивный режим для загрузки из БД
            stream_mode=False,
            archive_source=True,
            stream_source=False,
            strategy_params={} # Пустые параметры, так как DummyStrategy их не использует
        )
        
        # Мокируем вывод Backtest.run()
        with patch('backtesting.Backtest.run') as mock_backtest_run:
            mock_backtest_run.return_value = pd.Series({'_trades': MagicMock(), 'Equity Curve': [1000, 1010, 1005]}) # Мок результатов
            
            # Запускаем бэктест
            await runner.run_backtest()

            # Проверяем, что Backtest.run был вызван (индикация успешной подготовки данных)
            mock_backtest_run.assert_called_once()
            
            # Проверяем, что были вызваны методы для получения данных из репозитория
            # (Это уже происходит внутри prepare_backtest_data)
            # Для более глубокой проверки можно мокнуть ClickHouseRepository.get_data_by_time_range
            
            # Проверяем, что логирование об успешном бэктесте произошло
            # (предполагая, что BacktestRunner логирует это)
            # runner.logger.info.assert_any_call("Запуск оценки стратегий... По архивным данным. Обновление параметров стратегий.")
            # runner.logger.info.assert_any_call(f"Бэктест завершен для {DummyStrategy.__name__}.")