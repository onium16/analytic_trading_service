import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

# Ensure pytest-asyncio is installed and imported for async fixtures
# pip install pytest-asyncio
import pytest_asyncio # <--- ADDED/CONFIRMED IMPORT

from application.backtest_runner import BacktestRunner
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord
from infrastructure.storage.repositories.storage_initializer import StorageInitializer
from infrastructure.config.settings import settings
from backtesting import Backtest, Strategy

# --- Dummy Strategy for Backtesting.py ---
class DummyStrategy(Strategy):
    def init(self):
        # Using self.I for indicators. Let's make them return something plausible
        # or remove if not strictly needed for this basic test.
        # For a simple test, we can just ensure they are callable.
        self.set_entry_price = self.I(lambda: self.data.Close[0]) # Example: entry at first close
        self.set_exit_price = self.I(lambda: self.data.Close[0] * 1.05) # Example: exit at 5% profit

    def next(self):
        # Simplified logic: buy if trending up, sell if trending down
        if self.data.Close[-1] > self.data.Open[-1] and not self.position:
            self.buy()
        elif self.data.Close[-1] < self.data.Open[-1] and not self.position:
            self.sell()

# --- ClickHouse Setup Fixture for Backtest Data ---
# !!! IMPORTANT: Added @pytest_asyncio.fixture for async fixtures !!!
@pytest_asyncio.fixture(scope="module")
async def setup_clickhouse_for_backtest_data():
    test_db_name = f"test_backtest_db_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Store original settings to restore them after the test module runs
    original_db_name = settings.clickhouse.db_name
    original_kline_table = settings.clickhouse.table_kline_archive

    # Temporarily modify settings for the test database and table
    settings.clickhouse.db_name = test_db_name
    settings.clickhouse.table_kline_archive = "kline_for_backtest" # Use a distinct table name for test Klines

    client = None # Initialize client to None for proper cleanup
    kline_repo = None # Initialize kline_repo to None for proper cleanup

    try:
        # Establish ClickHouse connection
        # Ensure settings.clickhouse.connect() is an async method
        client = await settings.clickhouse.connect() 
        initializer = StorageInitializer(settings, MagicMock(), client)
        
        # Create database and initialize tables
        # Assume create_database and initialize methods on StorageInitializer are async
        try:
            await initializer.create_database(test_db_name)
            await initializer.initialize([
                (KlineRecord, settings.clickhouse.table_kline_archive)
            ])

            # Populate test Kline data
            kline_repo = ClickHouseRepository(
                schema=KlineRecord,
                db=settings.clickhouse.db_name,
                table_name=settings.clickhouse.table_kline_archive,
                port=settings.clickhouse.port # <--- ADDED: Explicitly pass the port
            )
            test_kline_data = []
            for i in range(100): # Generate 100 sample candles
                # Ensure timestamps are UTC if ClickHouse stores them as such
                timestamp = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=i)
                test_kline_data.append(KlineRecord(
                    symbol="ETHUSDT",
                    timestamp=timestamp,
                    open=100.0 + i,
                    high=101.0 + i,
                    low=99.0 + i,
                    close=100.5 + i + (0.5 if i % 2 == 0 else -0.5), # Simulate price movement
                    volume=10.0 + i,
                    interval="1m"
                ))
            await kline_repo.save_batch(test_kline_data)

            yield # Hand control to the test function
    finally:
        # --- Cleanup after tests ---
        # Close the repository connection if it was opened
        if kline_repo:
            await kline_repo.close() 

        await client.execute(f"DROP DATABASE IF EXISTS {test_db_name}") # <--- This client is used
        await client.close() # <--- This client is closed

        # Drop the test database using a temporary repository with the drop_database method
        # This is robust if you've already implemented `drop_database` in ClickHouseRepository
        temp_cleanup_repo = ClickHouseRepository(
            schema=KlineRecord, # Schema doesn't matter for drop_database
            table_name="dummy_table_for_cleanup", # Table name doesn't matter
            db=test_db_name,
            port=settings.clickhouse.port
        )
        try:
            await temp_cleanup_repo.drop_database() # <--- Using your custom drop_database
        except Exception as e:
            # Log the error but don't re-raise, as cleanup should ideally complete
            print(f"Error dropping database {test_db_name}: {e}")
        finally:
            # Ensure the temporary cleanup client is closed
            await temp_cleanup_repo.close() 
            
        # Restore original ClickHouse settings
        settings.clickhouse.db_name = original_db_name
        settings.clickhouse.table_kline_archive = original_kline_table

# --- Test Function for BacktestRunner ---
@pytest.mark.asyncio
async def test_backtest_runner_loads_from_db_and_runs(setup_clickhouse_for_backtest_data):
    # Patch the `strategies_to_test` attribute of the BacktestRunner class
    # to ensure our DummyStrategy is used.
    with patch('src.application.backtest_runner.BacktestRunner.strategies_to_test', [DummyStrategy]):
        runner = BacktestRunner(
            archive_mode=True, # Use archive mode to load from DB
            stream_mode=False,
            archive_source=True,
            stream_source=False,
            strategy_params={} # Empty params, as DummyStrategy doesn't use them
        )
        
        # Mock Backtest.run() to prevent actual backtest computation and speed up the test
        # The patch path here should reflect where `Backtest` is imported in `BacktestRunner`.
        # Assuming `from backtesting import Backtest` is used in `application/backtest_runner.py`
        with patch('backtesting.Backtest.run') as mock_backtest_run:
            # Backtesting.py's `run()` typically returns a pandas Series of results.
            # Provide a mock return value that includes common elements.
            mock_backtest_run.return_value = pd.Series({
                '_trades': MagicMock(), 
                'Equity Curve': [1000.0, 1010.0, 1005.0], 
                'Return [%]': 0.5,
                'Start': pd.Timestamp('2025-01-01', tz='UTC'), # Add sensible mock data
                'End': pd.Timestamp('2025-01-01 01:39:00', tz='UTC')
            }) 
            
            # Execute the backtest
            await runner.run_backtest()

            # Assert that Backtest.run was called, confirming data preparation and strategy execution logic
            mock_backtest_run.assert_called_once()
            
            # --- Further assertions (optional but recommended) ---
            # You might want to assert that the correct data was passed to Backtest.run().
            # This would require inspecting the arguments passed to mock_backtest_run.
            # Example (conceptual):
            # args, kwargs = mock_backtest_run.call_args
            # passed_data = args[0] # Assuming data is the first positional argument
            # assert isinstance(passed_data, pd.DataFrame)
            # assert not passed_data.empty
            # assert len(passed_data) == 100 # Check if all 100 candles were loaded
            # assert 'Open' in passed_data.columns and 'Close' in passed_data.columns