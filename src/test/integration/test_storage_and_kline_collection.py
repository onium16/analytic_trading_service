import time
import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from infrastructure.adapters.archive_kline_parser import KlineParser
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord, KlineRecordDatetime
from infrastructure.config.settings import settings
import pytest_asyncio
import requests
import requests_mock
import json
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__)

@pytest_asyncio.fixture
async def mock_repository():
    repo = MagicMock(spec=ClickHouseRepository)
    repo.schema = KlineRecord
    repo.db = settings.clickhouse.db_name
    repo.table_name = settings.clickhouse.table_kline_archive
    repo.database_exists = AsyncMock(return_value=True)
    repo.create_database = AsyncMock()
    repo.ensure_table = AsyncMock()
    repo.save_batch = AsyncMock()
    repo.get_all = AsyncMock(return_value=[])
    return repo

@pytest_asyncio.fixture
async def mock_save_date_repository():
    repo = MagicMock(spec=ClickHouseRepository)
    repo.schema = KlineRecordDatetime
    repo.db = settings.clickhouse.db_name
    repo.table_name = settings.clickhouse.table_kline_archive_datetime
    repo.ensure_table = AsyncMock()
    repo.save_batch = AsyncMock()
    repo.get_all = AsyncMock(return_value=[])
    return repo

@pytest_asyncio.fixture
async def kline_parser(mock_repository):
    parser = KlineParser(repository=mock_repository, symbol="BTCUSDT", interval="1m")
    return parser

@pytest.mark.parametrize("date_str, expected", [
    ("2025-06-01", True),
    ("01-06-2025", True),
    ("1735689600", True),
    ("1735689600000", True),
    ("invalid", False),
])
def test_is_supported_date_format(kline_parser, date_str, expected):
    # Act
    result = kline_parser.is_supported_date_format(date_str)
    
    # Assert
    assert result == expected

@pytest.mark.parametrize("date_str, expected", [
    ("2025-06-01", datetime(2025, 6, 1)),
    ("01-06-2025", datetime(2025, 6, 1)),
    ("1735689600", datetime(2025, 1, 1, 0, 0, 0)),
    ("1735689600000", datetime(2025, 1, 1, 0, 0, 0)),
])
def test_parse_date(kline_parser, date_str, expected):
    # Act
    result = kline_parser.parse_date(date_str)
    
    # Assert
    assert result == expected
    assert result.tzinfo is None  # Ensure naive datetime

@pytest.mark.asyncio
async def test_is_date_already_processed_not_processed(kline_parser, mock_save_date_repository):
    # Arrange
    kline_parser.repo_save_date = mock_save_date_repository
    kline_parser.interval = "1m"
    mock_save_date_repository.get_all.return_value = []
    
    # Act
    result = await kline_parser.is_date_already_processed("2025-06-01")
    
    # Assert
    assert result is False
    mock_save_date_repository.get_all.assert_awaited_once()

@pytest.mark.asyncio
async def test_is_date_already_processed_already_processed(kline_parser, mock_save_date_repository):
    # Arrange
    kline_parser.repo_save_date = mock_save_date_repository
    kline_parser.interval = "1m"
    mock_save_date_repository.get_all.return_value = [
        {"data": json.dumps({"date": "2025-06-01", "interval": "1m"})}
    ]
    
    # Act
    result = await kline_parser.is_date_already_processed("2025-06-01")
    
    # Assert
    assert result is True
    mock_save_date_repository.get_all.assert_awaited_once()

@pytest.mark.asyncio
async def test_mark_date_processed(kline_parser, mock_save_date_repository):
    # Arrange
    kline_parser.repo_save_date = mock_save_date_repository
    kline_parser.symbol = "BTCUSDT"
    kline_parser.interval = "1m"
    settings.binance.name = "binance"
    
    # Act
    await kline_parser.mark_date_processed("2025-06-01")
    
    # Assert
    mock_save_date_repository.ensure_table.assert_awaited_once()
    mock_save_date_repository.save_batch.assert_awaited_once()
    call_args = mock_save_date_repository.save_batch.call_args[0][0][0]
    assert call_args.exchange == "binance"
    assert call_args.symbol == "BTCUSDT"
    assert json.loads(call_args.data) == {"date": "2025-06-01", "interval": "1m"}
    assert call_args.interval == "1m"


@staticmethod
def get_binance_ohlcv_day(symbol: str = 'ETHUSDT', date_str: str = '2025-06-01', interval: str = settings.kline.interval) -> pd.DataFrame:
    url = settings.binance.kline_url
    date = datetime.strptime(date_str, "%Y-%m-%d")
    current_time = int(date.timestamp() * 1000)
    end_time = int((date + timedelta(days=1)).timestamp() * 1000)

    all_data = []

    while current_time < end_time:
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': 1000,
            'startTime': current_time,
            'endTime': end_time
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса к Binance API: {str(e)}")
            raise

        raw_data = response.json()
        if not raw_data:
            break

        all_data.extend(raw_data)
        current_time = raw_data[-1][0] + 60_000  # сдвигаемся на одну минуту вперед
        time.sleep(0.1)

    df = pd.DataFrame(all_data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_vol', 'taker_buy_quote_vol', 'ignore'
    ])

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    df[numeric_cols] = df[numeric_cols].astype(float)

    return df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']]


@pytest.mark.asyncio
async def test_fetch_and_save_day_success(kline_parser, mock_repository, mock_save_date_repository):
    # Arrange
    kline_parser.repo = mock_repository
    kline_parser.repo_save_date = mock_save_date_repository
    kline_parser.symbol = "BTCUSDT"
    kline_parser.interval = "1m"
    settings.binance.kline_url = "https://api.binance.com/api/v3/klines"
    settings.binance.name = "binance"

    mock_data = [
        [1625097600000, "50000", "51000", "49000", "50500", "1000", 1625097660000, "5000000", 100, "500", "2500000", "0"],
    ]
    
    with requests_mock.Mocker() as m:
        # Return mock_data for the first request, then [] for subsequent requests to exit the loop
        m.get(
            settings.binance.kline_url,
            [
                {"json": mock_data, "status_code": 200},
                {"json": [], "status_code": 200},  # Empty response to break the loop
            ]
        )
        with patch.object(kline_parser, "is_date_already_processed", AsyncMock(return_value=False)):
            with patch.object(kline_parser, "mark_date_processed", AsyncMock()):
                # Act
                await kline_parser.fetch_and_save_day(datetime(2021, 7, 1))
                
                # Assert
                kline_parser.is_date_already_processed.assert_awaited_once_with("2021-07-01")
                mock_repository.save_batch.assert_awaited_once()
                kline_parser.mark_date_processed.assert_awaited_once_with("2021-07-01")
                
                saved_records = mock_repository.save_batch.call_args[0][0]
                assert len(saved_records) == 1
                assert saved_records[0].symbol == "BTCUSDT"
                assert saved_records[0].interval == "1m"
                assert saved_records[0].open == 50000.0

@pytest.mark.asyncio
async def test_fetch_and_save_day_already_processed(kline_parser, mock_repository, mock_save_date_repository):
    # Arrange
    kline_parser.repo = mock_repository
    kline_parser.repo_save_date = mock_save_date_repository
    kline_parser.symbol = "BTCUSDT"
    kline_parser.interval = "1m"
    settings.binance.name = "binance"

    # Act
    await kline_parser.mark_date_processed("2025-06-01")

    # Assert
    mock_save_date_repository.ensure_table.assert_awaited_once()
    mock_save_date_repository.save_batch.assert_awaited_once()
    call_args = mock_save_date_repository.save_batch.call_args[0][0][0]
    assert call_args.exchange == "binance"
    assert call_args.symbol == "BTCUSDT"
    assert json.loads(call_args.data) == {"date": "2025-06-01", "interval": "1m"}
    assert call_args.interval == "1m"

@pytest.mark.asyncio
async def test_collect_kline_range(kline_parser, mock_repository, mock_save_date_repository):
    # Arrange
    kline_parser.repo = mock_repository
    kline_parser.repo_save_date = mock_save_date_repository
    kline_parser.symbol = "BTCUSDT"
    kline_parser.interval = "1m"
    settings.binance.kline_url = "https://api.binance.com/api/v3/klines"
    settings.binance.name = "binance"
    
    mock_data = [
        [1743984000000, "50000", "51000", "49000", "50500", "1000", 1743984060000, "5000000", 100, "500", "2500000", "0"],
    ]
    with requests_mock.Mocker() as m:
        # Return mock_data for each day to ensure save_batch is called twice
        m.get(
            settings.binance.kline_url,
            [
                {"json": mock_data, "status_code": 200},  # First day (2025-07-01)
                {"json": [], "status_code": 200},         # End of data for first day
                {"json": mock_data, "status_code": 200},  # Second day (2025-07-02)
                {"json": [], "status_code": 200},         # End of data for second day
            ]
        )
        with patch.object(kline_parser, "is_date_already_processed", AsyncMock(return_value=False)):
            with patch.object(kline_parser, "mark_date_processed", AsyncMock()):
                # Act
                await kline_parser.collect_kline_range("01-07-2025", "02-07-2025")
                
                # Assert
                assert kline_parser.is_date_already_processed.await_count == 2
                assert mock_repository.save_batch.await_count == 2
                assert kline_parser.mark_date_processed.await_count == 2
                kline_parser.is_date_already_processed.assert_any_await("2025-07-01")
                kline_parser.is_date_already_processed.assert_any_await("2025-07-02")