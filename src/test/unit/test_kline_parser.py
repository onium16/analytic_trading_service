from datetime import datetime, timezone
import pytest
import pandas as pd
import json
from unittest.mock import AsyncMock, MagicMock, patch
from infrastructure.adapters.archive_kline_parser import KlineParser
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.config.settings import settings
from infrastructure.storage.schemas import KlineRecord, KlineRecordDatetime
import requests


@pytest.fixture
def mock_kline_repo():
    repo = AsyncMock(spec=ClickHouseRepository)
    repo.schema = KlineRecord
    return repo

@pytest.fixture
def mock_kline_datetime_repo():
    repo = AsyncMock(spec=ClickHouseRepository)
    repo.schema = KlineRecord
    repo.get_all.return_value = []
    return repo

@pytest.fixture
def kline_parser(mock_kline_repo, mock_kline_datetime_repo):
    with patch('infrastructure.config.settings.settings.binance.kline_url', 'https://testnet.binance.vision/api/v3/klines'):
        parser = KlineParser(
            repository=mock_kline_repo,
            symbol="ETHUSDT",
            interval="1" 
        )
        parser.repo_save_date = mock_kline_datetime_repo
        return parser

@pytest.fixture
def mock_requests_get():
    with patch('requests.get') as mock_get:
        yield mock_get

def test_daterange():
    parser = KlineParser(None, "SYM", "1m")
    start = datetime(2025, 1, 1)
    end = datetime(2025, 1, 3)
    dates = parser.daterange(start, end)
    assert len(dates) == 3
    assert dates[0] == datetime(2025, 1, 1)
    assert dates[1] == datetime(2025, 1, 2)
    assert dates[2] == datetime(2025, 1, 3)

@pytest.mark.asyncio
async def test_is_date_already_processed(kline_parser, mock_kline_datetime_repo):
    
    mock_kline_datetime_repo.get_all.return_value = []
    assert not await kline_parser.is_date_already_processed("2025-07-01")

    processed_record = KlineRecordDatetime(
        exchange=settings.binance.name,
        symbol=kline_parser.symbol,
        data=json.dumps({"date": "2025-07-01", "interval": kline_parser.interval}),
        data_processed=datetime.now(timezone.utc),
        interval=kline_parser.interval
    )
    mock_kline_datetime_repo.get_all.return_value = [processed_record]
    assert await kline_parser.is_date_already_processed("2025-07-01")

    
    other_interval = "5m" if kline_parser.interval != "5m" else "1m"
    processed_record_other = KlineRecordDatetime(
        exchange=settings.binance.name,
        symbol=kline_parser.symbol,
        data=json.dumps({"date": "2025-07-01", "interval": other_interval}),
        data_processed=datetime.now(timezone.utc),
        interval=other_interval
    )
    mock_kline_datetime_repo.get_all.return_value = [processed_record_other]
    assert not await kline_parser.is_date_already_processed("2025-07-01")

@pytest.mark.asyncio
async def test_mark_date_processed(kline_parser, mock_kline_datetime_repo):
    await kline_parser.mark_date_processed("2025-07-01")
    mock_kline_datetime_repo.ensure_table.assert_called_once()
    mock_kline_datetime_repo.save_batch.assert_called_once()
    saved_record = mock_kline_datetime_repo.save_batch.call_args[0][0][0]
    assert saved_record.symbol == "ETHUSDT"
    
    expected_data_part = f'"date": "2025-07-01", "interval": "{kline_parser.interval}"'
    assert expected_data_part in saved_record.data
    assert saved_record.interval == "1m" 

def test_parse_date_yyyy_mm_dd(kline_parser):
    date = kline_parser.parse_date("2025-01-15")
    assert date == datetime(2025, 1, 15)

def test_parse_date_dd_mm_yyyy(kline_parser):
    date = kline_parser.parse_date("15-01-2025")
    assert date == datetime(2025, 1, 15)

def test_parse_date_unsupported_format(kline_parser):
    with pytest.raises(ValueError, match="Unsupported date format: 2025/01/15"):
        kline_parser.parse_date("2025/01/15")

def test_get_binance_ohlcv_day_empty_response(mock_requests_get, kline_parser):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_requests_get.return_value = mock_response

    df = kline_parser.get_binance_ohlcv_day(symbol="ETHUSDT", date_str="2023-03-15", interval="1m")

    assert df.empty
    mock_requests_get.assert_called_once()

def test_get_binance_ohlcv_day_api_error(mock_requests_get, kline_parser):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Internal Server Error")
    mock_requests_get.return_value = mock_response

    with pytest.raises(requests.exceptions.RequestException):
        kline_parser.get_binance_ohlcv_day(symbol="ETHUSDT", date_str="2023-03-15", interval="1m")