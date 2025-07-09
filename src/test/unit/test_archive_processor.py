import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
import os

from application.archive_processor import ArchiveProcessor
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import OrderbookSnapshotModel, OrderBookFilenameModel
from infrastructure.config.settings import settings

@pytest.fixture
def mock_repos():
    mock_repo_snapshots = AsyncMock(spec=ClickHouseRepository)
    mock_repo_filenames = AsyncMock(spec=ClickHouseRepository)
    mock_repo_snapshots.schema = OrderbookSnapshotModel
    mock_repo_filenames.schema = OrderBookFilenameModel
    return mock_repo_snapshots, mock_repo_filenames

@pytest.fixture
def archive_processor(mock_repos):
    mock_repo_snapshots, mock_repo_filenames = mock_repos
    processor = ArchiveProcessor()
    processor.repo_snapshots = mock_repo_snapshots
    processor.repo_filenames = mock_repo_filenames
    processor.logger = MagicMock()
    with patch('os.path.exists', return_value=True):
        yield processor

@pytest.mark.asyncio
async def test_process_single_archive_success(archive_processor, mock_repos):
    mock_repo_snapshots, mock_repo_filenames = mock_repos
    mock_df_raw = pd.DataFrame({
        'bid_price': [100.0], 'bid_volume': [100], 'ask_price': [100.1], 'ask_volume': [50],
        'local_timestamp': [pd.Timestamp.now()]
    })

    mock_df_full = pd.DataFrame({
        'symbol': ['ETHUSDT'], 'total_delta': [50], 'timestamp': [pd.Timestamp.now()],
        'bid_price': [100.0], 'bid_volume': [100], 'ask_price': [100.1], 'ask_volume': [50],
        'price': [100.05], 'local_timestamp': [pd.Timestamp.now()], 'server_timestamp': [pd.Timestamp.now()],
        'ask_size': [1], 'bid_size': [1], 'current_total_delta': [50], 'delta_total_delta': [0],
        'top_10_bid_volume_ratio': [0.5], 'top_10_ask_volume_ratio': [0.5],
        'cv_bid_volume': [0.1], 'cv_ask_volume': [0.1], 'delta_top_10_volume_ratio': [0],
        'kline_open': [100], 'kline_high': [101], 'kline_low': [99], 'kline_close': [100.05], 'kline_volume': [10],
        'kline_delta_volume': [0], 'kline_trade_count': [10],
        'topic': ['orderbook.50.ETHUSDT'],
        'type': ['snapshot'],
        'ts': [1234567890123],
        'cts': [pd.Timestamp.now().value],
        's': ['ETHUSDT'],
        'u': [1],
        'seq': [1],
        'uuid': [123]
    })

    with patch('application.archive_processor.ZipDataLoader') as MockZipDataLoader:
        MockZipDataLoader.return_value.load_data.return_value = mock_df_raw
        with patch('application.archive_processor.DeltaAnalyzerArchive') as MockDeltaAnalyzer:
            MockDeltaAnalyzer.return_value.archive_analyze.return_value = mock_df_full
            result = await archive_processor.process_single_archive("valid.zip")
            assert result
            mock_repo_snapshots.save_batch.assert_called_once_with(
                [OrderbookSnapshotModel(**row) for row in mock_df_full.to_dict(orient="records")]
            )
            mock_repo_filenames.save_string_to_table.assert_called_once_with(
                db=archive_processor.db,
                table=settings.clickhouse.table_orderbook_archive_filename,
                data_str="valid.zip"
            )
            
            archive_processor.logger.info.assert_called_with("Обработка файла valid.zip...")
    
@pytest.mark.asyncio
async def test_process_single_archive_empty_dataframe(archive_processor):
    with patch('application.archive_processor.ZipDataLoader') as MockZipDataLoader:
        MockZipDataLoader.return_value.load_data.return_value = pd.DataFrame()
        result = await archive_processor.process_single_archive("empty.zip")
        assert not result
        archive_processor.logger.warning.assert_called_with("DataFrame пуст — пропуск файла.")
        archive_processor.repo_snapshots.save_batch.assert_not_called()

@pytest.mark.asyncio
async def test_process_all_archives_no_files(archive_processor):
    with patch('application.archive_processor.os.listdir', return_value=[]):
        await archive_processor.process_all_archives()
        archive_processor.logger.info.assert_called_with("Нет файлов для обработки в папке datasets.")


@pytest.mark.asyncio
async def test_process_all_archives_existing_files_processed(archive_processor, mock_repos):
    mock_repo_snapshots, mock_repo_filenames = mock_repos
    mock_repo_filenames.get_all.return_value = [
        OrderBookFilenameModel(filename="test_file_1.zip", data_processed=pd.Timestamp.now())
    ]
    with patch('application.archive_processor.os.listdir', return_value=["test_file_1.zip", "test_file_2.zip"]):
        with patch.object(archive_processor, 'process_single_archive', AsyncMock(return_value=True)) as mock_process_single:
            await archive_processor.process_all_archives()
            archive_processor.logger.info.assert_any_call("Файл test_file_1.zip уже обработан, пропуск.")
            mock_process_single.assert_called_once_with("test_file_2.zip")

@pytest.mark.asyncio
async def test_process_single_archive_empty_analyzed_dataframe(archive_processor):
    with patch('application.archive_processor.ZipDataLoader') as MockZipDataLoader:
        MockZipDataLoader.return_value.load_data.return_value = pd.DataFrame({'col1': [1]})
        with patch('application.archive_processor.DeltaAnalyzerArchive') as MockDeltaAnalyzer:
            MockDeltaAnalyzer.return_value.archive_analyze.return_value = pd.DataFrame()
            result = await archive_processor.process_single_archive("empty_analyzed.zip")
            assert not result
            archive_processor.logger.warning.assert_called_with("Результирующий DataFrame пуст — пропуск файла.")
            archive_processor.repo_snapshots.save_batch.assert_not_called()