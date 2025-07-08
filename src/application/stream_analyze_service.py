# src/application/streaming_delta_service.py

import pandas as pd
from domain.delta_analyzer import DeltaAnalyzerArchive
from infrastructure.adapters.bybit_api_client import BybitClient

from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__, level=settings.logger_level)


class StreamingDeltaService:
    def __init__(self, delta_analyzer: DeltaAnalyzerArchive):
        self.delta_analyzer = delta_analyzer

    async def fetch_and_analyze(self, symbol: str, snapshots_count: int = 10, interval: float = 1.0) -> pd.DataFrame:
        """
        Получаем несколько снимков (snapshots_count) ордербука с биржи и анализируем их дельты за интервал (interval) секунд.
        Возвращаем DataFrame с результатами.
        """
        async with BybitClient() as client:
            logger.info(f"Запрашиваем {snapshots_count} снимков ордербука для {symbol}...")
            snapshots = await client.fetch_multiple_snapshots(symbol, snapshots_count, interval)

        rows = []
        for snap in snapshots:
            bids = snap.get('b', [])
            asks = snap.get('a', [])
            # Время в миллисекундах в формате int
            timestamp_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
            rows.append({'b': bids, 'a': asks, 'ts': timestamp_ms, 's': symbol})

        df_raw = pd.DataFrame(rows)
        logger.info(f"Получено {len(df_raw)} снимков, начинаем анализ...")

        # Анализируем
        df_analyzed = self.delta_analyzer.archive_analyze(df_raw)
        logger.info(f"Анализ завершён, строк: {len(df_analyzed)}")

        return df_analyzed

