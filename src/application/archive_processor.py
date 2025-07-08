import os
import pandas as pd
from tqdm.asyncio import tqdm as async_tqdm
from typing import List

from pydantic import ValidationError

from infrastructure.adapters._datasets_parser import ZipDataLoader
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import (
    OrderbookSnapshotModel,
    OrderBookFilenameModel,
    KlineRecord
)
from domain.delta_analyzer import DeltaAnalyzerArchive
from infrastructure.adapters.archive_kline_parser import KlineParser
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger
import logging


logger = setup_logger(__name__, level=settings.logger_level)


class ArchiveProcessor:
    """
    Класс для обработки архивных данных стакана и сохранения их в ClickHouse.
    """

    def __init__(self) -> None:
        """
        Инициализация настроек, репозиториев и переменных конфигурации.
        """
        self.logger: logging.Logger = logger
        self.settings = settings

        self.db: str = settings.clickhouse.db_name
        self.datasets_dir: str = settings.datasets_dir
        self.pair_tokens: str = settings.pair_tokens
        self.start_time: str = settings.start_time
        self.end_time: str = settings.end_time

        # Репозитории для хранения снимков стакана и имён обработанных архивов
        self.repo_snapshots: ClickHouseRepository = ClickHouseRepository(
            schema=OrderbookSnapshotModel,
            table_name=settings.clickhouse.table_orderbook_snapshots,
            db=self.db
        )
        self.repo_filenames: ClickHouseRepository = ClickHouseRepository(
            schema=OrderBookFilenameModel,
            table_name=settings.clickhouse.table_orderbook_archive_filename,
            db=self.db
        )

    async def process_all_archives(self) -> None:
        """
        Обрабатывает все архивные файлы из каталога datasets,
        исключая уже обработанные. Сохраняет результат в ClickHouse.
        """
        await self.repo_snapshots.ensure_table()
        await self.repo_filenames.ensure_table()

        files: List[str] = os.listdir(self.datasets_dir)

        if not files:
            self.logger.info("Нет файлов для обработки в папке datasets.")
            return

        processed_files = await self.repo_filenames.get_all()
        processed_names = {rec.filename for rec in processed_files}

        for filename in async_tqdm(files, desc="Обработка архивов", unit="файл"):
            if filename in processed_names:
                self.logger.info(f"Файл {filename} уже обработан, пропуск.")
                continue

            success: bool = await self.process_single_archive(filename)

            if success:
                self.logger.info(f"Файл {filename} успешно обработан.")

    async def process_single_archive(self, filename: str) -> bool:
        """
        Обрабатывает один архивный файл:
        - Загружает и анализирует данные
        - Сохраняет снимки стакана
        - Отмечает файл как обработанный
        - Загружает соответствующие Kline-данные

        Args:
            filename (str): Имя файла архива для обработки

        Returns:
            bool: Успешно ли завершилась обработка файла
        """
        self.logger.info(f"Обработка файла {filename}...")
        loader: ZipDataLoader = ZipDataLoader(data_dir=self.datasets_dir, filename=filename)
        df_raw: pd.DataFrame = loader.load_data()

        if df_raw.empty:
            self.logger.warning("DataFrame пуст — пропуск файла.")
            return False

        analyzer: DeltaAnalyzerArchive = DeltaAnalyzerArchive()
        df_full: pd.DataFrame = analyzer.archive_analyze(df_raw)

        if df_full.empty:
            self.logger.warning("Результирующий DataFrame пуст — пропуск файла.")
            return False

        records: List[OrderbookSnapshotModel] = []
        for row in df_full.to_dict(orient="records"):
            try:
                records.append(OrderbookSnapshotModel(**row))
            except ValidationError as ve:
                self.logger.error(f"Ошибка валидации строки: {str(ve)}")

        try:
            await self.repo_snapshots.save_batch(records)
            await self.repo_filenames.save_string_to_table(
                db=self.db,
                table=self.settings.clickhouse.table_orderbook_archive_filename,
                data_str=filename
            )
        except Exception as e:
            self.logger.error(f"Ошибка сохранения в БД: {str(e)}")
            return False

        return True
