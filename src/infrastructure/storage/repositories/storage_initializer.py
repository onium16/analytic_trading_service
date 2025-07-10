from copy import deepcopy
import sys
import logging

from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import (
    KlineRecord,
    KlineRecordDatetime,
    OrderBookDelta,
    OrderBookFilenameModel,
    OrderBookSnapshot,
    OrderbookSnapshotModel,
    TradeResult,
    TradeSignal,
)


class StorageInitializer:
    """
    Класс для инициализации хранилища ClickHouse, включая создание базы данных
    и всех необходимых таблиц.
    """
    def __init__(self, settings, logger: logging.Logger, client=None, test_net=False):
        """
        Инициализирует StorageInitializer.

        :param settings: Объект настроек (например, settings.clickhouse).
        :param logger: Объект логгера для вывода сообщений.
        :param client: Опциональный объект соединения ClickHouse.
                       Основное соединение будет установлено в initialisation_storage.
        :param test_net: Флаг, указывающий на использование тестовой сети.
        """
        self.settings = settings
        self.logger = logger
        self.client = client
        self.test_net = test_net

    async def create_database(self, db_name: str, client_conn):
        """
        Создает базу данных ClickHouse, если она не существует.

        :param db_name: Имя базы данных для создания.
        :param client_conn: Активное соединение с ClickHouse, используемое для выполнения запроса.
        """
        try:
            cur = client_conn.cursor()
            async with cur:
                await cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                self.logger.info(f"База данных ClickHouse '{db_name}' проверена/создана.")
        except Exception as e:
            self.logger.error(f"Ошибка при создании базы данных '{db_name}': {e}")
            raise

    async def initialize(self, tables):
        """
        Инициализирует переданные таблицы, проверяя их существование и создавая при необходимости.

        :param tables: Список кортежей, где каждый кортеж содержит (схема Pydantic, имя таблицы).
        """
        self.logger.info("Начинаем инициализацию таблиц ClickHouse...")

        if not self.client:
            raise RuntimeError("Соединение с ClickHouse не установлено. Вызовите initialisation_storage перед initialize.")

        for schema, table_name in tables:
            try:
                await self._ensure_table(schema, table_name)
            except Exception as e:
                self.logger.error(f"Ошибка при инициализации таблицы '{table_name}': {e}")
                raise

        self.logger.info("Все переданные таблицы ClickHouse успешно инициализированы.")

    async def initialisation_storage(self):
        """
        Осуществляет полную инициализацию хранилища ClickHouse, включая создание базы данных
        и всех необходимых таблиц.
        """
        self.logger.info("Запуск полной инициализации хранилища ClickHouse...")
        temp_client_for_db_creation = None # Инициализируем на None для finally блока
        try:
            orig_db_name = self.settings.clickhouse.db_name
            self.logger.info(f"Целевая база данных для инициализации: '{orig_db_name}'")

            tmp_settings = deepcopy(self.settings.clickhouse)
            tmp_settings.db_name = "default"

            self.logger.debug(f"Попытка подключения к 'default' базе данных для создания '{orig_db_name}'...")
            
            # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Ожидаем результат connect() напрямую ---
            temp_client_for_db_creation = await tmp_settings.connect()
            self.logger.debug("Соединение с 'default' установлено.")

            await self.create_database(orig_db_name, temp_client_for_db_creation)

            # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Пытаемся использовать .close() ---
            if temp_client_for_db_creation: # Проверяем, что клиент был создан
                await temp_client_for_db_creation.close() # <-- ИЗМЕНЕНО НА .close()
            self.logger.debug("Временное соединение с 'default' отключено.")

            self.logger.debug(f"Установка основного соединения с базой данных '{orig_db_name}'...")
            self.client = await self.settings.clickhouse.connect()
            self.logger.info(f"Основное соединение с '{orig_db_name}' установлено.")

            self.logger.info("Определение списка таблиц для инициализации...")
            tables_to_init = [
                (OrderbookSnapshotModel, self.settings.clickhouse.table_orderbook_snapshots),
                (OrderBookFilenameModel, self.settings.clickhouse.table_orderbook_archive_filename),
                (KlineRecord, self.settings.clickhouse.table_kline_archive),
                (KlineRecordDatetime, self.settings.clickhouse.table_kline_archive_datetime),
                (TradeSignal, self.settings.clickhouse.table_trade_signals),
                (TradeResult, self.settings.clickhouse.table_trade_results),
                (OrderBookDelta, self.settings.clickhouse.table_positions),
            ]

            await self.initialize(tables_to_init)
            self.logger.info("Полная инициализация хранилища ClickHouse успешно завершена.")

        except Exception as e:
            self.logger.error(f"Критическая ошибка инициализации базы данных: {str(e)}", exc_info=True)
            # Добавим попытку закрыть соединение даже при ошибке, если оно было установлено
            if temp_client_for_db_creation:
                try:
                    await temp_client_for_db_creation.close()
                except Exception as close_e:
                    self.logger.warning(f"Ошибка при попытке закрыть временное соединение после ошибки: {close_e}")
            sys.exit(1)

    async def _ensure_table(self, schema, table_name):
        """
        Гарантирует существование таблицы, используя ClickHouseRepository.
        Создает таблицу, если она не существует.

        :param schema: Схема Pydantic для таблицы.
        :param table_name: Имя таблицы.
        """
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Убираем client=self.client ---
        repo = ClickHouseRepository(
            schema=schema,
            db=self.settings.clickhouse.db_name,
            table_name=table_name
            # client=self.client # <-- УДАЛИТЬ ЭТУ СТРОКУ
        )
        await repo.ensure_table()
        self.logger.debug(f"Таблица ClickHouse '{table_name}' проверена/создана.")