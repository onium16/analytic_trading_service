from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository


class StorageInitializer:
    def __init__(self, settings, logger, client):
        self.settings = settings
        self.logger = logger
        self.client = client

    async def create_database(self, db_name: str):
        cur = await self.client.cursor()
        async with cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            self.logger.info(f"Инициализация базы данных {db_name} ClickHouse...")

    async def initialize(self, tables):
        """
        Инициализация переданных таблиц.
        :param tables: список кортежей (schema, table_name)
        """
        self.logger.info("Инициализация хранилища ClickHouse...")

        for schema, table_name in tables:
            try:
                await self._ensure_table(schema, table_name)
            except Exception as e:
                self.logger.error(f"Ошибка при инициализации таблицы {table_name}: {e}")
                raise

        self.logger.info("Все переданные таблицы ClickHouse успешно инициализированы.")


    async def _ensure_table(self, schema, table_name):
        repo = ClickHouseRepository(
            schema=schema,
            db=self.settings.clickhouse.db_name,
            table_name=table_name
        )
        await repo.ensure_table()
        self.logger.debug(f"Таблица `{table_name}` проверена/создана.")

