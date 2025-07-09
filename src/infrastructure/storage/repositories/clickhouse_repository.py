# src/infrastructure/storage/repositories/clickhouse_repository.py

import asyncio
from typing import List, Type, TypeVar, Generic, Union, get_origin, get_args
from decimal import Decimal
from datetime import datetime
import json
import pandas as pd
from pydantic import BaseModel
from infrastructure.storage.repositories.base_repository import BaseRepository
from clickhouse_connect import get_client
from concurrent.futures import ThreadPoolExecutor
from infrastructure.logging_config import setup_logger


logger = setup_logger(__name__)

# Создаём один экземпляр пула потоков для переиспользования
executor = ThreadPoolExecutor()

T = TypeVar("T", bound=BaseModel)

class ClickHouseRepository(BaseRepository[T], Generic[T]):
    def __init__(self, schema: Type[T], table_name: str, db: str = "default", port: int = 8123):
        self.schema = schema
        self.table_name = table_name
        self.port = port
        self.db = db

        self.client = get_client(port=port)  # Изначально default
        
        # Ленивая инициализация базы — база создастся при первом обращении
        self._db_checked = False

    def _ensure_database(self):
        # Этот метод синхронный, вызываем при первом запросе
        if not self._db_checked:
            exists = self.client.command(f"EXISTS DATABASE {self.db}")
            if not bool(int(exists)):
                self.client.command(f"CREATE DATABASE {self.db}")
            # Теперь переключаем клиент на нужную базу
            self.client = get_client(port=self.port, database=self.db)
            self._db_checked = True

    async def database_exists(self) -> bool:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, lambda: self.client.command(f"EXISTS DATABASE {self.db}"))
        return bool(int(result))

    async def create_database(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.db}"))

    async def table_exists(self) -> bool:
        self._ensure_database()
        loop = asyncio.get_running_loop()
        query = f"""
        SELECT name FROM system.tables
        WHERE database = '{self.db}' AND name = '{self.table_name}'
        """
        result = await loop.run_in_executor(None, lambda: self.client.query(query))
        return len(result.result_rows) > 0

    async def ensure_table(self) -> None:
        """
        Создаёт таблицу в базе, если она не существует.
        Поля создаются на основе Pydantic-схемы.
        """
        self._ensure_database()
        if not await self.table_exists():
            fields: List[str] = []
            for name, field in self.schema.model_fields.items():
                ch_type: str = self._map_field_type(field.annotation)
                fields.append(f"`{name}` {ch_type}")
            ddl: str = f"""
                CREATE TABLE {self.db}.{self.table_name} (
                    {', '.join(fields)}
                ) ENGINE = MergeTree()
                ORDER BY tuple()
            """
            await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self.client.command(ddl)
            )
            
    async def drop_table(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self.client.command(f"DROP TABLE IF EXISTS {self.db}.{self.table_name}"))

    async def delete_all(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self.client.command(f"TRUNCATE TABLE {self.db}.{self.table_name}"))

    # Обновление и сохранение данных
    async def update(self, df: pd.DataFrame) -> None:
        """
        Обновляет (дополняет) таблицу новыми данными из DataFrame.
        В ClickHouse обычно просто вставляем новые записи.
        """
        # Приводим DataFrame к списку моделей (твоего BaseModel)
        records = []
        for _, row in df.iterrows():
            data = row.to_dict()
            try:
                record = self.schema(**data)
                records.append(record)
            except Exception as e:
                logger.error(f"Failed to parse record for update: {e}")

        # Сохраняем пачкой
        if records:
            await self.save_batch(records)

    async def save(self, record: T) -> None:
        await self.save_batch([record])

    async def save_batch(self, records: List[T]) -> None:
        try:
            columns = list(self.schema.model_fields.keys())
            data = [
                [getattr(rec, field) for field in columns]
                for rec in records
            ]
            
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                executor,
                lambda: self.client.insert(f"{self.db}.{self.table_name}", data, column_names=columns)
            )
        except Exception as e:
            logger.error(f"Failed to save records: {str(e)}", exc_info=True)
    
    async def save_string_to_table(self, db: str, table: str, data_str: str) -> None:
        # Сохраним текущие значения, чтобы временно переключиться
                
        orig_db = self.db
        orig_table = self.table_name

        try:
            # Переключаемся на нужную базу и таблицу
            self.db = db
            self.table_name = table

            # Проверяем и создаём базу, если нужно
            if not await self.database_exists():
                self.client.command(f"CREATE DATABASE IF NOT EXISTS {db}")

            # Проверяем и создаём таблицу, если нужно
            if not await self.table_exists():
                ddl = f"""
                    CREATE TABLE {db}.{table} (
                        data String
                    ) ENGINE = MergeTree()
                    ORDER BY tuple()
                """
                self.client.command(ddl)

            # Вставляем строку
            self.client.insert(
                    table=table,
                    data=[[data_str]],
                    database=db,
                    column_names=["filename"]
                )

        finally:
            # Возвращаем исходные значения
            self.db = orig_db
            self.table_name = orig_table

    # ЗАПРОСЫ
    async def get_last_n(self, symbol: str, n: int) -> List[T]:
        rows = self.client.query(
            f"""
            SELECT * FROM {self.db}.{self.table_name}
            WHERE symbol = %(symbol)s
            ORDER BY timestamp DESC
            LIMIT %(n)s
            """,
            parameters={"symbol": symbol, "n": n}
        ).result_rows
        return [self._deserialize_row(dict(zip(self.schema.model_fields.keys(), row))) for row in rows]

    async def get_all(self) -> List[T]:
        rows = self.client.query(f"SELECT * FROM {self.db}.{self.table_name}").result_rows
        return [self._deserialize_row(dict(zip(self.schema.model_fields.keys(), row))) for row in rows]

    async def fetch_dataframe(self, query: str, params: dict = None) -> pd.DataFrame:
        result = self.client.query(query)  # тут result — сразу list строк, у него нет column_names
        columns = result.column_names  # Ошибка: result — это list, у него нет column_names
        rows = list(result.result_rows)  # Ошибка: result — list, нет .result_rows
        # logger.debug(columns)
        df = pd.DataFrame(rows, columns=columns)
        logger.debug(df)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    async def get_max_timestamp(self, column: str = 'ts', filter_expr: str = '') -> pd.Timestamp | None:
        query = f"SELECT max({column}) FROM {self.table_name}"
        if filter_expr:
            query += f" WHERE {filter_expr}"

        result = self.client.query(query)
        rows = result.result_rows
        max_ts = rows[0][0] if rows and rows[0][0] is not None else None

        if max_ts:
            # Преобразуем к pd.Timestamp, если надо
            return pd.to_datetime(max_ts)
        return None

    # Сервисные методы
    def _serialize_record(self, record: T) -> dict:
        data = record.model_dump()
        for key, value in data.items():
            if isinstance(value, (list, tuple)):
                # Сохраняем сложные типы как JSON-строку
                data[key] = json.dumps(value)
            elif isinstance(value, Decimal):
                data[key] = float(value)  # или str(value), если нужен точный формат
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

    def _deserialize_row(self, data: dict) -> T:
        for key, field in self.schema.model_fields.items():
            val = data.get(key)
            if val is None:
                continue
            typ = field.annotation
            origin = get_origin(typ)

            if typ == datetime:
                if isinstance(val, str):
                    data[key] = datetime.fromisoformat(val)
                elif isinstance(val, datetime):
                    data[key] = val
                else:
                    raise TypeError(f"Expected str or datetime, got {type(val)} for field '{key}'")
            elif typ == Decimal:
                data[key] = Decimal(str(val))
            elif origin in (list, tuple):
                # парсим JSON обратно в список/кортеж
                if isinstance(val, str):
                    data[key] = json.loads(val)
                else:
                    data[key] = val
        return self.schema(**data)


    def _map_field_type(self, python_type) -> str:
        origin = get_origin(python_type)
        args = get_args(python_type)

        # Обработка Optional / Union с NoneType
        if origin is Union and type(None) in args:
            # Возьмём первый непустой тип из Union
            non_none_types = [arg for arg in args if arg is not type(None)]
            if len(non_none_types) == 1:
                return self._map_field_type(non_none_types[0])
            else:
                raise TypeError(f"Unsupported Union field type: {python_type}")

        if python_type == str:
            return "String"
        elif python_type == int:
            return "Int64"
        elif python_type == float or python_type == Decimal:
            return "Float64"
        elif python_type == datetime:
            return "DateTime"
        elif origin in (list, tuple):
            # Сохраняем списки и кортежи как JSON-строки
            return "String"
        else:
            raise TypeError(f"Unsupported field type: {python_type}")
        
    async def close(self) -> None:
        """
        Closes the ClickHouse client connection.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.client.close)