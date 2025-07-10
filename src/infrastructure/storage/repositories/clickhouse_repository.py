import asyncio
import os
from typing import List, Type, TypeVar, Generic, Union, Optional, get_origin, get_args
from decimal import Decimal
from datetime import datetime
import json
import pandas as pd
from pydantic import BaseModel
from infrastructure.storage.repositories.base_repository import BaseRepository
from clickhouse_connect.driver.client import Client as ClickHouseClient
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError, DatabaseError
from concurrent.futures import ThreadPoolExecutor
from infrastructure.logging_config import setup_logger


logger = setup_logger(__name__)


executor = ThreadPoolExecutor()

T = TypeVar("T", bound=BaseModel)

class ClickHouseRepository(BaseRepository[T], Generic[T]):
    client: ClickHouseClient

    def __init__(self, schema: Type[T], table_name: str, db: str = "default"):
        self.schema = schema
        self.table_name = table_name
        self.db = db if db and db != 'default' else os.getenv('CLICKHOUSE_DB_NAME', 'default')
       
        ch_host = os.getenv('CLICKHOUSE_HOST', 'localhost')

        ch_port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        ch_user = os.getenv('CLICKHOUSE_USER', 'default')
        ch_password = os.getenv('CLICKHOUSE_PASSWORD', '') 

        
        self.client = get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database=db 
        )

        try:
            self.client.ping()
            logger.info(f"Успешное подключение к ClickHouse по адресу: {ch_host}:{ch_port}")
        except OperationalError as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            raise 

        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.db}")
            logger.info(f"База данных '{self.db}' успешно создана (или уже существовала).")
        except OperationalError as e:
            logger.error(f"Не удалось создать базу данных '{self.db}': {e}")
            raise 
 
    async def database_exists(self) -> bool:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, lambda: self.client.command(f"EXISTS DATABASE {self.db}"))
        if isinstance(result, int):
            return bool(result)
        elif isinstance(result, str):
            return bool(int(result))
        elif hasattr(result, 'result_rows'):
            rows = getattr(result, 'result_rows', [])
            if rows and rows[0]:
                return bool(int(rows[0][0]))
            return False
        else:
            raise TypeError(f"Unexpected result type from client.command: {type(result)}")

    async def create_database(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.db}"))

    async def drop_database(self) -> None:
        """
        Drops the database specified in self.db if it exists.
        """
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self.client.command(f"DROP DATABASE IF EXISTS {self.db}")
            )
            logger.info(f"Database {self.db} successfully dropped.")
        except Exception as e:
            logger.error(f"Failed to drop database {self.db}: {str(e)}", exc_info=True)
            raise

    async def table_exists(self) -> bool:
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

    async def update(self, df: pd.DataFrame) -> None:
        """
        Обновляет (дополняет) таблицу новыми данными из DataFrame.
        В ClickHouse обычно просто вставляем новые записи.
        """
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
                executor, # Используем общий пул потоков
                lambda: self.client.insert(f"{self.db}.{self.table_name}", data, column_names=columns)
            )
        except Exception as e:
            logger.error(f"Failed to save records: {str(e)}", exc_info=True)
    
    async def save_string_to_table(self, db: str, table: str, data_str: str) -> None:
        # Сохраним текущие значения, чтобы временно переключиться
        orig_db = self.db
        orig_table = self.table_name

        loop = asyncio.get_running_loop() # Получаем цикл событий один раз

        try:
            self.db = db
            self.table_name = table

            db_exists_raw_result = await loop.run_in_executor(None, lambda: self.client.command(f"EXISTS DATABASE {db}"))
            db_exists = False
            if isinstance(db_exists_raw_result, int):
                db_exists = bool(db_exists_raw_result)
            elif isinstance(db_exists_raw_result, str):
                db_exists = bool(int(db_exists_raw_result))
            elif hasattr(db_exists_raw_result, 'result_rows'):
                rows = getattr(db_exists_raw_result, 'result_rows', [])
                if rows and rows[0]:
                    db_exists = bool(int(rows[0][0]))
            else:
                raise TypeError(f"Unexpected result type from client.command for database_exists: {type(db_exists_raw_result)}")

            if not db_exists:
                await loop.run_in_executor(None, lambda: self.client.command(f"CREATE DATABASE {db}"))


            table_exists_query = f"""
            SELECT name FROM system.tables
            WHERE database = '{db}' AND name = '{table}'
            """
            table_exists_result = await loop.run_in_executor(None, lambda: self.client.query(table_exists_query))
            if not len(table_exists_result.result_rows) > 0:
                ddl = f"""
                    CREATE TABLE {db}.{table} (
                        data String
                    ) ENGINE = MergeTree()
                    ORDER BY tuple()
                """
                await loop.run_in_executor(None, lambda: self.client.command(ddl))

            # Вставляем данные
            await loop.run_in_executor(
                executor, # Используем общий пул потоков
                lambda: self.client.insert(
                    table=table,
                    data=[[data_str]],
                    database=db,
                    column_names=["data"] # Имя столбца должно соответствовать DDL
                )
            )

        finally:
            # Возвращаем исходные значения
            self.db = orig_db
            self.table_name = orig_table

    # ЗАПРОСЫ
    async def get_last_n(self, symbol: str, n: int) -> List[T]:
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(
            None,
            lambda: self.client.query(
                f"""
                SELECT * FROM {self.db}.{self.table_name}
                WHERE symbol = %(symbol)s
                ORDER BY timestamp DESC
                LIMIT %(n)s
                """,
                parameters={"symbol": symbol, "n": n}
            ).result_rows
        )
        return [self._deserialize_row(dict(zip(self.schema.model_fields.keys(), row))) for row in rows]

    async def get_all(self) -> List[T]:
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(
            None,
            lambda: self.client.query(f"SELECT * FROM {self.db}.{self.table_name}").result_rows
        )
        return [self._deserialize_row(dict(zip(self.schema.model_fields.keys(), row))) for row in rows]

    async def fetch_dataframe(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, lambda: self.client.query(query, parameters=params))
        columns = result.column_names
        rows = result.result_rows
        df = pd.DataFrame(rows, columns=columns)
        logger.debug(df)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    async def get_max_timestamp(self, column: str = 'ts', filter_expr: str = '') -> pd.Timestamp | None:
        loop = asyncio.get_running_loop()
        query = f"SELECT max({column}) FROM {self.db}.{self.table_name}" # Добавлено self.db
        if filter_expr:
            query += f" WHERE {filter_expr}"

        result = await loop.run_in_executor(None, lambda: self.client.query(query))
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
