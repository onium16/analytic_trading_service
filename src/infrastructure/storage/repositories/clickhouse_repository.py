# src/infrastructure/storage/repositories/clickhouse_repository.py

import asyncio
from typing import List, Type, TypeVar, Generic, Union, get_origin, get_args
from decimal import Decimal
from datetime import datetime, timezone # Добавил timezone для десериализации
import json
import pandas as pd
from pydantic import BaseModel
from infrastructure.storage.repositories.base_repository import BaseRepository
from clickhouse_connect import get_client
from concurrent.futures import ThreadPoolExecutor # Оставляем ваш ThreadPoolExecutor, как вы просили
from infrastructure.logging_config import setup_logger


logger = setup_logger(__name__)

# Используем ваш существующий ThreadPoolExecutor
executor = ThreadPoolExecutor()

T = TypeVar("T", bound=BaseModel)

class ClickHouseRepository(BaseRepository[T], Generic[T]):
    def __init__(self, schema: Type[T], table_name: str, db: str = "default", port: int = 8123):
        self.schema = schema
        self.table_name = table_name
        self.port = port
        self.db = db

        # Инициализация клиента. Это блокирующий вызов, но обычно быстрый.
        # Поскольку он происходит в __init__, он выполняется синхронно при создании объекта.
        self.client = get_client(port=port)  # Изначально default
        
        # Ленивая инициализация базы — база создастся при первом обращении
        self._db_checked = False
        logger.debug(f"ClickHouseRepository инициализирован для {db}.{table_name}")

    # Вспомогательный асинхронный метод для выполнения синхронных операций с self.client
    # Он будет использовать ваш глобальный 'executor'.
    async def _run_sync_op(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))

    # _ensure_database теперь асинхронный и использует _run_sync_op
    async def _ensure_database_async(self): # Переименовал, чтобы не конфликтовать со старым синхронным
        if not self._db_checked:
            logger.debug(f"Проверка существования базы данных {self.db}...")
            exists = await self._run_sync_op(self.client.command, f"EXISTS DATABASE {self.db}")
            if not bool(int(exists)):
                logger.info(f"База данных {self.db} не существует. Создаем...")
                await self._run_sync_op(self.client.command, f"CREATE DATABASE {self.db}")
                logger.info(f"База данных {self.db} создана.")
            
            # Переключение клиента на нужную базу. Этот вызов get_client также блокирующий.
            # Поэтому его тоже нужно обернуть в run_in_executor.
            old_client = self.client # Сохраняем ссылку на старый клиент
            self.client = await self._run_sync_op(get_client, port=self.port, database=self.db)
            if old_client:
                # Закрываем старый клиент, если он существовал
                await self._run_sync_op(old_client.close)
                
            self._db_checked = True
            logger.debug(f"Проверка базы данных {self.db} завершена.")

    async def database_exists(self) -> bool:
        # Используем _run_sync_op
        result = await self._run_sync_op(self.client.command, f"EXISTS DATABASE {self.db}")
        return bool(int(result))

    async def create_database(self) -> None:
        # Используем _run_sync_op
        await self._run_sync_op(self.client.command, f"CREATE DATABASE IF NOT EXISTS {self.db}")



    async def table_exists(self) -> bool:
        # Теперь вызываем асинхронную версию _ensure_database
        await self._ensure_database_async() 
        # Используем _run_sync_op
        query = f"""
        SELECT name FROM system.tables
        WHERE database = '{self.db}' AND name = '{self.table_name}'
        """
        result = await self._run_sync_op(self.client.query, query)
        return len(result.result_rows) > 0

    async def ensure_table(self) -> None:
        """
        Создаёт таблицу в базе, если она не существует.
        Поля создаются на основе Pydantic-схемы.
        """
        # Теперь вызываем асинхронную версию _ensure_database
        await self._ensure_database_async() 
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
            # Используем _run_sync_op
            await self._run_sync_op(self.client.command, ddl)
            logger.info(f"Таблица {self.db}.{self.table_name} создана.")
            
    async def drop_table(self) -> None:
        # Используем _run_sync_op
        await self._run_sync_op(self.client.command, f"DROP TABLE IF EXISTS {self.db}.{self.table_name}")
        logger.info(f"Таблица {self.db}.{self.table_name} удалена.")

    async def drop_database(self) -> None:
        logger.info(f"Удаляем базу данных {self.db}...")
        loop = asyncio.get_running_loop()
        temp_client_for_drop = await loop.run_in_executor(executor, lambda: get_client(port=self.port))
        try:
            await loop.run_in_executor(executor, lambda: temp_client_for_drop.command(f"DROP DATABASE IF EXISTS {self.db}"))
            logger.info(f"База данных {self.db} удалена.")
            self._db_checked = False # Reset flag after dropping
        except Exception as e:
            logger.error(f"Ошибка при удалении базы данных {self.db}: {e}", exc_info=True)
            raise
        finally:
            # Ensure the temporary client is closed
            await loop.run_in_executor(executor, lambda: temp_client_for_drop.close())

    async def delete_all(self) -> None:
        # Используем _run_sync_op
        await self._run_sync_op(self.client.command, f"TRUNCATE TABLE {self.db}.{self.table_name}")
        logger.info(f"Таблица {self.db}.{self.table_name} очищена.")

    # Обновление и сохранение данных
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

        if records:
            await self.save_batch(records)

    async def save(self, record: T) -> None:
        await self.save_batch([record])

    async def save_batch(self, records: List[T]) -> None:
        if not records:
            logger.debug("save_batch: Нет записей для сохранения. Выходим.")
            return

        logger.debug(f"save_batch: Попытка сохранения {len(records)} записей в {self.db}.{self.table_name}.")
        try:
            columns = list(self.schema.model_fields.keys())
            data = [
                [getattr(rec, field) for field in columns]
                for rec in records
            ]
            
            # Логируем данные перед вставкой
            logger.debug(f"save_batch: Данные для вставки подготовлены (первая запись): {data[0] if data else 'НЕТ ДАННЫХ'}")
            logger.debug(f"save_batch: Столбцы для вставки: {columns}")

            # Использование _run_sync_op уже присутствует, это хорошо.
            await self._run_sync_op(self.client.insert, f"{self.db}.{self.table_name}", data, column_names=columns)
            logger.info(f"save_batch: Успешно сохранено {len(records)} записей в {self.db}.{self.table_name}.")
        except Exception as e:
            logger.error(f"save_batch: Ошибка при сохранении записей: {str(e)}", exc_info=True)
            raise 

    
    async def save_string_to_table(self, db: str, table: str, data_str: str) -> None:
        logger.info(f"Попытка сохранения строки '{data_str}' в {db}.{table}...")
        
        
        loop = asyncio.get_running_loop()
        temp_client = await loop.run_in_executor(executor, lambda: get_client(port=self.port, database=db))
        try:
            exists_db = await loop.run_in_executor(executor, lambda: temp_client.command(f"EXISTS DATABASE {db}"))
            
            # Приводим к строке или числу, в зависимости от результата
            if isinstance(exists_db, (str, int)):
                exists_db_int = int(exists_db)
            elif isinstance(exists_db, (list, tuple)) and len(exists_db) > 0:
                # если вернулся список строк, берем первый элемент
                exists_db_int = int(exists_db[0])
            else:
                # если QuerySummary или неизвестный тип - обработай по ситуации или выбрось ошибку
                raise TypeError(f"Unexpected type for exists_db: {type(exists_db)}")

            if not bool(int(exists_db_int)):
                logger.info(f"Создаем базу данных {db} для сохранения имени файла...")
                await loop.run_in_executor(executor, lambda: temp_client.command(f"CREATE DATABASE IF NOT EXISTS {db}"))
                logger.info(f"База данных {db} создана.")

            table_check_query = f"""
            SELECT name FROM system.tables
            WHERE database = '{db}' AND name = '{table}'
            """
            table_exists = await loop.run_in_executor(executor, lambda: len(temp_client.query(table_check_query).result_rows) > 0)

            if not table_exists:
                ddl = f"""
                    CREATE TABLE {db}.{table} (
                        filename String  -- Изменил 'data' на 'filename' для ясности
                    ) ENGINE = MergeTree()
                    ORDER BY tuple()
                """
                logger.info(f"Создаем таблицу {db}.{table} для хранения имен файлов...")
                await loop.run_in_executor(executor, lambda: temp_client.command(ddl))
                logger.info(f"Таблица {db}.{table} создана.")

            # Вставляем строку через временный клиент
            logger.debug(f"Вставка строки '{data_str}' в {db}.{table}...")
            await loop.run_in_executor(
                executor,
                lambda: temp_client.insert(
                    table=table,
                    data=[[data_str]],
                    database=db,
                    column_names=["filename"] # Изменил 'data' на 'filename'
                )
            )
            logger.info(f"Строка '{data_str}' успешно сохранена в {db}.{table}.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении строки '{data_str}' в {db}.{table}: {str(e)}", exc_info=True)
            raise
        finally:
            # Важно: закрываем временный клиент!
            await loop.run_in_executor(executor, lambda: temp_client.close())


    # ЗАПРОСЫ
    async def get_last_n(self, symbol: str, n: int) -> List[T]:
        logger.debug(f"Получение последних {n} записей для {symbol} из {self.table_name}.")
        # Используем _run_sync_op
        result = await self._run_sync_op(
            self.client.query,
            f"""
            SELECT * FROM {self.db}.{self.table_name}
            WHERE symbol = %(symbol)s
            ORDER BY ts DESC -- Изменил timestamp на ts, как в OrderbookSnapshotModel
            LIMIT %(n)s
            """,
            parameters={"symbol": symbol, "n": n}
        )
        rows = result.result_rows
        return [self._deserialize_row(dict(zip(self.schema.model_fields.keys(), row))) for row in rows]

    async def get_all(self) -> List[T]:
        logger.debug(f"get_all: Попытка получить все записи из {self.db}.{self.table_name}.")
        try:
            # Использование _run_sync_op уже присутствует, это хорошо.
            result = await self._run_sync_op(self.client.query, f"SELECT * FROM {self.db}.{self.table_name}")
            rows = result.result_rows
            
            logger.debug(f"get_all: Получено {len(rows)} сырых строк из ClickHouse.")
            if rows:
                logger.debug(f"get_all: Первая сырая строка: {rows[0]}")
                logger.debug(f"get_all: Имена столбцов из результата запроса: {result.column_names}")

            # Проверяем, что _deserialize_row возвращает что-то полезное
            deserialized_records = [self._deserialize_row(dict(zip(self.schema.model_fields.keys(), row))) for row in rows]
            logger.debug(f"get_all: Успешно десериализовано {len(deserialized_records)} записей.")
            return deserialized_records
        except Exception as e:
            logger.error(f"get_all: Ошибка при получении или десериализации записей: {str(e)}", exc_info=True)
            raise # Перебрасываем ошибку

    async def fetch_dataframe(self, query: str, params: dict = None) -> pd.DataFrame:
        logger.debug(f"Выполнение запроса для DataFrame: {query[:100]}...")
        # Используем _run_sync_op и исправляем доступ к result
        result = await self._run_sync_op(self.client.query, query, params=params)
        columns = result.column_names  # Теперь result имеет .column_names
        rows = list(result.result_rows) # Теперь result имеет .result_rows
        
        df = pd.DataFrame(rows, columns=columns)
        logger.debug(f"DataFrame получен, строки: {len(df)}")
        if "timestamp" in df.columns:
            logger.warning("Обнаружен 'timestamp' столбец в DataFrame. Возможно, ожидался 'ts'.")
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    async def get_max_timestamp(self, column: str = 'ts', filter_expr: str = '') -> pd.Timestamp | None:
        query = f"SELECT max({column}) FROM {self.db}.{self.table_name}" # Добавил self.db
        if filter_expr:
            query += f" WHERE {filter_expr}"
        logger.debug(f"Получение максимального timestamp: {query}")

        # Используем _run_sync_op
        result = await self._run_sync_op(self.client.query, query)
        rows = result.result_rows
        max_ts = rows[0][0] if rows and rows[0][0] is not None else None

        if max_ts:
            return pd.to_datetime(max_ts)
        return None

    # Сервисные методы
    def _serialize_record(self, record: T) -> dict:
        data = record.model_dump()
        for key, value in data.items():
            if isinstance(value, (list, tuple)):
                data[key] = json.dumps(value)
            elif isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

    def _deserialize_row(self, data: dict) -> T:
        # Добавим логирование здесь, особенно перед валидацией Pydantic-схемой
        logger.debug(f"_deserialize_row: Входные данные до обработки: {data}")
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
                elif isinstance(val, (int, float)): # ClickHouse DateTime64(3) хранит как Int64 (миллисекунды от эпохи)
                    # Преобразуем миллисекунды в секунды для fromtimestamp
                    # Убедимся, что timezone импортирован: from datetime import datetime, timezone
                    data[key] = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
                else:
                    logger.warning(f"Ожидался str, datetime, int или float, получен {type(val)} для поля '{key}'. Значение: {val}")
            elif typ == Decimal:
                # ClickHouse Float64 или Decimal может быть передан как float, конвертируем в строку, затем в Decimal
                data[key] = Decimal(str(val))
            elif origin in (list, tuple):
                if isinstance(val, str):
                    try:
                        data[key] = json.loads(val)
                    except json.JSONDecodeError:
                        logger.error(f"_deserialize_row: Ошибка декодирования JSON для поля '{key}': '{val}'", exc_info=True)
                        data[key] = [] # Или вернуть пустой список, или пробросить ошибку
                else:
                    data[key] = val
        try:
            # Убеждаемся, что Pydantic получает только те поля, которые есть в схеме, чтобы избежать ошибок.
            final_data = {k: v for k, v in data.items() if k in self.schema.model_fields}
            logger.debug(f"_deserialize_row: Данные после преобразования типов, перед валидацией Pydantic: {final_data}")
            return self.schema(**final_data)
        except Exception as e:
            logger.error(f"_deserialize_row: Ошибка валидации Pydantic модели {self.schema.__name__} данными {data}. Ошибка: {e}", exc_info=True)
            raise # Перебрасываем ошибку

    def _map_field_type(self, python_type) -> str:
        origin = get_origin(python_type)
        args = get_args(python_type)

        if origin is Union and type(None) in args:
            non_none_types = [arg for arg in args if arg is not type(None)]
            if len(non_none_types) == 1:
                return self._map_field_type(non_none_types[0]) + " NULL" # Добавлено " NULL" для Optional
            else:
                logger.warning(f"Unsupported complex Union field type, mapping to String: {python_type}")
                return "String"

        if python_type == str:
            return "String"
        elif python_type == int:
            return "Int64"
        elif python_type == float: 
            return "Float64"
        elif python_type == Decimal: # Разделил float и Decimal для более точного маппинга
            return "Float64" # Можно использовать "Decimal(18, 9)" для лучшей точности
        elif python_type == datetime:
            return "DateTime64(3)" # Используем DateTime64(3) для миллисекунд
        elif origin in (list, tuple):
            return "String"
        else:
            raise TypeError(f"Unsupported field type: {python_type}")

    # Добавляем метод для корректного закрытия клиента ClickHouse
    async def close(self):
        if self.client:
            logger.debug("Закрываем соединение с ClickHouse.")
            # Закрытие клиента также должно быть в ThreadPoolExecutor
            await self._run_sync_op(self.client.close)
            self.client = None
            logger.debug("Соединение с ClickHouse закрыто.")