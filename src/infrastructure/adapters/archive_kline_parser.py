from datetime import datetime, timedelta, timezone
import re
import json
from zoneinfo import ZoneInfo
import requests
import time
import pandas as pd
from typing import AsyncGenerator, List

from pydantic import ValidationError

from infrastructure import ClickHouseRepository
from infrastructure.storage.schemas import KlineRecord, KlineRecordDatetime
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__, level=settings.logger_level)


class KlineParser:
    """
    Класс для сбора и сохранения дневных OHLCV данных с Binance API в ClickHouse.
    
    Атрибуты:
        repo (ClickHouseRepository): репозиторий для сохранения данных OHLCV.
        symbol (str): торговый символ, например, 'ETHUSDT'.
        interval (str): интервал свечей, например, '1m'.
        repo_save_date (ClickHouseRepository): репозиторий для меток обработанных дат.
    """

    def __init__(self, repository: ClickHouseRepository, symbol: str, interval: str = settings.kline.interval):
        self.repo: ClickHouseRepository = repository
        self.symbol: str = symbol
        # Уберём все лишние символы 'm' в конце и добавим ровно один 'm'
        interval = re.sub(r"m+$", "", interval) + "m"
        self.interval = interval
        self.repo_save_date: ClickHouseRepository = ClickHouseRepository(
            KlineRecordDatetime,
            db=settings.clickhouse.db_name,
            table_name=settings.clickhouse.table_kline_archive_datetime
        )

    def daterange(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        return [start_date + timedelta(n) for n in range((end_date - start_date).days + 1)]

    @staticmethod
    def get_binance_ohlcv_day(symbol: str = 'ETHUSDT', date_str: str = '2025-06-01', interval: str = settings.kline.interval) -> pd.DataFrame:
        """
        Загружает OHLCV данные за один день с Binance API.

        Args:
            symbol (str): торговый символ.
            date_str (str): дата в формате 'YYYY-MM-DD'.
            interval (str): интервал свечей.

        Returns:
            pd.DataFrame: таблица с OHLCV данными за день.
        """
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

        # Создаем DataFrame только из all_data, а не raw_data (исправлено)
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

    async def is_date_already_processed(self, date_str: str) -> bool:
        """
        Проверяет, была ли дата уже обработана и сохранена в метках.

        Args:
            date_str (str): дата в формате 'YYYY-MM-DD'.

        Returns:
            bool: True, если дата уже обработана, иначе False.
        """
        existing = await self.repo_save_date.get_all()
        for row in existing:
            if isinstance(row, dict):
                value = row.get("data")
            else:
                value = getattr(row, "data", None)
            try:
                parsed = json.loads(value) if isinstance(value, str) else value
                if isinstance(parsed, dict) and parsed.get("date") == date_str and parsed.get("interval") == self.interval:
                    return True
            except Exception:
                continue
        return False

    async def mark_date_processed(self, date_str: str) -> None:
        """
        Отмечает дату как обработанную, сохраняя метку в базе.

        Args:
            date_str (str): дата в формате 'YYYY-MM-DD'.
        """
        record = {
            "exchange": settings.binance.name,
            "symbol": self.symbol,
            "data": json.dumps({"date": date_str, "interval": self.interval}),
            "data_processed": datetime.now(timezone.utc),
            "interval": self.interval
        }
        try:
            record_model = KlineRecordDatetime(**record)
            await self.repo_save_date.ensure_table() 
            await self.repo_save_date.save_batch([record_model])
            logger.info(f"Отмечена обработка даты: {date_str}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении метки обработки {date_str}: {str(e)}")
            raise

    
    def is_supported_date_format(self, date_str: str) -> bool:
        """
        Проверяет, является ли дата строкой в одном из поддерживаемых форматов:
        - YYYY-MM-DD
        - DD-MM-YYYY
        - Unix timestamp (в секундах или миллисекундах)
        """
        # Example regex for 'YYYY-MM-DD'
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', date_str):
            return True # Or it might be returning the match object directly
        # Example regex for 'DD-MM-YYYY'
        if re.fullmatch(r'\d{2}-\d{2}-\d{4}', date_str):
            return True
        # Example regex for Unix timestamp (10 or 13 digits)
        if re.fullmatch(r'\d{10}|\d{13}', date_str):
            return True
        return False


    def parse_date(self, date_str: str) -> datetime:
        """
        Парсинг строки даты в datetime.
        Поддерживаемые форматы: YYYY-MM-DD, DD-MM-YYYY, Unix timestamp (в секундах или миллисекундах)
        """
        utc_zone = ZoneInfo("UTC")

        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', date_str):
            return datetime.strptime(date_str, '%Y-%m-%d')
        elif re.fullmatch(r'\d{2}-\d{2}-\d{4}', date_str):
            return datetime.strptime(date_str, '%d-%m-%Y')
        elif re.fullmatch(r'\d{10}', date_str): # Таймстамп в секундах (например, 1735689500)
            return datetime.fromtimestamp(int(date_str), tz=utc_zone).replace(tzinfo=None)
        elif re.fullmatch(r'\d{13}', date_str): # Таймстамп в миллисекундах (например, 1735689500000)
            return datetime.fromtimestamp(int(date_str) / 1000, tz=utc_zone).replace(tzinfo=None)
        else:
            raise ValueError(f"Unsupported date format: {date_str}")


    async def fetch_and_save_day(self, date: datetime) -> None:
        """
        Загружает OHLCV данные за один день и сохраняет их в ClickHouse,
        если они еще не были обработаны.

        Args:
            date (datetime): дата для загрузки.
        """
        date_str = date.strftime("%Y-%m-%d")
        if await self.is_date_already_processed(date_str):
            logger.info(f"Пропуск {date_str}: уже обработано.")
            return

        logger.info(f"Загружаем данные за {date_str}...")
        try:
            df_day = self.get_binance_ohlcv_day(symbol=self.symbol, date_str=date_str, interval=self.interval)
        except Exception as e:
            logger.error(f"Ошибка загрузки данных за {date_str}: {str(e)}")
            return

        records: List[KlineRecord] = []
        for _, row in df_day.iterrows():
            record_data = {
                "symbol": self.symbol,
                "timestamp": row['open_time'],
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close'],
                "volume": row['volume'],
                "interval": self.interval
            }
            try:
                record = self.repo.schema(**record_data)
                records.append(record)
            except ValidationError as ve:
                logger.error(f"Ошибка валидации записи: {str(ve)}\nСтрока: {record_data}")
                continue

        if records:
            try:
                await self.repo.save_batch(records)
                await self.mark_date_processed(date_str) 
                logger.info(f"Сохранено {len(records)} записей за {date_str}.")
            except Exception as e:
                logger.error(f"Ошибка сохранения записей за {date_str}: {str(e)}")
                raise
        else:
            logger.warning(f"Нет данных за {date_str}.")

    async def collect_kline_range(self, start_date_str: str, end_date_str: str) -> None:
        """
        Загружает и сохраняет OHLCV данные за диапазон дат.
        Поддерживаются форматы дат: 'DD-MM-YYYY' и 'YYYY-MM-DD'.

        Args:
            start_date_str (str): начальная дата в формате 'DD-MM-YYYY' или 'YYYY-MM-DD'.
            end_date_str (str): конечная дата в формате 'DD-MM-YYYY' или 'YYYY-MM-DD'.
        """

        start_date = self.parse_date(start_date_str)
        end_date = self.parse_date(end_date_str)

        if not await self.repo.database_exists():
            await self.repo.create_database()
            logger.info(f"Создана база данных {self.repo.db}")

        await self.repo.ensure_table()
        await self.repo_save_date.ensure_table()

        for single_date in self.daterange(start_date, end_date):
            await self.fetch_and_save_day(single_date)


if __name__ == "__main__":
    repo = ClickHouseRepository(
        schema=KlineRecord,
        db=settings.clickhouse.db_name,
        table_name=settings.clickhouse.table_kline_archive  
    )
    parser = KlineParser(repo, symbol="BTCUSDT", interval="1m")
    print(parser.parse_date("01-06-2025"))
    print(parser.parse_date("1735689500000"))

# if __name__ == "__main__":
#     repo = ClickHouseRepository(
#         schema=KlineRecord,
#         db=settings.clickhouse.db_name,
#         table_name=settings.clickhouse.table_kline_archive
#     )
#     collector = KlineDataCollector(repo, symbol=settings.pair_tokens, interval=settings.kline.interval)
#     asyncio.run(collector.collect_kline_range("01-06-2025", "30-06-2025"))
