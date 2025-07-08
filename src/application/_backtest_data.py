import asyncio
from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt

from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineArchive, OrderbookSnapshotModel

from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__,    level=settings.logger_level)

def floor_timestamp_to_tf(ts_int: int, tf: int, offset: int) -> int:
    """
    Приводит ts_int (UNIX timestamp в секундах) к ближайшему нижнему узлу временного таймфрейма с заданным offset.
    """
    return ((ts_int - offset) // tf) * tf + offset

def get_timeframe(timestamps):
    if len(timestamps) < 2:
        return pd.Timedelta(seconds=60)
    diffs = [(t2 - t1).total_seconds() for t1, t2 in zip(timestamps, timestamps[1:])]
    most_common_diff = max(set(diffs), key=diffs.count)
    return pd.Timedelta(seconds=most_common_diff)


async def prepare_backtest_data(
    repo_kline: ClickHouseRepository[KlineArchive],
    repo_orderbook: ClickHouseRepository[OrderbookSnapshotModel],
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    database_mode: bool = True,
    kline_df: Optional[pd.DataFrame] = None,
    orderbook_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Загружает данные из ClickHouse или использует данные из базы данных или данные из предоставленных DataFrame'ов,
    возвращает объединённый датафрейм или пустой, если что-то не так.
    """

    # Преобразуем метки времени в строку
    start_str = start_time.isoformat()
    end_str = end_time.isoformat()

    if database_mode:
        logger.info("Загрузка данных из базы данных ClickHouse")
        logger.debug(repo_kline.db)
        logger.debug(repo_kline.table_name)
        logger.debug(repo_orderbook.db)
        logger.debug(repo_orderbook.table_name)
        logger.debug(start_str)
        logger.debug(end_str)

        # Запросы
        kline_query = (
            f"SELECT * FROM {repo_kline.db}.{repo_kline.table_name} "
            f"WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}' "
            "ORDER BY timestamp"
        )
        orderbook_query = (
            f"SELECT * FROM {repo_orderbook.db}.{repo_orderbook.table_name} "
            f"WHERE ts >= toUnixTimestamp64Milli(toDateTime64('{start_str}', 3)) "
            f"AND ts <= toUnixTimestamp64Milli(toDateTime64('{end_str}', 3)) "
            "ORDER BY ts"
        )
        logger.debug(f"Kline query: {kline_query}")
        logger.debug(f"Orderbook query: {orderbook_query}")
        # Асинхронная загрузка
        try:
            kline_df = await repo_kline.fetch_dataframe(kline_query)
            if kline_df.empty:
                logger.warning(f"Нет данных в таблице {repo_kline.db}.{repo_kline.table_name} за период {start_str} - {end_str}")
                kline_df = pd.DataFrame()
            logger.debug(f"Kline columns: {kline_df.columns.tolist()}")
            
        except Exception as e:
            logger.exception("Ошибка при получении данных из Kline таблицы")
            raise

        try:
            orderbook_df = await repo_orderbook.fetch_dataframe(orderbook_query)
            if orderbook_df.empty:
                orderbook_df = pd.DataFrame()
                logger.warning(f"Нет данных в таблице {repo_orderbook.db}.{repo_orderbook.table_name} за период {start_str} - {end_str}")
            logger.debug(f"Orderbook columns: {orderbook_df.columns.tolist()}")
        except Exception as e:
            logger.exception("Ошибка при получении данных из OrderBook таблицы")
            raise

        if kline_df.empty and orderbook_df.empty:
            logger.warning(f"Данные за период {start_str} - {end_str} для дальнейшей работы отсутствуют")
            return pd.DataFrame()


    else:
        logger.info("Режим без БД. Ожидается загрукза данных в DataFrame`s.")

        if kline_df is None or kline_df.empty:
            raise ValueError("kline_df не найден или пуст.")
        if orderbook_df is None or orderbook_df.empty:
            raise ValueError("orderbook_df не найден или пуст.")


    logger.info(f"Found {len(kline_df)} kline rows and {len(orderbook_df)} orderbook rows.")

    # Времена
    kline_df["timestamp"] = pd.to_datetime(kline_df["timestamp"]).dt.floor("60s").dt.tz_localize(None)
    orderbook_df["timestamp"] = pd.to_datetime(orderbook_df["ts"], unit="ms").dt.tz_localize(None)

    # Сортируем, если надо
    kline_df.sort_values("timestamp", inplace=True)
    orderbook_df.sort_values("timestamp", inplace=True)

    # Добавляем временную колонку для агрегации ордербука (1 секунда)
    orderbook_df["interval"] = orderbook_df["timestamp"].dt.floor("60s")

    # Все колонки ордербука, кроме временной "interval"
    all_cols = [col for col in orderbook_df.columns if col != "interval"]

    # Группируем ордербук по интервалу, агрегируем все колонки (последнее значение)
    orderbook_agg = orderbook_df.groupby("interval")[all_cols].agg("last")

    # Аналогично в kline_df - устанавливаем индекс по времени (timestamp), но сохраняем колонку timestamp
    kline_df["timestamp_copy"] = kline_df["timestamp"]  # Сохраняем копию timestamp
    kline_df.set_index("timestamp", inplace=True)

    # Мерджим по индексу (времени), сохраняем все колонки
    merged = kline_df.merge(orderbook_agg, left_index=True, right_index=True, how="left")

    # Заполняем пропуски
    merged.bfill(inplace=True)

    # Удаляем временную колонку "interval", если она попала в merged
    if "interval" in merged.columns:
        merged.drop(columns=["interval"], inplace=True)

    # Удаляем временную колонку "timestamp", если она попала в merged
    if "timestamp" in merged.columns:
        merged.drop(columns=["timestamp"], inplace=True)

    # Переименовываем timestamp_copy обратно в timestamp
    if "timestamp_copy" in merged.columns:
        merged.rename(columns={"timestamp_copy": "timestamp"}, inplace=True)

    # Логируем количество строк
    logger.info(f"Kline rows: {len(kline_df)}, Orderbook rows: {len(orderbook_df)}, Merged rows: {len(merged)}")

    rename_columns = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    
    merged = merged.rename(columns=rename_columns)

    # Возвращаем только объединенный DataFrame
    return merged

async def main():
    repo_kline = ClickHouseRepository(
        schema=KlineArchive,
        db=settings.clickhouse.db_name,
        table_name=settings.clickhouse.table_kline_archive,
        port=8123,
    )
    repo_orderbook = ClickHouseRepository(
        schema=OrderbookSnapshotModel,
        db=settings.clickhouse.db_name,
        table_name=settings.clickhouse.table_orderbook_snapshots,
        port=8123,
    )

    # Убедимся, что таблицы существуют (опционально)
    await repo_kline.ensure_table()
    await repo_orderbook.ensure_table()

    start_time = pd.Timestamp(f"{settings.start_time} 00:00:00")
    end_time = pd.Timestamp(f"{settings.end_time} 00:00:00")

    merged_df = await prepare_backtest_data(repo_kline, repo_orderbook, start_time, end_time, database_mode = True)

    if merged_df.empty:
        logger.warning("Нет данных для бэктеста")
        return pd.DataFrame()

    logger.info("Данные загружены:")
    logger.info(merged_df.head(100))
    logger.info(merged_df.info())
    return merged_df

if __name__ == "__main__":
    import asyncio
    import matplotlib.pyplot as plt

    try:
        # Запускаем асинхронную основную функцию и получаем DataFrame
        df = asyncio.run(main())
    except Exception as e:
        logger.error(f"Error: {e}")
        df = pd.DataFrame()

    # Проверяем, что DataFrame не пустой
    if df.empty:
        logger.warning("DataFrame is empty, skipping further processing.")
    else:
        # Создаем новые колонки, чтобы избежать конфликтов с индексом и именами колонок
        df['timestamp_merge'] = df.index
        if 'ts' in df.columns:
            df['ts_merge'] = pd.to_datetime(df['ts'], unit='ms')
        else:
            logger.warning("'ts' column not found in DataFrame.")
            df['ts_merge'] = pd.NaT

        # Пример: конвертация в секунды для построения графиков
        ts_seconds = df['ts_merge'].astype('int64') // 10**9
        timestamp_seconds = df['timestamp_merge'].astype('int64') // 10**9

        # Тут может быть твой код для графиков или анализа

        # Пример простой визуализации
        import matplotlib.pyplot as plt

        x = range(len(df))
        plt.figure(figsize=(15, 6))
        plt.scatter(x, ts_seconds, label='ts_merge (seconds since epoch)', color='orange', s=40)  # маленькие точки
        plt.scatter(x, timestamp_seconds, label='timestamp_merge (seconds since epoch)', color='blue', s=10)  # большие точки (толще)
        plt.xlabel("Row number")
        plt.ylabel("Time (seconds since epoch)")
        plt.title("Comparison of ts_merge and timestamp_merge over rows")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("data/test_timestamp_vs_ts_scatter_thicker_lower.png")
        plt.close()

        logger.info("Numeric time comparison plot saved to timestamp_vs_ts_numeric.png.")
    