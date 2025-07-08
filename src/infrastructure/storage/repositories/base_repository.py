# src/infrastructure/storage/repositories/base_repository.py
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List

T = TypeVar("T")

class BaseRepository(ABC, Generic[T]):
    """
    Абстрактный базовый класс для репозиториев.
    Определяет интерфейс, обязательный для реализации.
    """

    @abstractmethod
    async def database_exists(self) -> bool:
        """Проверить, что база данных существует и доступна."""
        pass

    @abstractmethod
    async def create_database(self) -> None:
        """Создать базу данных, если она не существует."""
        pass

    @abstractmethod
    async def table_exists(self) -> bool:
        """Проверить, что основная таблица репозитория существует."""
        pass

    @abstractmethod
    async def ensure_table(self) -> None:
        """Гарантирует существование таблицы и базы."""
        pass

    @abstractmethod
    async def drop_table(self) -> None:
        """Удаляет таблицу, если она существует."""
        pass

    @abstractmethod
    async def delete_all(self) -> None:
        """Удаляет все записи из таблицы."""
        pass

    @abstractmethod
    async def save(self, record: T) -> None:
        """Сохраняет одну запись."""
        pass

    @abstractmethod
    async def save_batch(self, records: List[T]) -> None:
        """Сохраняет несколько записей батчем."""
        pass

    @abstractmethod
    async def get_last_n(self, symbol: str, n: int) -> List[T]:
        """Получает последние N записей для заданного символа."""
        pass

    @abstractmethod
    async def get_all(self) -> List[T]:
        """Получает все записи."""
        pass
