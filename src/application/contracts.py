# src/application/contracts.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, List, Optional, Tuple

from domain._exceptions import InvalidOrderBookError, InvalidPriceError, InvalidVolumeError # Добавляем Optional

class Command(ABC):
    """Базовая команда - намерение что-то изменить."""
    pass

class Query(ABC):
    """Базовый запрос - намерение что-то получить."""
    pass

class Handler(ABC):
    """
    Базовый обработчик команд или запросов.
    Метод handle должен быть асинхронным, если предполагаются I/O операции.
    """
    @abstractmethod
    async def handle(self, request: Command | Query) -> Any:
        raise NotImplementedError

class HandlerWs(ABC):
    """
    Базовый обработчик команд или запросов по WebSocket.
    Метод handle должен быть асинхронным, если предполагаются I/O операции.
    """
    @abstractmethod
    async def handle(self, request: Command | Query) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def listen(self, request: Command | Query) -> Any:
        raise NotImplementedError


@dataclass(frozen=True)
class OrderBook:
    """Доменная сущность ордербука."""
    bids: List[Tuple[Decimal, Decimal]]  # (цена, объём)
    asks: List[Tuple[Decimal, Decimal]]  # (цена, объём)

    def __post_init__(self):
        if not self.bids or not self.asks:
            raise InvalidOrderBookError("Order book must contain at least one bid and one ask")
        for price, volume in self.bids + self.asks:
            if price <= 0:
                raise InvalidPriceError(f"Invalid price: {price}")
            if volume <= 0:
                raise InvalidVolumeError(f"Invalid volume: {volume}")

@dataclass(frozen=True)
class DeltaResult:
    """Результат вычисления дельты."""
    total_delta: Decimal
    level_deltas: List[Tuple[Decimal, Decimal]]



@dataclass(frozen=True)
class Order:
    """Доменная сущность ордера."""
    id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    is_buy: bool

# --- Команды ---
@dataclass
class PlaceOrderCommand(Command):
    """Команда для размещения ордера."""
    symbol: str
    quantity: float
    price: float
    is_buy: bool

@dataclass
class RunBacktestCommand(Command):
    """Команда для запуска бэктеста."""
    symbol: str | None
    start_date: str
    end_date: str

@dataclass
class CollectOrderbookDataCommand(Command): # Команда для сбора данных
    """Команда для запуска циклического сбора данных ордербука."""
    symbol: str
    duration: int
    interval: int
    limit: int = 200

# --- Запросы ---
@dataclass
class GetOrderBookQuery(Query):
    """Запрос для получения ордербука и вычисления дельты."""
    symbol: str
    max_levels: int = 10

@dataclass
class GetBacktestReportQuery(Query):
    """Запрос для получения отчёта по бэктесту."""
    symbol: str | None
    start_date: str
    end_date: str

@dataclass
class GetOrderbookDataQuery(Query):
    """Запрос для получения ранее собранных данных ордербука."""
    symbol: Optional[str] = None 
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None