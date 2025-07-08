# src/domain/events/data_events.py

from dataclasses import dataclass
from typing import Any, Dict

@dataclass(frozen=True)
class KlineDataReceivedEvent:
    """
    Событие, возникающее при получении новых данных торговой свечи (Kline).
    """
    kline_data: Dict[str, Any]
    # Пример структуры kline_data:
    # {
    #   "symbol": "ETHUSDT",
    #   "interval": "1m",
    #   "start": 1678886400000, # Начало свечи в мс
    #   "open": "1700.0",
    #   "high": "1710.0",
    #   "low": "1695.0",
    #   "close": "1705.0",
    #   "volume": "100.5",
    #   "timestamp": 1678886459999 # Время получения данных или закрытия свечи
    # }


@dataclass(frozen=True) # frozen=True делает объекты неизменяемыми
class OrderbookSnapshotReceivedEvent:
    """
    Событие, возникающее при получении нового снимка стакана ордеров.
    """
    orderbook_data: Dict[str, Any]
    # Пример структуры orderbook_data:
    # {
    #   "symbol": "ETHUSDT",
    #   "bids": [["1700.5", "10.0"], ["1699.0", "5.0"]], # bid_price, bid_quantity
    #   "asks": [["1701.0", "12.0"], ["1702.5", "8.0"]], # ask_price, ask_quantity
    #   "timestamp": 1678886460000, # Время снимка в мс
    #   "update_id": 123456789 # Идентификатор обновления (если есть)
    #   "ts": 1678886460000 # аналог timestamp
    # }

# WebSocket events
@dataclass(frozen=True)
class KlineDataReceivedWsEvent:
    """
    Событие, возникающее при получении новых данных торговой свечи (Kline).
    """
    kline_data: Dict[str, Any]
    # Пример структуры kline_data:
    # {
    #   "symbol": "ETHUSDT",
    #   "interval": "1m",
    #   "start": 1678886400000, # Начало свечи в мс
    #   "open": "1700.0",
    #   "high": "1710.0",
    #   "low": "1695.0",
    #   "close": "1705.0",
    #   "volume": "100.5",
    #   "timestamp": 1678886459999 # Время получения данных или закрытия свечи
    # }
@dataclass(frozen=True) # frozen=True делает объекты неизменяемыми
class OrderbookSnapshotReceivedWsEvent:
    """
    Событие, возникающее при получении нового снимка стакана ордеров.
    """
    orderbook_data: Dict[str, Any]
    # Пример структуры orderbook_data:
    # {
    #   "symbol": "ETHUSDT",
    #   "bids": [["1700.5", "10.0"], ["1699.0", "5.0"]], # bid_price, bid_quantity
    #   "asks": [["1701.0", "12.0"], ["1702.5", "8.0"]], # ask_price, ask_quantity
    #   "timestamp": 1678886460000, # Время снимка в мс
    #   "update_id": 123456789 # Идентификатор обновления (если есть)
    #   "ts": 1678886460000 # аналог timestamp
    # }