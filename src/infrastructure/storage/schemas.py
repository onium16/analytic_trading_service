# src/infrastructure/storage/schemas.py
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timezone

class OrderbookRecord(BaseModel):
    timestamp: int
    symbol: str
    side: str
    price: float
    size: float

class OrderBookSnapshot(BaseModel):
    timestamp: datetime
    symbol: str
    bids: List[Tuple[Decimal, Decimal]]
    asks: List[Tuple[Decimal, Decimal]]

class OrderBookFilenameModel(BaseModel):
    data: Optional[str] = None
    data_processed: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class OrderBookDelta(BaseModel):
    timestamp: datetime
    symbol: str
    total_delta: Decimal
    level_deltas: List[Tuple[Decimal, Decimal]]

class TradeSignal(BaseModel):
    timestamp: datetime
    strategy: str
    parammeters: str
    symbol: str
    action: str  # e.g. "buy", "sell"
    volume: Decimal

class TradeResult(BaseModel):
    timestamp: datetime
    symbol: str
    action: str  # e.g. "buy", "sell"
    volume: Decimal
    price: Decimal
    result: Decimal

class ReportSchema(BaseModel):
    total_return: float
    avg_return: float
    number_of_trades: int
 
class OrderbookSnapshotModel(BaseModel):
    timestamp: datetime = Field(alias="ts")
    topic: str
    type: str
    ts: int 
    cts: int 
    s: str
    u: int
    seq: int
    uuid: int
    num_bids: Optional[int] = None
    num_asks: Optional[int] = None
    total_bid_volume: Optional[float] = None
    total_ask_volume: Optional[float] = None
    min_bid_price: Optional[float] = None
    max_bid_price: Optional[float] = None
    min_ask_price: Optional[float] = None
    max_ask_price: Optional[float] = None
    avg_bid_volume: Optional[float] = None
    avg_ask_volume: Optional[float] = None
    cv_bid_volume: Optional[float] = None
    cv_ask_volume: Optional[float] = None
    top_10_bid_volume_ratio: Optional[float] = None
    top_10_ask_volume_ratio: Optional[float] = None
    top_10_bid_count_ratio: Optional[float] = None
    top_10_ask_count_ratio: Optional[float] = None
    delta_total_bid_volume: Optional[float] = None
    delta_total_ask_volume: Optional[float] = None
    current_total_delta: Optional[float] = None
    delta_total_delta: Optional[float] = None
    delta_top_10_volume_ratio: Optional[float] = None

class KlineRecord(BaseModel):
   symbol: str
   timestamp: datetime
   open: float
   high: float
   low: float
   close: float
   volume: float
   interval: str

class KlineRecordDatetime(BaseModel):
    data: str  
    data_processed: datetime 
    interval: str  # Интервал (например, '1m')
    exchange: str  # Имя биржи
    symbol: str

class KlineArchive(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str

class OrderBookArchive(BaseModel):
    timestamp: datetime
    ts: int 
    delta_total_bid_volume: float
    delta_total_ask_volume: float
    current_total_delta: float
    delta_total_delta: float
    cv_bid_volume: float
    cv_ask_volume: float
    top_10_bid_volume_ratio: float
    top_10_ask_volume_ratio: float
    delta_top_10_volume_ratio: float
    top_10_bid_count_ratio: float
    top_10_ask_count_ratio: float
    min_bid_price: float
    max_ask_price: float
    symbol: str
