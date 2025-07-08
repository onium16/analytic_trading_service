from datetime import datetime
from typing import Optional
from application.contracts import Command

class CollectOrderbookWsDataCommand(Command):
    def __init__(
            self,
            symbol: str,
            duration: Optional[int] = None,  # None = бесконечно
            depth: int = 200,
            start_time: Optional[datetime] = None
        ):
        self.symbol = symbol
        self.duration = duration  # сколько слушаем в секундах
        self.depth = depth
        self.start_time = start_time

    def __str__(self):
        return f"CollectOrderbookWsDataCommand(symbol={self.symbol}, duration={self.duration}, depth={self.depth})"
