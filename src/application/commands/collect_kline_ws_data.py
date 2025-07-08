from datetime import datetime
from typing import Optional
from application.contracts import Command

class CollectKlineWsDataCommand(Command):
    def __init__(
            self,
            symbol: str,
            interval: str,
            duration: int | None = None,  # None = бесконечно
            start_time: Optional[datetime] = None
        ):
        self.symbol = symbol
        self.interval = interval  # например, "1", "5", "D"
        self.duration = duration    # сколько слушаем в секундах
        self.start_time = start_time

    def __str__(self):
        return f"CollectWsKlineDataCommand(symbol={self.symbol}, interval={self.interval}, duration={self.duration})"
