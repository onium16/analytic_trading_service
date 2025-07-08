# src/application/commands/collect_orderbook_api_data.py

from application.contracts import Command

class CollectOrderbookApiDataCommand(Command):
    def __init__(
            self,
            symbol: str,
            limit: int,
            duration: int = 20,
            interval_iteration: float = 1.0
        ):
        self.symbol = symbol
        self.limit = limit  # Limit size for each bid and ask default 200
        self.duration = duration  # сколько слушаем в секундах
        self.interval_iteration = interval_iteration  # интервал между запросами

    def __str__(self):
        return (f"CollectOrderbookApiDataCommand(symbol={self.symbol}, limit={self.limit}, "
                f"duration={self.duration}, interval_iteration={self.interval_iteration})")
