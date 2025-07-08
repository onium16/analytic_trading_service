# src/application/commands/collect_kline_api_data.py

from application.contracts import Command

class CollectKlineApiDataCommand(Command):
    def __init__(
            self,
            symbol: str,
            interval: str,
            duration: int,
            interval_iteration: float,

        ):
        self.symbol = symbol
        self.interval = interval  # например, Kline interval. 1,3,5,15,30,60,120,240,360,720,D,W,M
        self.duration = duration  
        self.interval_iteration = interval_iteration  

    def __str__(self):
        return f"CollectKlineApiDataCommand(symbol={self.symbol}, interval={self.interval})"
