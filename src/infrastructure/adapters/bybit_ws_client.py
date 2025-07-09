# src/infrastructure/adapters/bybit_ws_client.py

import websockets
import json
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__, level=settings.logger_level)

class BybitWebSocketClient:
    
    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self._ws = None

    async def __aenter__(self):
        self._ws = await websockets.connect(self.ws_url)
        logger.info(f"Connected to {self.ws_url}")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._ws:
            await self._ws.close()
            logger.info("WebSocket connection closed")

    async def listen(self):
        async for message in self._ws:
            yield json.loads(message)

    async def subscribe_to_kline(self, symbol: str, interval: str = "1"):
        if self._ws is None:
            raise RuntimeError("WebSocket connection is not established. Use 'async with' to connect.")
        msg = {
            "op": "subscribe",
            "args": [f"kline.{interval}.{symbol}"]
        }
        await self._ws.send(json.dumps(msg))
        logger.info(f"Subscribed to kline.{interval}.{symbol}")

    async def subscribe_to_orderbook(self, symbol: str, depth: int = 200):
        if self._ws is None:
            raise RuntimeError("WebSocket connection is not established. Use 'async with' to connect.")
        msg = {
            "op": "subscribe",
            "args": [f"orderbook.{depth}.{symbol}"]
        }
        await self._ws.send(json.dumps(msg))
        logger.info(f"Subscribed to orderbook.{depth}.{symbol}")

# async def main():
#     import asyncio
#     from application.commands.collect_kline_ws_data import CollectKlineWsDataCommand
#     from application.handlers.collect_kline_ws_data_handler import CollectKlineWsDataHandler
#     from infrastructure.config.settings import settings

    
#     ws_url = settings.bybit.ws_url
#     settings.pair_tokens = "ETHUSDT"
#     settings.kline_interval = "1"
#     settings.streaming.timer = 125
#     command = CollectKlineWsDataCommand(
#         symbol=settings.pair_tokens,
#         interval=settings.kline_interval,
#         duration=settings.streaming.timer  # слушаем N секунд если Nan то бесконечно
#     )

#     handler = CollectKlineWsDataHandler(ws_url)
#     klines = []
#     async for kline in handler.listen(command):
#         klines.append(kline)

# if __name__ == "__main__":
#     asyncio.run(main())

# async def main():


#     from application.commands.collect_orderbook_ws_data import CollectOrderbookWsDataCommand
#     from application.handlers.collect_orderbook_ws_data_handler import CollectOrderbookWsDataHandler
#     from infrastructure.config.settings import settings

#     ws_url = settings.bybit.ws_url
#     command = CollectOrderbookWsDataCommand(
#         symbol="BTCUSDT",
#         duration=30, 
#         depth=200
#     )

#     handler = CollectOrderbookWsDataHandler(ws_url)
#     snapshots = []
#     async for snapshot in handler.listen(command):
#         snapshots.append(snapshot)
#         logger.debug(snapshots)

# if __name__ == "__main__":
#     asyncio.run(main())