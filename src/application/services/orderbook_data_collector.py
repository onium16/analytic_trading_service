from application.commands.collect_orderbook_api_data import CollectOrderbookApiDataCommand
from application.commands.collect_orderbook_ws_data import CollectOrderbookWsDataCommand
from application.handlers.collect_orderbook_api_data_handler import CollectOrderbookApiDataHandler
from application.handlers.collect_orderbook_ws_data_handler import CollectOrderbookWsDataHandler

class OrderbookDataCollector:
    def __init__(
        self,
        api_handler: CollectOrderbookApiDataHandler,
        ws_handler: CollectOrderbookWsDataHandler,
        use_ws: bool = False
    ):
        self.api_handler = api_handler
        self.ws_handler = ws_handler
        self.use_ws = use_ws

    async def collect(self, command: CollectOrderbookApiDataCommand | CollectOrderbookWsDataCommand):
        if self.use_ws:
            await self.ws_handler.handle(command)
        else:
            await self.api_handler.handle(command)
