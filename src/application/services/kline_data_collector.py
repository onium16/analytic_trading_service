from application.commands.collect_kline_api_data import CollectKlineApiDataCommand
from application.commands.collect_kline_ws_data import CollectKlineWsDataCommand
from application.handlers.collect_kline_api_data_handler import CollectKlineApiDataHandler
from application.handlers.collect_kline_ws_data_handler import CollectKlineWsDataHandler
from application.services.event_publisher import EventPublisher


class KlineDataCollector:
    def __init__(
        self,
        api_handler: CollectKlineApiDataHandler,
        ws_handler: CollectKlineWsDataHandler,
        use_ws: bool = False
    ):
        self.api_handler = api_handler
        self.ws_handler = ws_handler
        self.use_ws = use_ws

    async def collect(self, command: CollectKlineApiDataCommand | CollectKlineWsDataCommand):
        if self.use_ws:
            await self.ws_handler.handle(command)
        else:
            await self.api_handler.handle(command)
