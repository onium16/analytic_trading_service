import pytest
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch
from infrastructure.adapters.bybit_api_client import BybitClient
from infrastructure.config.settings import settings
import pytest_asyncio

@pytest_asyncio.fixture
async def client_session():
    async with aiohttp.ClientSession() as session:
        yield session

@pytest.mark.asyncio
async def test_get_orderbook_snapshot_success(client_session):
    
    client = BybitClient(client_session)
    symbol = "BTCUSDT"
    limit = 200
    mock_response_data = { 
        "result": {
            "s": "BTCUSDT",
            "b": [["50000", "1.0"], ["49999", "0.5"]],
            "a": [["50001", "0.8"], ["50002", "1.2"]]
        },
        "retCode": 0,
        "retMsg": "OK"
    }
    
    settings.bybit.orderbook_url = "https://api.bybit.com/v5/market/orderbook"
    
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_response_data 
    mock_resp.text.return_value = "OK" 
    
    mock_get_coroutine = AsyncMock() 
    mock_get_coroutine.__aenter__.return_value = mock_resp 
    mock_get_coroutine.__aexit__.return_value = None 
    
    with patch.object(client_session, "get", return_value=mock_get_coroutine) as mock_get:
        
        result = await client.get_orderbook_snapshot(symbol, limit)
        
        
        mock_get.assert_called_once_with(
            settings.bybit.orderbook_url,
            params={"category": "linear", "symbol": symbol, "limit": str(limit)}
        )
        assert result == mock_response_data 

@pytest.mark.asyncio
async def test_get_orderbook_snapshot_error(client_session):
    
    client = BybitClient(client_session)
    symbol = "BTCUSDT"
    settings.bybit.orderbook_url = "https://api.bybit.com/v5/market/orderbook"
    
    mock_resp = AsyncMock()
    mock_resp.status = 400
    mock_resp.text.return_value = "Invalid symbol" 
    mock_resp.json.side_effect = aiohttp.ContentTypeError(None, None) 
    
    mock_get_coroutine = AsyncMock()
    mock_get_coroutine.__aenter__.return_value = mock_resp
    mock_get_coroutine.__aexit__.return_value = None
    
    with patch.object(client_session, "get", return_value=mock_get_coroutine) as mock_get:
        
        with pytest.raises(Exception, match="Error fetching orderbook snapshot: 400 Invalid symbol"):
            await client.get_orderbook_snapshot(symbol)

@pytest.mark.asyncio
async def test_get_kline_snapshot_success(client_session):
    
    client = BybitClient(client_session)
    symbol = "BTCUSDT"
    interval = "1"
    mock_response_data = {
        "result": {
            "list": [
                ["1631234567890", "50000", "51000", "49000", "50500", "1000", "5000000"],
                ["1631234627890", "50500", "51500", "49500", "51000", "1200", "6000000"]
            ]
        },
        "retCode": 0,
        "retMsg": "OK"
    }
    
    settings.bybit.kline_url = "https://api.bybit.com/v5/market/kline"
    
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_response_data
    mock_resp.text.return_value = "OK"

    mock_get_coroutine = AsyncMock()
    mock_get_coroutine.__aenter__.return_value = mock_resp
    mock_get_coroutine.__aexit__.return_value = None
    
    with patch.object(client_session, "get", return_value=mock_get_coroutine) as mock_get:
        
        result = await client.get_kline_snapshot(symbol, interval)
        
        mock_get.assert_called_once_with(
            settings.bybit.kline_url,
            params={"category": "linear", "symbol": symbol, "interval": interval, "limit": "2"}
        )
        assert result == mock_response_data

@pytest.mark.asyncio
async def test_get_kline_snapshot_error(client_session):
    
    client = BybitClient(client_session)
    symbol = "BTCUSDT"
    settings.bybit.kline_url = "https://api.bybit.com/v5/market/kline"
    
    mock_resp = AsyncMock()
    mock_resp.status = 400
    mock_resp.text.return_value = "Invalid interval"
    mock_resp.json.side_effect = aiohttp.ContentTypeError(None, None)

    mock_get_coroutine = AsyncMock()
    mock_get_coroutine.__aenter__.return_value = mock_resp
    mock_get_coroutine.__aexit__.return_value = None
    
    with patch.object(client_session, "get", return_value=mock_get_coroutine) as mock_get:
        
        with pytest.raises(Exception, match="Error fetching kline snapshot: 400 Invalid interval"):
            await client.get_kline_snapshot(symbol)

@pytest.mark.asyncio
async def test_get_multiple_snapshots_universal(client_session):
    
    client = BybitClient(client_session)
    duration = 2
    interval_iteration = 1.0
    symbol = "BTCUSDT"
    mock_response_data = {"result": {"s": "BTCUSDT", "price": 50000}}

    mock_func = AsyncMock(side_effect=[mock_response_data, mock_response_data]) 
    
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_response_data
    mock_resp.text.return_value = "OK"

    mock_get_coroutine = AsyncMock()
    mock_get_coroutine.__aenter__.return_value = mock_resp
    mock_get_coroutine.__aexit__.return_value = None

    with patch.object(client_session, "get", return_value=mock_get_coroutine): 
        
        snapshots = []

        async for snapshot in client.get_multiple_snapshots_universal(
            mock_func,
            duration=duration,
            interval_iteration=interval_iteration,
            symbol=symbol
        ):
            snapshots.append(snapshot)
    
        assert len(snapshots) == 2
        assert snapshots[0] == mock_response_data
        assert snapshots[1] == mock_response_data
        assert mock_func.call_count == 2
        mock_func.assert_any_call(symbol=symbol)

@pytest.mark.asyncio
async def test_get_multiple_snapshots_universal_handles_error(client_session):
    
    client = BybitClient(client_session)
    duration = 2
    interval_iteration = 1.0
    symbol = "BTCUSDT"

    mock_func = AsyncMock(side_effect=Exception("API error"))
    
    mock_resp_error = AsyncMock()
    mock_resp_error.status = 500
    mock_resp_error.text.return_value = "Internal Server Error"
    mock_resp_error.json.side_effect = aiohttp.ContentTypeError(None, None)

    mock_get_coroutine_error = AsyncMock()
    mock_get_coroutine_error.__aenter__.return_value = mock_resp_error
    mock_get_coroutine_error.__aexit__.return_value = None

    with patch.object(client_session, "get", return_value=mock_get_coroutine_error):
        
        snapshots = []
        async for snapshot in client.get_multiple_snapshots_universal(
            mock_func,
            duration=duration,
            interval_iteration=interval_iteration,
            symbol=symbol
        ):
            snapshots.append(snapshot)
        
        
        assert len(snapshots) == 0
        assert mock_func.call_count == 2