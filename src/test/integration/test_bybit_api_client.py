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
    # Arrange
    client = BybitClient(client_session)
    symbol = "BTCUSDT"
    limit = 200
    mock_response_data = { # Renamed to avoid confusion with mock_response object
        "result": {
            "s": "BTCUSDT",
            "b": [["50000", "1.0"], ["49999", "0.5"]],
            "a": [["50001", "0.8"], ["50002", "1.2"]]
        },
        "retCode": 0,
        "retMsg": "OK"
    }
    
    settings.bybit.orderbook_url = "https://api.bybit.com/v5/market/orderbook"
    
    # Mock HTTP response object (the 'resp' in 'async with ... as resp')
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_response_data # Use the data directly
    mock_resp.text.return_value = "OK" # Good practice to mock text too for robustness
    
    # Mock client_session.get() to return an async context manager
    # The return value of client_session.get() must support __aenter__ and __aexit__
    mock_get_coroutine = AsyncMock() # This mock represents the coroutine returned by session.get()
    mock_get_coroutine.__aenter__.return_value = mock_resp # __aenter__ returns the mock response object
    mock_get_coroutine.__aexit__.return_value = None # __aexit__ can return None
    
    with patch.object(client_session, "get", return_value=mock_get_coroutine) as mock_get:
        # Act
        result = await client.get_orderbook_snapshot(symbol, limit)
        
        # Assert
        mock_get.assert_called_once_with(
            settings.bybit.orderbook_url,
            params={"category": "linear", "symbol": symbol, "limit": str(limit)}
        )
        assert result == mock_response_data # Assert against the data

@pytest.mark.asyncio
async def test_get_orderbook_snapshot_error(client_session):
    # Arrange
    client = BybitClient(client_session)
    symbol = "BTCUSDT"
    settings.bybit.orderbook_url = "https://api.bybit.com/v5/market/orderbook"
    
    mock_resp = AsyncMock()
    mock_resp.status = 400
    mock_resp.text.return_value = "Invalid symbol" # For error messages
    mock_resp.json.side_effect = aiohttp.ContentTypeError(None, None) # Mock json() if client tries to call it on error
    
    mock_get_coroutine = AsyncMock()
    mock_get_coroutine.__aenter__.return_value = mock_resp
    mock_get_coroutine.__aexit__.return_value = None
    
    with patch.object(client_session, "get", return_value=mock_get_coroutine) as mock_get:
        # Act & Assert
        with pytest.raises(Exception, match="Error fetching orderbook snapshot: 400 Invalid symbol"):
            await client.get_orderbook_snapshot(symbol)

@pytest.mark.asyncio
async def test_get_kline_snapshot_success(client_session):
    # Arrange
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
        # Act
        result = await client.get_kline_snapshot(symbol, interval)
        
        # Assert
        mock_get.assert_called_once_with(
            settings.bybit.kline_url,
            params={"category": "linear", "symbol": symbol, "interval": interval, "limit": "2"}
        )
        assert result == mock_response_data

@pytest.mark.asyncio
async def test_get_kline_snapshot_error(client_session):
    # Arrange
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
        # Act & Assert
        with pytest.raises(Exception, match="Error fetching kline snapshot: 400 Invalid interval"):
            await client.get_kline_snapshot(symbol)

@pytest.mark.asyncio
async def test_get_multiple_snapshots_universal(client_session):
    # Arrange
    client = BybitClient(client_session)
    duration = 2
    interval_iteration = 1.0
    symbol = "BTCUSDT"
    mock_response_data = {"result": {"s": "BTCUSDT", "price": 50000}}
    
    # Mock orderbook snapshot function
    # This mock_func will internally use the patched client_session.get
    mock_func = AsyncMock(side_effect=[mock_response_data, mock_response_data]) # Provide 2 responses
    
    # We still need to mock the underlying client_session.get if mock_func calls into BybitClient methods
    # For this test, mock_func itself is mocked, so we just need to ensure it's called
    # and returns what we expect. The `patch.object(client_session, "get", ...)` from other tests
    # is not directly relevant *if* `mock_func` completely bypasses actual `BybitClient` calls.
    # However, if `mock_func` calls `client.get_orderbook_snapshot` or `client.get_kline_snapshot`,
    # then the patches for those methods (which internally use `client_session.get`) are needed.
    
    # To be safe, ensure the client_session.get is mocked for calls *within* BybitClient methods
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_response_data
    mock_resp.text.return_value = "OK"

    mock_get_coroutine = AsyncMock()
    mock_get_coroutine.__aenter__.return_value = mock_resp
    mock_get_coroutine.__aexit__.return_value = None

    with patch.object(client_session, "get", return_value=mock_get_coroutine): # Patch client_session.get
        # Act
        snapshots = []
        # The `mock_func` is directly awaited, so its `side_effect` dictates what it returns.
        # This particular test mocks `get_multiple_snapshots_universal` using a generic `mock_func`.
        # This setup seems slightly inconsistent with the method's purpose (which takes a BybitClient method).
        # Assuming `mock_func` *is* one of `client.get_orderbook_snapshot` or `client.get_kline_snapshot`
        # in the actual BybitClient implementation that calls `get_multiple_snapshots_universal`.
        # For the test, we're just mocking `mock_func`'s return value.
        
        async for snapshot in client.get_multiple_snapshots_universal(
            mock_func,
            duration=duration,
            interval_iteration=interval_iteration,
            symbol=symbol
        ):
            snapshots.append(snapshot)
        
        # Assert
        assert len(snapshots) == 2
        assert snapshots[0] == mock_response_data
        assert snapshots[1] == mock_response_data
        assert mock_func.call_count == 2
        mock_func.assert_any_call(symbol=symbol)

@pytest.mark.asyncio
async def test_get_multiple_snapshots_universal_handles_error(client_session):
    # Arrange
    client = BybitClient(client_session)
    duration = 2
    interval_iteration = 1.0
    symbol = "BTCUSDT"
    
    # Mock function that raises an exception
    mock_func = AsyncMock(side_effect=Exception("API error"))
    
    # Ensure client_session.get is also mocked for any internal calls, if `mock_func` was a real method
    mock_resp_error = AsyncMock()
    mock_resp_error.status = 500
    mock_resp_error.text.return_value = "Internal Server Error"
    mock_resp_error.json.side_effect = aiohttp.ContentTypeError(None, None)

    mock_get_coroutine_error = AsyncMock()
    mock_get_coroutine_error.__aenter__.return_value = mock_resp_error
    mock_get_coroutine_error.__aexit__.return_value = None

    with patch.object(client_session, "get", return_value=mock_get_coroutine_error):
        # Act
        snapshots = []
        async for snapshot in client.get_multiple_snapshots_universal(
            mock_func,
            duration=duration,
            interval_iteration=interval_iteration,
            symbol=symbol
        ):
            snapshots.append(snapshot)
        
        # Assert
        assert len(snapshots) == 0
        assert mock_func.call_count == 2