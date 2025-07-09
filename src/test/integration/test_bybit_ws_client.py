import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from infrastructure.adapters.bybit_ws_client import BybitWebSocketClient
from infrastructure.config.settings import settings

@pytest.mark.asyncio
async def test_bybit_ws_client_connect_disconnect():
    
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    client = BybitWebSocketClient(ws_url)
    
    
    mock_ws = AsyncMock()
    with patch("websockets.connect", AsyncMock(return_value=mock_ws)):
        
        async with client:
            
            assert client._ws == mock_ws
            mock_ws.close = AsyncMock()
        
        mock_ws.close.assert_awaited_once()

@pytest.mark.asyncio
async def test_subscribe_to_kline():
    
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    client = BybitWebSocketClient(ws_url)
    symbol = "BTCUSDT"
    interval = "1"
    expected_msg = {"op": "subscribe", "args": [f"kline.{interval}.{symbol}"]}
    
    
    mock_ws = AsyncMock()
    mock_ws.send = AsyncMock()
    
    with patch("websockets.connect", AsyncMock(return_value=mock_ws)):
        async with client:
            
            await client.subscribe_to_kline(symbol, interval)
            
            
            mock_ws.send.assert_awaited_once_with(json.dumps(expected_msg))

@pytest.mark.asyncio
async def test_subscribe_to_orderbook():
    
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    client = BybitWebSocketClient(ws_url)
    symbol = "BTCUSDT"
    depth = 200
    expected_msg = {"op": "subscribe", "args": [f"orderbook.{depth}.{symbol}"]}
    
    
    mock_ws = AsyncMock()
    mock_ws.send = AsyncMock()
    
    with patch("websockets.connect", AsyncMock(return_value=mock_ws)):
        async with client:
            
            await client.subscribe_to_orderbook(symbol, depth)
        
            mock_ws.send.assert_awaited_once_with(json.dumps(expected_msg))

@pytest.mark.asyncio
async def test_listen_receives_messages():
    
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    client = BybitWebSocketClient(ws_url)
    mock_messages = [
        json.dumps({"type": "kline", "data": {"symbol": "BTCUSDT", "price": 50000}}),
        json.dumps({"type": "kline", "data": {"symbol": "BTCUSDT", "price": 51000}})
    ]
    
    mock_ws = AsyncMock()
    mock_ws.__aiter__.return_value = mock_messages
    
    with patch("websockets.connect", AsyncMock(return_value=mock_ws)):
        async with client:
            
            messages = []
            async for message in client.listen():
                messages.append(message)
            
            assert len(messages) == 2
            assert messages[0] == {"type": "kline", "data": {"symbol": "BTCUSDT", "price": 50000}}
            assert messages[1] == {"type": "kline", "data": {"symbol": "BTCUSDT", "price": 51000}}

@pytest.mark.asyncio
async def test_subscribe_without_connection_raises_error():
    
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    client = BybitWebSocketClient(ws_url)
    
    with pytest.raises(RuntimeError, match="WebSocket connection is not established"):
        await client.subscribe_to_kline("BTCUSDT", "1")
    
    with pytest.raises(RuntimeError, match="WebSocket connection is not established"):
        await client.subscribe_to_orderbook("BTCUSDT", 200)
