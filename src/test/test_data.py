# src/test/test_data.py
import pandas as pd

# Данные для мокирования OrderbookSnapshotModel
MOCK_ORDERBOOK_SNAPSHOT_DATA = pd.DataFrame([
    {
        "ts": 1678886400000,
        "s": "ETHUSDT",
        "bid_price": 100.0,
        "bid_volume": 1000.0,
        "ask_price": 100.1,
        "ask_volume": 800.0,
        "topic": "orderbook.400.B",
        "type": "update",
        "cts": 1678886400000,
        "s": "ETHUSDT",
        "u": 12345,
        "seq": 1,
        "uuid": 111,
        "num_bids": None,
        "num_asks": None,
        "total_bid_volume": None,
        "total_ask_volume": None,
        "min_bid_price": None,
        "max_bid_price": None,
        "min_ask_price": None,
        "max_ask_price": None,
        "avg_bid_volume": None,
        "avg_ask_volume": None,
        "cv_bid_volume": None,
        "cv_ask_volume": None,
        "top_10_bid_volume_ratio": None,
        "top_10_ask_volume_ratio": None,
        "top_10_bid_count_ratio": None,
        "top_10_ask_count_ratio": None,
        "delta_total_bid_volume": None,
        "delta_total_ask_volume": None,
        "current_total_delta": None,
        "delta_total_delta": None,
        "delta_top_10_volume_ratio": None
    },
    {
        "ts": 1678886401000,
        "s": "ETHUSDT",
        "bid_price": 99.9,
        "bid_volume": 500.0,
        "ask_price": 100.2,
        "ask_volume": 400.0,
        "topic": "orderbook.400.B",
        "type": "update",
        "cts": 1678886401000,
        "s": "ETHUSDT",
        "u": 12346,
        "seq": 2,
        "uuid": 222,
        "num_bids": None,
        "num_asks": None,
        "total_bid_volume": None,
        "total_ask_volume": None,
        "min_bid_price": None,
        "max_bid_price": None,
        "min_ask_price": None,
        "max_ask_price": None,
        "avg_bid_volume": None,
        "avg_ask_volume": None,
        "cv_bid_volume": None,
        "cv_ask_volume": None,
        "top_10_bid_volume_ratio": None,
        "top_10_ask_volume_ratio": None,
        "top_10_bid_count_ratio": None,
        "top_10_ask_count_ratio": None,
        "delta_total_bid_volume": None,
        "delta_total_ask_volume": None,
        "current_total_delta": None,
        "delta_total_delta": None,
        "delta_top_10_volume_ratio": None
    }
])