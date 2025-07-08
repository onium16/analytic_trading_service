import pytest
from domain._delta_calculator import DeltaCalculator
from application.contracts import OrderBook, DeltaResult
from domain._exceptions import InvalidOrderBookError


def test_calculate_delta_basic():
    bids = [(100.0, 500), (99.9, 300), (99.8, 200)]
    asks = [(100.1, 400), (100.2, 250), (100.3, 150)]
    order_book = OrderBook(bids=bids, asks=asks)

    calculator = DeltaCalculator()
    result: DeltaResult = calculator.calculate_delta(order_book)

    expected_total_delta = (500 + 300 + 200) - (400 + 250 + 150)
    expected_level_deltas = [
        (100.0, 500 - 400),
        (99.9, 300 - 250),
        (99.8, 200 - 150)
    ]

    assert result.total_delta == expected_total_delta
    assert result.level_deltas == expected_level_deltas


def test_calculate_delta_limited_levels():
    bids = [(100.0, 500), (99.9, 300), (99.8, 200)]
    asks = [(100.1, 400), (100.2, 250), (100.3, 150)]
    order_book = OrderBook(bids=bids, asks=asks)

    calculator = DeltaCalculator()
    result = calculator.calculate_delta(order_book, max_levels=2)

    expected_total_delta = (500 + 300) - (400 + 250)
    expected_level_deltas = [
        (100.0, 500 - 400),
        (99.9, 300 - 250),
    ]

    assert result.total_delta == expected_total_delta
    assert result.level_deltas == expected_level_deltas


def test_calculate_delta_with_uneven_levels():
    bids = [(100.0, 500), (99.9, 300)]  # 2 levels
    asks = [(100.1, 400), (100.2, 250), (100.3, 100)]  # 3 levels
    order_book = OrderBook(bids=bids, asks=asks)

    calculator = DeltaCalculator()
    result = calculator.calculate_delta(order_book)

    expected_total_delta = (500 + 300) - (400 + 250 + 100)
    expected_level_deltas = [
        (100.0, 500 - 400),
        (99.9, 300 - 250)
    ]

    assert result.total_delta == expected_total_delta
    assert result.level_deltas == expected_level_deltas


def test_calculate_delta_empty_book():
    with pytest.raises(InvalidOrderBookError, match="at least one bid and one ask"):
        OrderBook(bids=[], asks=[])
