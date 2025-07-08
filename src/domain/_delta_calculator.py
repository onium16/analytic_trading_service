# src/domain/delta_calculator.py

from application.contracts import DeltaResult, OrderBook

class DeltaCalculator:
    """Сервис для вычисления дельты ордербука."""
    def calculate_delta(self, order_book: OrderBook, max_levels: int = 10) -> DeltaResult:
        """
        Вычисляет дельту ордербука.

        Args:
            order_book: Ордербук с bids и asks.
            max_levels: Максимальное количество уровней для анализа.

        Returns:
            DeltaResult: Общая дельта и дельта по уровням.
        """
        bids = order_book.bids[:min(max_levels, len(order_book.bids))]
        asks = order_book.asks[:min(max_levels, len(order_book.asks))]

        # Общая дельта
        bid_volume = sum(volume for _, volume in bids)
        ask_volume = sum(volume for _, volume in asks)
        total_delta = bid_volume - ask_volume

        # Дельта по уровням
        level_deltas = []
        min_levels = min(len(bids), len(asks))
        for i in range(min_levels):
            bid_price, bid_volume = bids[i]
            ask_price, ask_volume = asks[i]
            level_delta = bid_volume - ask_volume
            level_deltas.append((bid_price, level_delta))

        return DeltaResult(total_delta=total_delta, level_deltas=level_deltas)