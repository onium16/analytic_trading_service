# src/services/delta_analyzer.py
import sys
import time
import numpy as np
import pandas as pd
from decimal import Decimal
from typing import Any, Dict

from domain._delta_calculator import OrderBook, DeltaCalculator, DeltaResult
from infrastructure.config.settings import settings
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__, level=settings.logger_level)


class DeltaAnalyzer:
    def __init__(self, repository, delta_repository):
        
        self.repository = repository
        self.delta_repository = delta_repository
        self.calculator = DeltaCalculator()

    async def analyze_recent(self, symbol: str):
        records = await self.repository.get_last_n(symbol, 2)
        if len(records) < 2:
            return []

        prev, curr = records[-2], records[-1]

        orderbook = OrderBook(
            bids=[(Decimal(price), Decimal(volume)) for price, volume in curr.bids],
            asks=[(Decimal(price), Decimal(volume)) for price, volume in curr.asks]
        )
        deltas = []
        result: DeltaResult = self.calculator.calculate_delta(orderbook)
        deltas.append({
            "timestamp": curr.timestamp,  # обращение к атрибуту через точку
            "symbol": symbol,
            "total_delta": str(result.total_delta),
            "level_deltas": [(str(price), str(delta)) for price, delta in result.level_deltas]
        })

        return deltas

class DeltaAnalyzerArchive:
    def __init__(self) -> None:
        pass

    @staticmethod
    def bids_asks_to_dict(bids, asks) -> Dict[str, Dict[float, float]]:
        bids_dict = {float(price): float(qty) for price, qty in bids}
        asks_dict = {float(price): float(qty) for price, qty in asks}
        return {'bids': bids_dict, 'asks': asks_dict}

    @staticmethod
    def analyze_recent(df: pd.DataFrame) -> pd.DataFrame:
        df_full = df.copy(deep=True)
        df_full['orderbook_dict'] = [
            DeltaAnalyzerArchive.bids_asks_to_dict(b, a)
            for b, a in zip(df_full['b'], df_full['a'])
        ]
        df_full.drop(columns=['b', 'a'], inplace=True)
        df_full['uuid'] = range(len(df_full))
        # logger.info("df_full:")
        # logger.info(df_full.info())
        # logger.info(df_full.head())
        return df_full

    @staticmethod
    def analyze_orderbook_optimized(orderbook_dict: Dict[str, Dict[float, float]]) -> Dict[str, Any]:
        
        bids_prices = np.array(list(orderbook_dict['bids'].keys()), dtype=float)
        bids_volumes = np.array(list(orderbook_dict['bids'].values()), dtype=float)
        asks_prices = np.array(list(orderbook_dict['asks'].keys()), dtype=float)
        asks_volumes = np.array(list(orderbook_dict['asks'].values()), dtype=float)

        # logger.debug(bids_prices, bids_volumes, asks_prices, asks_volumes)
        
        num_bids = len(bids_volumes) if bids_volumes is not None else 0
        num_asks = len(asks_volumes) if asks_volumes is not None else 0

        total_bid_volume = bids_volumes.sum()
        total_ask_volume = asks_volumes.sum()

        min_bid_price = bids_prices.min() if num_bids > 0 else 0.0
        max_bid_price = bids_prices.max() if num_bids > 0 else 0.0
        min_ask_price = asks_prices.min() if num_asks > 0 else 0.0
        max_ask_price = asks_prices.max() if num_asks > 0 else 0.0

        avg_bid_volume = total_bid_volume / num_bids if num_bids > 0 else 0.0
        std_bid_volume = bids_volumes.std() if num_bids > 0 else 0.0
        cv_bid_volume = std_bid_volume / avg_bid_volume if avg_bid_volume != 0 else 0.0

        avg_ask_volume = total_ask_volume / num_asks if num_asks > 0 else 0.0
        std_ask_volume = asks_volumes.std() if num_asks > 0 else 0.0
        cv_ask_volume = std_ask_volume / avg_ask_volume if avg_ask_volume != 0 else 0.0

        top_10_bids_sum = np.sort(bids_volumes)[-10:].sum() if num_bids >= 10 else total_bid_volume
        top_10_asks_sum = np.sort(asks_volumes)[-10:].sum() if num_asks >= 10 else total_ask_volume

        top_10_bid_volume_ratio = top_10_bids_sum / total_bid_volume if total_bid_volume > 0 else 0.0
        top_10_ask_volume_ratio = top_10_asks_sum / total_ask_volume if total_ask_volume > 0 else 0.0

        top_10_bid_count_ratio = min(10, num_bids) / num_bids if num_bids > 0 else 0.0
        top_10_ask_count_ratio = min(10, num_asks) / num_asks if num_asks > 0 else 0.0

        return {
            'num_bids': num_bids,
            'num_asks': num_asks,
            'total_bid_volume': total_bid_volume,
            'total_ask_volume': total_ask_volume,
            'min_bid_price': min_bid_price,
            'max_bid_price': max_bid_price,
            'min_ask_price': min_ask_price,
            'max_ask_price': max_ask_price,
            'avg_bid_volume': avg_bid_volume,
            'avg_ask_volume': avg_ask_volume,
            'cv_bid_volume': cv_bid_volume,
            'cv_ask_volume': cv_ask_volume,
            'top_10_bid_volume_ratio': top_10_bid_volume_ratio,
            'top_10_ask_volume_ratio': top_10_ask_volume_ratio,
            'top_10_bid_count_ratio': top_10_bid_count_ratio,
            'top_10_ask_count_ratio': top_10_ask_count_ratio,
        }

    def archive_analyze(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        timestart = time.perf_counter()
        try:
            df_full = self.analyze_recent(df_raw)
        except Exception as e:
            logger.error(f"Failed to analyze recent data: {str(e)}")
            sys.exit(1)
        
        # logger.info(df_full.info())
        # logger.info(df_full.head())

        try:
            results_series = df_full['orderbook_dict'].apply(self.analyze_orderbook_optimized)

            results_df = pd.DataFrame(results_series.tolist())
            results_df['uuid'] = df_full['uuid']
        except Exception as e:
            logger.error(f"Failed to analyze orderbook data: {str(e)}")
            sys.exit(1)

            # logger.info(results_df.info())
            
        try:
            df_full = df_full.merge(results_df, on='uuid')
            df_full['num_bids'] = df_full['num_bids'].fillna(0).astype(int)
        except Exception as e:
            logger.error(f"Failed to merge dataframes: {str(e)}")
            sys.exit(1)

        try:
            df_full.sort_values(by=['s', 'ts'], inplace=True)

            df_full['delta_total_bid_volume'] = df_full.groupby('s')['total_bid_volume'].diff()
            df_full['delta_total_ask_volume'] = df_full.groupby('s')['total_ask_volume'].diff()

            df_full['current_total_delta'] = df_full['total_bid_volume'] - df_full['total_ask_volume']
            df_full['delta_total_delta'] = df_full.groupby('s')['current_total_delta'].diff()

            df_full['delta_top_10_volume_ratio'] = (
                df_full.groupby('s')['top_10_bid_volume_ratio'].diff() -
                df_full.groupby('s')['top_10_ask_volume_ratio'].diff()
            )
        except Exception as e:
            logger.error(f"Failed to calculate deltas: {str(e)}")
            sys.exit(1)

        timeend = time.perf_counter()
        logger.debug(f"--- Анализ завершён. Время выполнения: {timeend - timestart:.2f} сек ---")

        return df_full
