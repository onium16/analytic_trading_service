# src/application/trade_processor.py
from typing import Dict, Any, List
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__)

class TradingProcessor:
    def __init__(self):
        logger.info("Инициализация TradingProcessor")
        # Rиента биржи для выполнения ордеров
        # self.exchange_client = ExchangeClient()

    async def process_signal(self, signals_list: List[Dict[str, Any]]): 
            """
            Обрабатывает список сигналов от различных стратегий.
            """
            logger.info(f"TradingProcessor: Получен сигнал: {signals_list}")

            for signal_data in signals_list: 
                signal_type = signal_data.get('signal') 
                strategy_name = signal_data.get('strategy', 'UnknownStrategy') 
                reason = signal_data.get('reason', 'No specific reason.')
                entry_price = signal_data.get('entry_price')
                exit_conditions = signal_data.get('exit_conditions', {})

                if signal_type == "BUY":
                    logger.info(f"[{strategy_name}] BUY signal received. Entry price: {entry_price}. Reason: {reason}")
                    # Здесь ваша логика для открытия длинной позиции
                elif signal_type == "SELL":
                    logger.info(f"[{strategy_name}] SELL signal received. Entry price: {entry_price}. Reason: {reason}")
                    # Здесь ваша логика для открытия короткой позиции
                elif signal_type == "CLOSE_LONG":
                    logger.info(f"[{strategy_name}] CLOSE_LONG signal received. Reason: {reason}")
                    # Здесь ваша логика для закрытия длинной позиции
                elif signal_type == "CLOSE_SHORT":
                    logger.info(f"[{strategy_name}] CLOSE_SHORT signal received. Reason: {reason}")
                    # Здесь ваша логика для закрытия короткой позиции
                elif signal_type == "HOLD":
                    logger.debug(f"[{strategy_name}] HOLD signal received. Reason: {reason}")
                    # Ничего не делаем, просто держим
                elif signal_type == "ERROR":
                    error_message = signal_data.get('error_message', 'No error message provided.')
                    logger.error(f"[{strategy_name}] ERROR signal received. Error: {error_message}. Reason: {reason}")
                else:
                    logger.warning(f"[{strategy_name}] Unknown signal type: {signal_type}. Reason: {reason}")