import asyncio
from typing import List, Dict, Type, Any, Union
from pathlib import Path
from application.services import event_publisher
import pandas as pd
import inspect
import json
from backtesting import Backtest, Strategy

from infrastructure.config.settings import BacktestingSettings
import domain.strategies.strategies as strategy_module  # модуль, где определены все стратегии

# Импорты для логирования и конфигурации
from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__, level=settings.logger_level)

class StrategyManager:
    def __init__(self):
        settings = BacktestingSettings()
        self.default_params_path = settings.default_path
        self.best_params_path = settings.best_path

        self.default_params = self._load_json(self.default_params_path)
        self.best_params = self._load_json(self.best_params_path)

        self.strategies = self._discover_strategies(strategy_module)

    def _load_json(self, path: Union[str, Path]) -> Dict[str, Any]:
        if Path(path).exists():
            with open(path, "r") as f:
                return json.load(f)
        return {}

    def _discover_strategies(self, module) -> Dict[str, Type[Strategy]]:
        strategies = {}
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, Strategy) and obj is not Strategy:
                strategies[name] = obj
        return strategies

    def _get_params_for_strategy(self, strategy_name: str) -> Dict[str, Any]:
        if strategy_name in self.best_params:
            return {k: v["values"][0] for k, v in self.best_params[strategy_name].items()}
        elif strategy_name in self.default_params:
            return {k: v["values"][0] for k, v in self.default_params[strategy_name].items()}
        else:
            raise ValueError(f"Параметры для стратегии {strategy_name} не найдены.")

    async def run_all(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for strategy_name, strategy_class in self.strategies.items():
            params = self._get_params_for_strategy(strategy_name)

            bt = Backtest(
                df.copy(),
                strategy_class,
                cash=10_000,
                exclusive_orders=True
            )

            try:
                df_strategy_results = bt.run(**params)
                logger.debug(df_strategy_results)

                logger.info(
                    f"Статистика для стратегии {strategy_name}: \n"
                    f" _strategy: {df_strategy_results['strategy'] if 'strategy' in df_strategy_results else 'N/A'}\n"
                    f" _signals: {df_strategy_results['signals'] if 'signals' in df_strategy_results else 'N/A'}\n"
                    f" _stats: {df_strategy_results['stats'] if 'stats' in df_strategy_results else 'N/A'}"
                )

                results.append({
                    "strategy_name": strategy_name,
                    "_strategy": df_strategy_results["strategy"] if "strategy" in df_strategy_results else None,
                    "params": params,
                    "_signals": df_strategy_results["signals"] if "signals" in df_strategy_results else None,
                    "_stats": df_strategy_results["signals"] if "signals" in df_strategy_results else None,

                    # опционально можно сохранить _signals/_stats если они есть
                })

            except Exception as e:
                logger.exception(f"Ошибка при выполнении стратегии {strategy_name}: {e}")

        

        # event_publisher.publish(results)
        return results
