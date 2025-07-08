from typing import Any, Dict, Union

from domain.strategies.base_strategy_utils import extract_default_value
from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings

logger = setup_logger(__name__, level=settings.logger_level)

class StrategyDefaultSettings:
    """
    Класс для хранения дефолтных параметров стратегии с доступом через точечную нотацию.
    """
    def __init__(self, strategy_name: str, all_strategy_params: Dict[str, Dict[str, Any]]):
        self._strategy_name = strategy_name
        self._all_strategy_params = all_strategy_params

        strategy_config = all_strategy_params.get(strategy_name, {})

        for param_name, param_config in strategy_config.items():
            # Проверяем, что параметр имеет корректную структуру
            if isinstance(param_config, dict) and "type" in param_config:
                try:
                    default_value = extract_default_value(param_config)
                    setattr(self, param_name, default_value)
                except ValueError as e:
                    logger.warning(f"ПРЕДУПРЕЖДЕНИЕ: Не удалось извлечь дефолтное значение для {strategy_name}.{param_name}: {e}")
            else:
                raise ValueError(
                    f"Параметр '{param_name}' стратегии '{strategy_name}' не имеет ожидаемой структуры "
                    f"(dict с ключом 'type'). Получено: {param_config}"
                )