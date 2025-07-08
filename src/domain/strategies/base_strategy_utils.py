import json
from typing import Any, Dict, Union
import pandas as pd
from pathlib import Path
from infrastructure.logging_config import setup_logger
from infrastructure.config.settings import settings


logger = setup_logger(__name__, level=settings.logger_level)

def is_sustained_positive(series: pd.Series, period: int) -> bool:
    if len(series) < period:
        return False
    return all(d > 0 for d in series[-period:])

def is_sustained_negative(series: pd.Series, period: int) -> bool:
    if len(series) < period:
        return False
    return all(d < 0 for d in series[-period:])

def extract_default_value(param_config: dict):
    if "type" not in param_config:
        raise ValueError(f"Отсутствует 'type' в конфигурации параметра: {param_config}")

    param_type = param_config["type"]

    if param_type in ("list", "integer", "float"):
        values = param_config.get("values")
        if values is None:
            raise ValueError(f"Для типа '{param_type}' должны быть указаны 'values': {param_config}")
        # Если values не список, оборачиваем в список
        if not isinstance(values, (list, tuple)):
            values = [values]
        if not values:
            raise ValueError(f"Для типа '{param_type}' 'values' не должен быть пустым: {param_config}")
        return values[0]

    elif param_type == "range":
        if "start" not in param_config:
            raise ValueError(f"Для типа 'range' должен быть указан 'start': {param_config}")
        return param_config["start"]

    else:
        raise ValueError(f"Неизвестный тип параметра: {param_type}")

def parse_strategy_params_from_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Загружает параметры стратегий из JSON файла."""
    file_path = Path(file_path)
    if not file_path.exists():
        logger.error(f"ОШИБКА: Файл конфигурации '{file_path}' не найден. Возвращаются пустые параметры.")
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"ОШИБКА: Ошибка парсинга JSON в файле '{file_path}': {e}")
        return {}
    except Exception as e:
        logger.error(f"ОШИБКА: Неизвестная ошибка при чтении файла '{file_path}': {e}")
        return {}

def check_required_fields(data, fields: list, period: int = 1) -> bool:
    """Проверяет, что в данных есть необходимые поля и нет NaN в последних period точках."""
    df = data.df if hasattr(data, 'df') else data

    for field in fields:
        if field not in df.columns:
            logger.error(f"ОШИБКА: Отсутствует требуемое поле: '{field}' в DataFrame.")
            return False

        if len(df[field]) < period:
            return False

        data_slice = df[field].iloc[-period:]
        if data_slice.isna().any():
            nan_values = data_slice[data_slice.isna()]
            # logger.debug(f"ПРЕДУПРЕЖДЕНИЕ: Поле '{field}' содержит NaN значения в последних {period} барах.")
            # logger.debug(f"Проблемные точки (NaN):\n{nan_values.to_string(header=False)}")
            return False

    return True