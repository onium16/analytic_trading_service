import json
from typing import Any, Dict, List, Union

def _parse_param(param: Dict[str, Any]) -> List[Union[int, float]]:
    """
    Парсит параметр стратегии из формата с описанием типа в список значений.

    Поддерживаемые типы параметров:
    - "range": создаёт список чисел по диапазону с шагом (start, stop, step).
    - "list": просто возвращает список значений.

    Args:
        param (Dict[str, Any]): Словарь с описанием параметра, который содержит
            ключ "type" и параметры диапазона или список значений.

    Returns:
        List[Union[int, float]]: Список числовых значений, полученных из параметра.

    Raises:
        ValueError: Если тип параметра неизвестен или отсутствует.
    """
    param_type = param.get("type")
    if param_type == "range":
        start = param["start"]
        stop = param["stop"]
        step = param["step"]
        return list(range(start, stop, step))
    elif param_type == "list":
        return param["values"]
    else:
        raise ValueError(f"Unknown param type: {param_type}")


def _parse_strategy_params(
    raw_params: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, List[Union[int, float]]]]:
    """
    Парсит словарь стратегий с параметрами в удобный формат — словарь с параметрами в виде списков значений.

    Args:
        raw_params (Dict[str, Dict[str, Any]]): Словарь, где ключ — имя стратегии,
            значение — словарь параметров с описаниями.

    Returns:
        Dict[str, Dict[str, List[Union[int, float]]]]: Словарь стратегий с параметрами,
            где каждый параметр — список числовых значений.
    """
    parsed = {}
    for strategy_name, params in raw_params.items():
        parsed[strategy_name] = {k: _parse_param(v) for k, v in params.items()}
    return parsed

def parse_strategy_params_from_file(filepath: str):

    with open(filepath, "r", encoding="utf-8") as f:
        raw_params = json.load(f)
    return _parse_strategy_params(raw_params)
