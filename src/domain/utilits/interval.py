from domain.constants import VALID_INTERVALS


def _validate_interval(interval: str) -> str:
    """
    Приводит интервал к допустимому формату для Bybit WebSocket и проверяет его валидность.
    """
    interval = interval.strip().upper()

    # Удалим 'M' или 'm' только если она часть числового значения (например, '1m' -> '1')
    if interval.endswith("M") and interval[:-1].isdigit():
        interval = interval[:-1]
    elif interval.endswith("m"):
        interval = interval[:-1]

    if interval not in VALID_INTERVALS:
        raise ValueError(
            f"Интервал '{interval}' не поддерживается. "
            f"Допустимые значения: {sorted(VALID_INTERVALS)}"
        )

    return interval
