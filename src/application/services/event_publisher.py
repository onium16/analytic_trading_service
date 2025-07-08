# src/application/services/event_publisher.py

import asyncio
from collections import defaultdict
from typing import Any, Callable, Dict, List, Type

from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__)

class EventPublisher:
    """
    Класс для публикации и подписки на события.
    Позволяет различным компонентам системы взаимодействовать асинхронно
    через события, уменьшая прямую связанность.
    """

    def __init__(self):
        """
        Инициализирует издателя событий.
        _subscribers: Словарь, где ключ - тип события, значение - список асинхронных колбэков.
        """
        self._subscribers: Dict[Type[Any], List[Callable[..., Any]]] = defaultdict(list)
        logger.info("EventPublisher инициализирован.")

    def subscribe(self, event_type: Type[Any], handler: Callable[..., Any]):
        """
        Подписывает обработчик на определенный тип события.

        Args:
            event_type (Type[Any]): Тип события (например, KlineDataReceivedEvent).
            handler (Callable[..., Any]): Асинхронная функция (корутина), которая будет вызвана
                                         при публикации события данного типа.
        """
        if not asyncio.iscoroutinefunction(handler):
            logger.warning(f"Обработчик {handler.__name__} для события {event_type.__name__} не является асинхронной функцией. "
                           "Рекомендуется использовать async def.")
        self._subscribers[event_type].append(handler)
        logger.debug(f"Обработчик {handler.__name__} подписан на событие {event_type.__name__}.")

    async def publish(self, event: Any):
        """
        Публикует событие всем подписанным обработчикам.
        Каждый обработчик вызывается как отдельная асинхронная задача,
        чтобы публикация не блокировалась медленными обработчиками.

        Args:
            event (Any): Экземпляр события, который будет опубликован.
                         Тип события используется для поиска подписчиков.
        """
        event_type = type(event)
        logger.debug(f"Публикация события типа: {event_type.__name__}")

        if event_type in self._subscribers:
            tasks = []
            for handler in self._subscribers[event_type]:
                try:
                    # Запускаем каждый обработчик как отдельную задачу asyncio.
                    # Это гарантирует, что медленный обработчик не заблокирует
                    # других обработчиков или основной цикл событий.
                    task = asyncio.create_task(handler(event))
                    tasks.append(task)
                    logger.debug(f"Задача для обработчика {handler.__name__} ({event_type.__name__}) создана.")
                except Exception as e:
                    logger.error(f"Ошибка при создании задачи для обработчика {handler.__name__} на событие {event_type.__name__}: {e}", exc_info=True)
            
            # Ждем завершения всех задач, но с return_exceptions=True,
            # чтобы ошибки в отдельных обработчиках не приводили к падению всего паблишера.
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                for task in tasks:
                    if task.done() and task.exception():
                        logger.error(f"Ошибка выполнения обработчика: {task.exception()}", exc_info=True)
        else:
            logger.debug(f"Нет подписчиков для события типа: {event_type.__name__}.")