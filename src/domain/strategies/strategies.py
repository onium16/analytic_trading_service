from pathlib import Path
from typing import Any, Dict, Union
from domain.strategies.base_strategy_utils import check_required_fields, is_sustained_positive, parse_strategy_params_from_file
from domain.strategies.strategy_default_settings import StrategyDefaultSettings
import pandas as pd
import numpy as np
from backtesting import Strategy, Backtest
from infrastructure.config.settings import BacktestingSettings

bt_settings = BacktestingSettings()

# Путь к DEFAULT конфигу 
CONFIG_PATH: Path = bt_settings.default_path


"""
структура блока
project_root/
├── ... (остальные файлы и папки)
└── src/
    ├── application/
    │   ├── backtest_runner.py  # Теперь это основной скрипт для запуска бэктестов
    │   └── ...
    ├── domain/
    │   └── strategies/
    │       ├── base_strategy_utils.py     # Общие утилиты
    │       ├── __init__.py                # Делает 'strategies' пакетом
    │       ├── strategies.py              # ВСЕ КЛАССЫ СТРАТЕГИЙ ЗДЕСЬ
    │       └── strategy_default_settings.py # Класс StrategyDefaultSettings и парсер конфига
    ├── infrastructure/
    │   └── config/
    │       ├── best_strategy_settings.json   # Лучшие параметры (после оптимизации)
    │       ├── custom_strategy_settings.json # Кастомные параметры
    │       ├── default_strategy_settings.json # Дефолтные параметры (единственный источник правды)
    │       └── settings.py                 # Содержит BacktestingSettings
    │   └── ...
    └── ...


1.  Файл настроек (`src/infrastructure/config/settings.py`):
    Содержит базовые настройки для бэктестинга, включая пути к файлам конфигурации
    и данным. Указывает на `default_strategy_settings.json`, `custom_strategy_settings.json`
    и `best_strategy_settings.json`.

2.  Файлы конфигурации параметров (`src/infrastructure/config/*.json`):
    -   `default_strategy_settings.json`: Является ЕДИНСТВЕННЫМ ИСТОЧНИКОМ ПРАВДЫ для
        дефолтных (начальных) значений параметров каждой стратегии.
        Параметры здесь описываются как 'range' (диапазон для оптимизации)
        или 'list' (список фиксированных значений, первое из которых - дефолтное).
    -   `custom_strategy_settings.json`: Содержит пользовательские параметры,
        которые переопределяют дефолтные для обычных запусков бэктестов.
        Это позволяет быстро тестировать изменения без оптимизации.
    -   `best_strategy_settings.json`: Пустой файл, который будет заполняться
        результатами лучшей оптимизированной стратегии.

3.  Вспомогательные утилиты (`src/domain/strategies/base_strategy_utils.py`):
    Содержит общие функции, используемые несколькими стратегиями,
    такие как `is_sustained_positive`, `is_sustained_negative`,
    `extract_default_value` и `check_required_fields`.
    Эти функции выделены для повторного использования и чистоты кода.

4.  Класс StrategyDefaultSettings (`src/domain/strategies/strategy_default_settings.py`):
    Этот файл содержит класс `StrategyDefaultSettings`, который динамически
    загружает дефолтные параметры для конкретной стратегии из `all_default_strategy_params`.
    Также здесь находится функция `parse_strategy_params_from_file`,
    которая отвечает за чтение JSON-файлов.
    `all_default_strategy_params`, `all_custom_strategy_params` и `all_best_strategy_params`
    загружаются ГЛОБАЛЬНО один раз при импорте модуля `strategy_default_settings.py`,
    чтобы избежать многократного чтения файлов.

5.  Классы стратегий (`src/domain/strategies/strategies.py`):
    ВСЕ классы стратегий теперь находятся в этом одном файле.
    -   Каждый класс стратегии является подклассом `backtesting.Strategy`.
    -   В методе `init()` каждой стратегии создается экземпляр `StrategyDefaultSettings`
        для этой конкретной стратегии.
    -   Используется вспомогательная функция `get_strategy_params`,
        которая устанавливает параметры стратегии с определенным приоритетом:
        а) Параметры, переданные напрямую в `bt.run()` или `bt.optimize()` (наивысший приоритет).
        б) Параметры из `best_strategy_settings.json`.
        в) Параметры из `custom_strategy_settings.json`.
        г) Дефолтные параметры из `default_strategy_settings.json` (наименьший приоритет).
    -   ВАЖНО: Дефолтные значения параметров, ранее указанные как переменные класса
        (например, `period = 5` прямо в `class S_BidAcc:`), УДАЛЕНЫ.
        Теперь единственный источник дефолтных значений — это `default_strategy_settings.json`.

6.  Основной скрипт запуска (`src/application/backtest_runner.py` - этот файл):
    -   Импортирует необходимые компоненты и все классы стратегий.
    -   Загружает исторические данные (OHLCV + кастомные индикаторы).
    -   Позволяет запускать бэктесты для отдельных стратегий с их параметрами
        (дефолтными, кастомными или лучшими).
    -   Позволяет запускать оптимизацию, где `backtesting.py` будет перебирать
        параметры согласно определениям 'range' в `default_strategy_settings.json`
        или переданным напрямую в `optimize()`.
    -   Результаты оптимизации (best_params) могут быть сохранены в
        `best_strategy_settings.json` для последующего использования.

Порядок работы:
----------------
1.  **Проверка структуры:** Убедитесь, что структура файлов и пути, указанные
    в `src/infrastructure/config/settings.py`, корректны относительно корня вашего проекта.
2.  **Настройка дефолтов:** Файл `src/infrastructure/config/default_strategy_settings.json`
    должен содержать все дефолтные параметры для каждой стратегии.
3.  **Настройка кастомов (опционально):** Если вы хотите использовать определенные
    параметры для тестов без оптимизации, измените `src/infrastructure/config/custom_strategy_settings.json`.
4.  **Запуск бэктеста:**
    -   Импортируйте нужную стратегию из `src/domain/strategies/strategies.py`.
    -   Создайте экземпляр `Backtest` и передайте данные и класс стратегии.
    -   Вызовите `bt.run()`. Параметры будут автоматически загружены с учетом приоритета.
5.  **Запуск оптимизации:**
    -   Вызовите `bt.optimize()`. Параметры для оптимизации (диапазоны) должны быть
        переданы как kwargs в `optimize()`. Эти параметры перекроют все остальные.
    -   После оптимизации вы можете сохранить `stats_optimized._strategy._params`
        в `best_strategy_settings.json` для дальнейшего использования.
    
"""

# Загружаем конфигурацию по умолчанию
all_default_strategy_params = parse_strategy_params_from_file(CONFIG_PATH)
    
# --- 1. Стратегии на основе delta_total_bid_volume ---
class S_BidAcc(Strategy):
    """
    Гипотеза:
    Устойчивый рост delta_total_bid_volume сигнализирует о накоплении покупок.

    Стратегия:
    BUY при устойчивом положительном delta_total_bid_volume с учётом средней дельты за период.
    Дополнительно фильтрация по CV (коэффициент вариации) и концентрации Top-10 бидов.

    Вывод:
    Открываем длинную позицию при подтверждении условий накопления.
    """
    parameters_strategy = StrategyDefaultSettings('S_BidAcc', all_default_strategy_params)

    period = parameters_strategy.period
    delta_threshold_mult = parameters_strategy.delta_threshold_mult
    cv_buy_threshold = parameters_strategy.cv_buy_threshold
    top10_buy_threshold = parameters_strategy.top10_buy_threshold

    def init(self):
        pass

    def next(self):
        required_fields = ['delta_total_bid_volume', 'cv_bid_volume', 'top_10_bid_volume_ratio']
        if not check_required_fields(self.data, required_fields, self.period):
            return

        current_delta_bid = self.data.delta_total_bid_volume[-1]
        current_cv_bid = self.data.cv_bid_volume[-1]
        current_top10_bid_ratio = self.data.top_10_bid_volume_ratio[-1]

        avg_delta = self.data.delta_total_bid_volume[-self.period:].mean()
        sustained_positive_delta = is_sustained_positive(self.data.delta_total_bid_volume, self.period)

        if (sustained_positive_delta and
            current_delta_bid > avg_delta * self.delta_threshold_mult and
            (current_cv_bid < self.cv_buy_threshold or
            current_top10_bid_ratio > self.top10_buy_threshold)):
            if not self.position:
                self.buy(size=10, tag="Buy_S_BidAcc")
        else:
            if self.position:
                self.position.close()

class S_AskAccumulationShort(Strategy):
    """
    Гипотеза:
    Устойчивый положительный delta_total_ask_volume сигнализирует о накоплении продаж (медвежье давление).

    Стратегия:
    SELL (открытие короткой позиции) при устойчивом положительном delta_total_ask_volume,
    с учётом средней дельты асков за период.
    Дополнительная фильтрация по CV (коэффициент вариации) аск-объема
    и концентрации Top-10 аск-бидов.

    Вывод:
    Открываем короткую позицию при подтверждении условий накопления продаж.
    """
    parameters_strategy = StrategyDefaultSettings('S_AskAccumulationShort', all_default_strategy_params)

    period = parameters_strategy.period
    delta_threshold_mult = parameters_strategy.delta_threshold_mult
    cv_sell_threshold = parameters_strategy.cv_sell_threshold
    top10_sell_threshold = parameters_strategy.top10_sell_threshold

    def init(self):
        pass
    
    def next(self):
        required_fields = ['delta_total_ask_volume', 'cv_ask_volume', 'top_10_ask_volume_ratio']
        if not check_required_fields(self.data, required_fields, self.period):
            return

        current_delta_ask = self.data.delta_total_ask_volume[-1]
        current_cv_ask = self.data.cv_ask_volume[-1]
        current_top10_ask_ratio = self.data.top_10_ask_volume_ratio[-1]
        avg_delta_ask = self.data.delta_total_ask_volume[-self.period:].mean()
        sustained_positive_ask_delta = is_sustained_positive(self.data.delta_total_ask_volume, self.period)

        # Условия:
        # 1. Устойчивое накопление асков (положительная дельта)
        # 2. Текущая дельта асков выше средней дельты за период (подтверждение силы)
        # 3. Низкий CV асков (сделки концентрированы) ИЛИ высокая концентрация в Top-10 асков (крупные продажи)
        if (sustained_positive_ask_delta and
            current_delta_ask > avg_delta_ask * self.delta_threshold_mult and
            (current_cv_ask < self.cv_sell_threshold or
             current_top10_ask_ratio > self.top10_sell_threshold)):
                        
            # Открываем короткую позицию, если у нас нет открытых позиций
            if not self.position:
                self.sell(size=10, tag="Sell_S_AskAcc") # Открытие короткой позиции

        else:
            if self.position.is_short:
                self.position.close() # Закрываем короткую позицию

class S_BidExh(Strategy):
    """
    Гипотеза:
    Резкий спад delta_total_bid_volume сопровождается истощением покупательской активности.

    Стратегия:
    SELL или закрытие лонга при резком отрицательном delta_total_bid_volume и росте CV.

    Вывод:
    Используется для скальпинга или выхода из длинной позиции.
    """
    parameters_strategy = StrategyDefaultSettings('S_BidExh', all_default_strategy_params)

    reversal_delta_threshold = parameters_strategy.reversal_delta_threshold
    cv_increase_mult = parameters_strategy.cv_increase_mult

    def init(self):
        pass

    def next(self):
        required_fields = ['delta_total_bid_volume', 'cv_bid_volume']
        if not check_required_fields(self.data, required_fields, 5):
            return

        current_delta_bid = self.data.delta_total_bid_volume[-1]
        current_cv_bid = self.data.cv_bid_volume[-1]
        avg_cv_prev = self.data.cv_bid_volume[-5:-1].mean()

        if current_delta_bid < self.reversal_delta_threshold:
            if current_cv_bid > avg_cv_prev * (1 + self.cv_increase_mult):
                if self.position.is_long:
                    self.position.close()
                else:
                    self.sell(size=10, tag="Sell_S_BidExh")


class S_BidWall(Strategy):
    """
    Гипотеза:
    Большие значения delta_total_bid_volume с высокой концентрацией Top-10 бидов формируют "стены".

    Стратегия:
    Определяем стену по порогам delta, CV и концентрации Top-10.
    При приближении цены к стене — покупаем, если стена держит; продаём, если пробивает.

    Вывод:
    Реакция на манипуляции и защиту уровней бидов.
    """
    parameters_strategy = StrategyDefaultSettings('S_BidWall', all_default_strategy_params)

    delta_bid_threshold = parameters_strategy.delta_bid_threshold
    cv_high = parameters_strategy.cv_high
    top10_high = parameters_strategy.top10_high
    price_approach_zone = parameters_strategy.price_approach_zone

    _wall_detected = False
    _wall_price = 0.0

    def init(self):
        pass

    def next(self):
        required_fields = ['delta_total_bid_volume', 'cv_bid_volume', 'top_10_bid_volume_ratio', 'min_bid_price', 'Close']
        if not check_required_fields(self.data, required_fields, 3):
            return

        current_delta_bid = self.data.delta_total_bid_volume[-1]
        current_cv_bid = self.data.cv_bid_volume[-1]
        current_top10_bid_ratio = self.data.top_10_bid_volume_ratio[-1]
        current_min_bid_price = self.data.min_bid_price[-1]
        current_price = self.data.Close[-1]

        if self._wall_detected:
            if current_price >= self._wall_price * (1 - self.price_approach_zone * 0.5):
                if not self.position.is_long:
                    self.buy(size=10, tag="Buy_S_BidWall")
            else:
                if not self.position.is_short:
                    self.sell(tag="Sell_S_BidWall")
            self._wall_detected = False

        if (current_delta_bid > self.delta_bid_threshold and
            current_cv_bid > self.cv_high and
            current_top10_bid_ratio > self.top10_high):
            if current_min_bid_price and (current_price - current_min_bid_price) / current_price < self.price_approach_zone:
                self._wall_detected = True
                self._wall_price = current_min_bid_price



# --- 2. Стратегии на основе delta_total_ask_volume ---

class S_AskAcc(Strategy):
    """
    Гипотеза:
    Устойчивый рост delta_total_ask_volume сигнализирует о накоплении продаж.

    Стратегия:
    SELL при устойчивом положительном delta_total_ask_volume с фильтрацией по CV и Top-10 асков.

    Вывод:
    Открываем короткую позицию при подтверждении условий.
    """
    parameters_strategy = StrategyDefaultSettings('S_AskAcc', all_default_strategy_params)

    period = parameters_strategy.period
    delta_threshold_mult = parameters_strategy.delta_threshold_mult
    cv_sell_threshold = parameters_strategy.cv_sell_threshold
    top10_sell_threshold = parameters_strategy.top10_sell_threshold

    def init(self):
        # Правильно: получаем 'period' из _params, если нет, используем self.period (по умолчанию)
        self.period = getattr(self._params, 'period', self.period)
        
        # Правильно: получаем 'delta_threshold_mult' из _params, если нет, используем self.delta_threshold_mult (по умолчанию)
        self.delta_threshold_mult = getattr(self._params, 'delta_threshold_mult', self.delta_threshold_mult) 
        
        self.cv_sell_threshold = getattr(self._params, 'cv_sell_threshold', self.cv_sell_threshold)
        self.top10_sell_threshold = getattr(self._params, 'top10_sell_threshold', self.top10_sell_threshold)
        pass

    def next(self):
        required_fields = ['delta_total_ask_volume', 'cv_ask_volume', 'top_10_ask_volume_ratio']
        if not check_required_fields(self.data, required_fields, self.period):
            return

        current_delta_ask = self.data.delta_total_ask_volume[-1]
        current_cv_ask = self.data.cv_ask_volume[-1]
        current_top10_ask_ratio = self.data.top_10_ask_volume_ratio[-1]

        avg_delta = self.data.delta_total_ask_volume[-self.period:].mean()
        sustained_positive_delta = is_sustained_positive(self.data.delta_total_ask_volume, self.period)

        if (sustained_positive_delta and
            current_delta_ask > avg_delta * self.delta_threshold_mult and
            (current_cv_ask < self.cv_sell_threshold or
            current_top10_ask_ratio > self.top10_sell_threshold)):
            if not self.position:
                self.sell(tag="Sell_S_AskAcc")
        else:
            # Если условия не выполняются, закрываем позицию (если она открыта)
            if self.position and self.position.is_short:
                self.position.close()



class S_AskExh(Strategy):
    """
    Гипотеза:
    Резкий спад delta_total_ask_volume сопровождается истощением продавцов.

    Стратегия:
    BUY или закрытие шорта при резком отрицательном delta_total_ask_volume и росте CV.

    Вывод:
    Используется для скальпинга или выхода из короткой позиции.
    """
    parameters_strategy = StrategyDefaultSettings('S_AskExh', all_default_strategy_params)

    reversal_delta_threshold = parameters_strategy.reversal_delta_threshold
    cv_increase_mult = parameters_strategy.cv_increase_mult

    def init(self):
        pass

    def next(self):
        required_fields = ['delta_total_ask_volume', 'cv_ask_volume']
        if not check_required_fields(self.data, required_fields, 5):
            return

        current_delta_ask = self.data.delta_total_ask_volume[-1]
        current_cv_ask = self.data.cv_ask_volume[-1]
        avg_cv_prev = self.data.cv_ask_volume[-5:-1].mean()

        if current_delta_ask < self.reversal_delta_threshold:
            if current_cv_ask > avg_cv_prev * (1 + self.cv_increase_mult):
                if self.position.is_short:
                    self.position.close()
                else:
                    self.buy(size=10, tag="Buy_S_AskExh")

# --- 3. Стратегии на основе delta_total_delta ---
class S_ImbReversal(Strategy):
    """
    Гипотеза:
    Экстремальные значения current_total_delta указывают на потенциальный разворот.

    Стратегия:
    SELL при развороте delta_total_delta вниз после сильного бычьего дисбаланса,
    BUY — при развороте вверх после сильного медвежьего дисбаланса.

    Вывод:
    Торговля против тренда при обнаружении исчерпания дисбаланса.
    """
    parameters_strategy = StrategyDefaultSettings('S_ImbReversal', all_default_strategy_params)

    extreme_imb_threshold = parameters_strategy.extreme_imb_threshold
    reversal_delta_threshold = parameters_strategy.reversal_delta_threshold

    def init(self):
        pass

    def next(self):
        required_fields = ['current_total_delta', 'delta_total_delta']
        if not check_required_fields(self.data, required_fields, 3):
            return

        current_total_delta = self.data.current_total_delta[-1]
        delta_total_delta = self.data.delta_total_delta[-1]

        if (current_total_delta > self.extreme_imb_threshold and
            delta_total_delta < -self.reversal_delta_threshold):
            if not self.position.is_short:
                self.sell()

        if (current_total_delta < -self.extreme_imb_threshold and
            delta_total_delta > self.reversal_delta_threshold):
            if not self.position.is_long:
                self.buy(size=10, tag="Buy_S_ImbReversal")


# --- 4. Стратегии на основе delta_top_10_volume_ratio ---a

class S_LiquidityTrap(Strategy):
    """
    Гипотеза:
    Резкие изменения delta_top_10_volume_ratio при отсутствии движения цены и высоком CV — ловушка ликвидности.

    Стратегия:
    SELL если рост delta_top_10_volume_ratio и цена стоит,
    BUY если падение delta_top_10_volume_ratio и цена стоит,
    при условии высокого CV на стороне.

    Вывод:
    Используется для выявления и торговли ловушек ликвидности.
    """
    parameters_strategy = StrategyDefaultSettings('S_LiquidityTrap', all_default_strategy_params)

    delta_top10_large_change = parameters_strategy.delta_top10_large_change
    price_no_move_threshold = parameters_strategy.price_no_move_threshold
    cv_high = parameters_strategy.cv_high

    def init(self):
        pass

    def next(self):
        required_fields = ['delta_top_10_volume_ratio', 'cv_bid_volume', 'cv_ask_volume', 'Close']
        if not check_required_fields(self.data, required_fields, 2):
            return

        current_delta_top10 = self.data.delta_top_10_volume_ratio[-1]
        current_cv_bid = self.data.cv_bid_volume[-1]
        current_cv_ask = self.data.cv_ask_volume[-1]

        current_price = self.data.Close[-1]
        previous_price = self.data.Close[-2]
        price_change_pct = (current_price - previous_price) / previous_price if previous_price != 0 else 0

        if (current_delta_top10 > self.delta_top10_large_change and
            abs(price_change_pct) < self.price_no_move_threshold and
            current_cv_bid > self.cv_high):
            if not self.position.is_short:
                self.sell(tag="Sell_S_LiquidityTrap")

        if (current_delta_top10 < -self.delta_top10_large_change and
            abs(price_change_pct) < self.price_no_move_threshold and
            current_cv_ask > self.cv_high):
            if not self.position.is_long:
                self.buy(size=10, tag="Buy_S_LiquidityTrap")

