# test/unit/conftest.py
import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Импортируем реальный класс из его файла
from domain.strategies.strategy_default_settings import StrategyDefaultSettings as RealStrategyDefaultSettings

# Мокаем функции, которые реально нужны для импортов
def mock_extract_default_value(param_config):
    # Возвращаем первое значение из values или None
    return param_config.get("values", [None])[0]

def mock_check_required_fields(*args, **kwargs):
    # Заглушка, возвращаем True (или подходящее значение)
    return True

def mock_is_sustained_positive(*args, **kwargs):
    return True

def mock_parse_strategy_params_from_file(*args, **kwargs):
    # Возвращаем пустой или тестовый словарь с параметрами стратегий
    return {
                        "S_BidAcc": {
                            "period": {"type": "integer", "values": [5]},
                            "delta_threshold_mult": {"type": "float", "values": [1.0]},
                            "cv_buy_threshold": {"type": "float", "values": [1.0]},
                            "top10_buy_threshold": {"type": "float", "values": [0.8]}
                        },
                        "S_AskAccumulationShort": {
                            "period": {"type": "integer", "values": [5]},
                            "delta_threshold_mult": {"type": "float", "values": [1.0]},
                            "cv_sell_threshold": {"type": "float", "values": [1.0]},
                            "top10_sell_threshold": {"type": "float", "values": [0.8]}
                        },
                        "S_BidExh": {
                            "reversal_delta_threshold": {"type": "float", "values": [-70.0]},
                            "cv_increase_mult": {"type": "float", "values": [0.5]}
                        },
                        "S_BidWall": {
                            "delta_bid_threshold": {"type": "integer", "values": [100]},
                            "cv_high": {"type": "float", "values": [2.0]},
                            "top10_high": {"type": "float", "values": [0.9]},
                            "price_approach_zone": {"type": "float", "values": [0.005]}
                        },
                        "S_AskAcc": {
                            "period": {"type": "integer", "values": [5]},
                            "delta_threshold_mult": {"type": "float", "values": [1.0]},
                            "cv_sell_threshold": {"type": "float", "values": [1.0]},
                            "top10_sell_threshold": {"type": "float", "values": [0.8]}
                        },
                        "S_AskExh": {
                            "reversal_delta_threshold": {"type": "float", "values": [-70.0]},
                            "cv_increase_mult": {"type": "float", "values": [0.5]}
                        },
                        "S_ImbReversal": {
                            "extreme_imb_threshold": {"type": "integer", "values": [150]},
                            "reversal_delta_threshold": {"type": "integer", "values": [30]}
                        },
                        "S_LiquidityTrap": {
                            "delta_top10_large_change": {"type": "float", "values": [0.05]},
                            "price_no_move_threshold": {"type": "float", "values": [0.0001]},
                            "cv_high": {"type": "float", "values": [2.0]}
                        }
                        }

# Создаём мок-модуль domain.strategies.base_strategy_utils
mock_base_utils = type(sys)("domain.strategies.base_strategy_utils")
mock_base_utils.StrategyDefaultSettings = RealStrategyDefaultSettings
mock_base_utils.extract_default_value = mock_extract_default_value
mock_base_utils.check_required_fields = mock_check_required_fields
mock_base_utils.is_sustained_positive = mock_is_sustained_positive
mock_base_utils.parse_strategy_params_from_file = mock_parse_strategy_params_from_file

# Регистрируем мок-модуль в sys.modules, чтобы импорт проходил корректно
sys.modules["domain.strategies.base_strategy_utils"] = mock_base_utils

@pytest.fixture(scope="session", autouse=True)
def patch_global_dependencies():
    # Подготовка тестовых параметров стратегий
    strategy_params = {
                        "S_BidAcc": {
                            "period": {"type": "integer", "values": [5]},
                            "delta_threshold_mult": {"type": "float", "values": [1.0]},
                            "cv_buy_threshold": {"type": "float", "values": [1.0]},
                            "top10_buy_threshold": {"type": "float", "values": [0.8]}
                        },
                        "S_AskAccumulationShort": {
                            "period": {"type": "integer", "values": [5]},
                            "delta_threshold_mult": {"type": "float", "values": [1.0]},
                            "cv_sell_threshold": {"type": "float", "values": [1.0]},
                            "top10_sell_threshold": {"type": "float", "values": [0.8]}
                        },
                        "S_BidExh": {
                            "reversal_delta_threshold": {"type": "float", "values": [-70.0]},
                            "cv_increase_mult": {"type": "float", "values": [0.5]}
                        },
                        "S_BidWall": {
                            "delta_bid_threshold": {"type": "integer", "values": [100]},
                            "cv_high": {"type": "float", "values": [2.0]},
                            "top10_high": {"type": "float", "values": [0.9]},
                            "price_approach_zone": {"type": "float", "values": [0.005]}
                        },
                        "S_AskAcc": {
                            "period": {"type": "integer", "values": [5]},
                            "delta_threshold_mult": {"type": "float", "values": [1.0]},
                            "cv_sell_threshold": {"type": "float", "values": [1.0]},
                            "top10_sell_threshold": {"type": "float", "values": [0.8]}
                        },
                        "S_AskExh": {
                            "reversal_delta_threshold": {"type": "float", "values": [-70.0]},
                            "cv_increase_mult": {"type": "float", "values": [0.5]}
                        },
                        "S_ImbReversal": {
                            "extreme_imb_threshold": {"type": "integer", "values": [150]},
                            "reversal_delta_threshold": {"type": "integer", "values": [30]}
                        },
                        "S_LiquidityTrap": {
                            "delta_top10_large_change": {"type": "float", "values": [0.05]},
                            "price_no_move_threshold": {"type": "float", "values": [0.0001]},
                            "cv_high": {"type": "float", "values": [2.0]}
                        }
                        }


    with patch("application._backtest_param_parser.parse_strategy_params_from_file") as mock_parse_func, \
         patch("infrastructure.config.settings.BacktestingSettings") as MockBTSettings, \
         patch("infrastructure.config.settings.settings") as mock_settings:

        mock_parse_func.return_value = strategy_params

        mock_bt_instance = MagicMock()
        mock_bt_instance.default_path = Path("/mock/path/default.json")
        mock_bt_instance.custom_path = Path("/mock/path/custom.json")
        mock_bt_instance.best_path = Path("/mock/path/best.json")
        MockBTSettings.return_value = mock_bt_instance

        mock_settings.logger_level = "INFO"
        mock_settings.clickhouse.db_name = "mock_db"
        mock_settings.clickhouse.table_kline_archive = "mock_kline"
        mock_settings.clickhouse.table_orderbook_snapshots = "mock_orderbook"
        mock_settings.pair_tokens = ["BTCUSDT"]
        mock_settings.start_time = "2025-01-01"
        mock_settings.end_time = "2025-01-02"
        mock_settings.folder_report = "/tmp/reports"
        mock_settings.backtesting.cash = 10000

        yield


import numpy as np

arr = np.array([np.int32(1), np.float64(2.5), np.bool_(True)])
print(arr)