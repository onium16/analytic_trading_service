# test/unit/test_backtest_runner_params.py

import pytest
import numpy as np
from unittest.mock import MagicMock
from application.backtest_runner import BacktestRunner

@pytest.fixture
def backtest_runner_instance():
    # Глобальные зависимости замоканы через conftest.py
    runner = BacktestRunner(strategy_params={})
    runner.logger = MagicMock()  # Подмена логгера, если используются warning/info/debug
    return runner

def test_process_single_value_numpy_integers(backtest_runner_instance):
    assert backtest_runner_instance._process_single_value(np.int32(10)) == 10
    assert isinstance(backtest_runner_instance._process_single_value(np.int64(20)), int)

def test_process_single_value_numpy_floats(backtest_runner_instance):
    assert backtest_runner_instance._process_single_value(np.float32(10.5)) == 10.5
    assert isinstance(backtest_runner_instance._process_single_value(np.float64(20.5)), float)

def test_process_single_value_numpy_bool(backtest_runner_instance):
    assert backtest_runner_instance._process_single_value(np.bool_(True)) is True
    assert isinstance(backtest_runner_instance._process_single_value(np.bool_(False)), bool)

def test_process_single_value_lists_and_tuples(backtest_runner_instance):
    arr = np.array([np.int32(1), np.float64(2.5), np.bool_(True)])
    result = backtest_runner_instance._process_single_value(arr.tolist())
    assert result == [1, 2.5, True]
    assert isinstance(result[0], int)
    assert isinstance(result[1], float)
    assert isinstance(result[2], bool)

    result_tuple = backtest_runner_instance._process_single_value(tuple(arr))
    assert result_tuple == [1, 2.5, True]  # tuple -> list

def test_process_single_value_lists_and_tuples(backtest_runner_instance):
    # Original test setup:
    # arr = np.array([np.int32(1), np.float64(2.5), np.bool_(True)])
    # result = backtest_runner_instance._process_single_value(arr.tolist())

    # Revised setup: Directly provide a list with native Python types,
    # and then one with NumPy types to test handling within a list.

    # Test case 1: List with native Python types (already handled correctly by _process_single_value's base cases)
    list_native = [1, 2.5, True, 'string']
    result_native = backtest_runner_instance._process_single_value(list_native)
    assert result_native == [1, 2.5, True, 'string']
    assert isinstance(result_native[0], int)
    assert isinstance(result_native[1], float)
    assert isinstance(result_native[2], bool)
    assert isinstance(result_native[3], str)

    # Test case 2: List with NumPy types that _process_single_value should convert
    # This is closer to your original intent, ensuring the conversion happens within a list.
    list_numpy_types = [np.int32(10), np.float64(20.5), np.bool_(False)]
    result_numpy = backtest_runner_instance._process_single_value(list_numpy_types)
    assert result_numpy == [10, 20.5, False]
    assert isinstance(result_numpy[0], int) # Should now pass, as 10 comes in as np.int32
    assert isinstance(result_numpy[1], float)
    assert isinstance(result_numpy[2], bool)

    # Test case 3: A tuple with mixed types
    tuple_mixed = (np.int32(100), 200.0, np.bool_(True), "text")
    result_tuple = backtest_runner_instance._process_single_value(tuple_mixed)
    assert result_tuple == [100, 200.0, True, "text"] # _process_single_value returns a list for lists/tuples
    assert isinstance(result_tuple[0], int)
    assert isinstance(result_tuple[1], float)
    assert isinstance(result_tuple[2], bool)
    assert isinstance(result_tuple[3], str)


def test_create_param_config_from_raw_value(backtest_runner_instance):
    config_int = backtest_runner_instance.create_param_config(10, param_type='integer')
    assert config_int == {'type': 'integer', 'values': [10]}

    config_float = backtest_runner_instance.create_param_config(0.5, param_type='float')
    assert config_float == {'type': 'float', 'values': [0.5]}

    config_list = backtest_runner_instance.create_param_config([1, 2, 3], param_type='list')
    assert config_list == {'type': 'list', 'values': [1, 2, 3]}

    # Без указания param_type -> предполагаем list по умолчанию
    config_auto = backtest_runner_instance.create_param_config(7)
    assert config_auto == {'type': 'integer', 'values': [7]}

def test_create_param_config_from_existing_config(backtest_runner_instance):
    existing_config = {'type': 'integer', 'values': [np.int32(5), np.int64(10)]}
    processed_config = backtest_runner_instance.create_param_config(existing_config)
    assert processed_config == {'type': 'integer', 'values': [5, 10]}
    assert isinstance(processed_config['values'][0], int)
    assert isinstance(processed_config['values'][1], int)

    invalid_config = {'type': 'integer', 'values': "not_a_list"}
    with pytest.raises(ValueError, match="Некорректный формат 'values'"):
        backtest_runner_instance.create_param_config(invalid_config)

def test_create_param_config_invalid_param_type(backtest_runner_instance):
    with pytest.raises(ValueError, match="Неподдерживаемый тип параметра"):
        backtest_runner_instance.create_param_config("some_string", param_type='unknown')

