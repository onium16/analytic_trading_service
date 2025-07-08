# src/application/backtest_runner.py

import asyncio
import datetime
import json
import numbers
import os
from pathlib import Path
import sys
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from typing import Optional, Dict, Any, Union

import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from backtesting import Backtest, Strategy
from application._backtest_data import prepare_backtest_data
from application._backtest_param_parser import parse_strategy_params_from_file
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository
from infrastructure.storage.schemas import KlineArchive, OrderBookArchive
from domain.strategies.strategies import (
    S_AskAccumulationShort, S_BidAcc, S_BidExh, S_BidWall, S_AskAcc, 
    S_AskExh, S_ImbReversal, S_LiquidityTrap
)
from infrastructure.config.settings import BacktestingSettings, settings
from infrastructure.logging_config import setup_logger

logger = setup_logger(__name__, level=settings.logger_level)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
logger.info(f"Project root: {PROJECT_ROOT}")

bt_setttings = BacktestingSettings()

class BacktestRunner:
    """
    Класс для запуска и управления бэктестами различных торговых стратегий.

    Атрибуты:
        archive_mode (bool): Использовать ли архивные данные из базы.
        strategy_params (Optional[Dict[str, Any]]): Параметры для оптимизации стратегий.
    """
    def __init__(self, archive_mode: bool = False, stream_mode: bool = False, archive_source: bool = True, stream_source: bool = True,  strategy_params: Optional[Dict[str, Any]] = None):
   
        self.logger = logger
        self.PAIR_TOKENS: list = [settings.pair_tokens]
        self.DB_NAME: str = settings.clickhouse.db_name
        self.TABLE_KLINE_ARCHIVE: str = settings.clickhouse.table_kline_archive
        self.TABLE_ORDERBOOK_ARCHIVE: str = settings.clickhouse.table_orderbook_snapshots
        self.ARCHIVE_MODE: bool = archive_mode
        self.STREAM_MODE: bool = stream_mode
        self.ARCHIVE_SOURCE: bool = archive_source
        self.STREAM_SOURCE: bool = stream_source
        self.START_TIME: str = settings.start_time
        self.END_TIME: str = settings.end_time
        self.FOLDER_REPORT: str = settings.folder_report
        self.UNIQUE_ID = None
        self.strategies_to_test: list = [
            S_BidAcc, S_BidExh, S_BidWall, S_AskAcc, 
            S_AskExh, S_ImbReversal, S_LiquidityTrap, S_AskAccumulationShort
        ]
        strategy_params_custom = parse_strategy_params_from_file(bt_setttings.custom_path) 
        logger.debug(strategy_params_custom)
        self.strategy_params = strategy_params if strategy_params is not None else strategy_params_custom
    

    def _process_single_value(self, value: Any) -> Any:
        """
        Вспомогательная функция для преобразования одиночного значения (включая NumPy-типы)
        в нативный Python-тип. Используется рекурсивно.
        """
        # CHECK FOR BOOLEANS FIRST!
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        elif isinstance(value, (int, np.integer)):
            return int(value)
        elif isinstance(value, (float, np.floating)):
            return float(value)
        elif isinstance(value, (list, tuple, np.ndarray)):
            return [self._process_single_value(item) for item in value]
        return value

    def create_param_config(self, param_value: Any, param_type: str = None) -> Dict[str, Any]:
        """
        Преобразует значение параметра в конфигурацию формата {'type': str, 'values': list}.
        Эта функция теперь также обрабатывает типы NumPy на входе и предназначена
        для создания как конфигурации параметров ОПТИМИЗАЦИИ (диапазонов, списков),
        так и для сохранения конкретных оптимизированных значений в унифицированном формате.

        Args:
            param_value: Значение параметра (число, список, кортеж, или уже готовый dict config).
            param_type: Явно указанный тип параметра ('integer', 'float', 'list'). Если None, определяется автоматически.

        Returns:
            Dict[str, Any]: Словарь конфигурации параметра.

        Raises:
            ValueError: Если тип параметра неизвестен или некорректен.
        """
        if isinstance(param_value, dict) and 'type' in param_value and 'values' in param_value:
            if isinstance(param_value['values'], (list, tuple)):
                # Рекурсивно обрабатываем значения внутри 'values' на случай NumPy-типов
                param_value['values'] = [self._process_single_value(v) for v in param_value['values']]
                return param_value
            else:
                raise ValueError(f"Некорректный формат 'values' в существующей конфигурации: {param_value['values']}")

        processed_value = self._process_single_value(param_value)

        if param_type is None:
            if isinstance(processed_value, numbers.Integral):
                param_type = "integer"
            elif isinstance(processed_value, numbers.Real):
                param_type = "float"
            elif isinstance(processed_value, (list, tuple)):
                param_type = "list"
            else:
                raise ValueError(f"Не удалось определить тип для значения: {processed_value} ({type(processed_value)})")

        if param_type not in ("integer", "float", "list"):
            raise ValueError(f"Неподдерживаемый тип параметра: {param_type}")

        if param_type == "list":
            if not isinstance(processed_value, (list, tuple)):
                raise ValueError(f"Для типа 'list' ожидается список или кортеж, получено: {processed_value}")
            return {"type": "list", "values": list(processed_value)}
        
        if not isinstance(processed_value, numbers.Real):
            raise ValueError(f"Для типа '{param_type}' ожидается число, получено: {processed_value}")
        
        return {"type": param_type, "values": [processed_value]}


    def save_optimized_params_to_file(self, optimized_results_map: Dict[str, Dict[str, Any]], best_params_path: Union[str, Path]):
        """
        Сохраняет лучшие оптимизированные параметры для стратегий в указанный JSON файл.
        Формат записи: { "StrategyName": { "param1": value1, "param2": value2, ... } }
        Это перезаписывает существующие параметры для данных стратегий и сохраняет другие.

        Args:
            optimized_results_map (Dict[str, Dict[str, Any]]): Словарь, где ключ - имя стратегии,
                                                                значение - словарь {param_name: value}.
            best_params_path (Union[str, Path]): Путь к файлу best_strategy_settings.json.
        """
        best_params_path = Path(best_params_path)
        current_best_params = {}

        if best_params_path.exists():
            try:
                with open(best_params_path, 'r', encoding='utf-8') as f:
                    current_best_params = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"ПРЕДУПРЕЖДЕНИЕ: Не удалось распарсить существующий файл {best_params_path}: {e}. Создаем новый.")
                current_best_params = {}
            except Exception as e:
                logger.error(f"ПРЕДУПРЕЖДЕНИЕ: Ошибка при чтении файла {best_params_path}: {e}. Создаем новый.")
                current_best_params = {}
        
        for strategy_name, params in optimized_results_map.items():
            clean_strategy_name = strategy_name.replace(" (Optimized)", "").replace(" (Optimized_pair)", "") # Учитываем ваш формат
            current_best_params[clean_strategy_name] = params

        try:
            with open(best_params_path, 'w', encoding='utf-8') as f:
                json.dump(current_best_params, f, indent=4, ensure_ascii=False)
            logger.info(f"Лучшие параметры стратегий сохранены в: {best_params_path}")
        except Exception as e:
            logger.error(f"ОШИБКА: Не удалось сохранить лучшие параметры стратегий в {best_params_path}: {e}")
            raise

    def _enhance_equity_curve(self, output):
        equity_curve = output._equity_curve.copy()
        if 'Returns' not in equity_curve.columns:
            equity_curve['Returns'] = equity_curve['Equity'].pct_change().fillna(0)
        if 'Volatility' not in equity_curve.columns:
            equity_curve['Volatility'] = equity_curve['Returns'].rolling(window=20).std().fillna(0)
        if 'Sharpe' not in equity_curve.columns:
            rolling_returns_mean = equity_curve['Returns'].rolling(window=20).mean()
            rolling_returns_std = equity_curve['Returns'].rolling(window=20).std()
            sharpe_ratio_series = (rolling_returns_mean / rolling_returns_std.replace(0, np.nan)) * np.sqrt(252)
            equity_curve['Sharpe'] = sharpe_ratio_series.fillna(0)
        if 'Drawdown' not in equity_curve.columns:
            running_max = equity_curve['Equity'].cummax()
            equity_curve['Drawdown'] = (equity_curve['Equity'] - running_max) / running_max * 100
        return equity_curve

    def get_color(self, num_colors: int) -> list:
        cmap = plt.get_cmap('Set3')
        colors = []
        for i in range(num_colors):
            normalized_i = i / max(num_colors - 1, 1)
            rgb_float = cmap(normalized_i)[:3]
            rgb_int = tuple(int(255 * x) for x in rgb_float)
            colors.append(f'rgb{rgb_int}')
        return colors

    def plot_single_large_dashboard(self, results_df, all_strategy_outputs, price_df=None):

        try:
            strategies_dict = {}

            for name, result_series in all_strategy_outputs:
                strategies_dict[name] = {
                    'summary': result_series,
                    '_trades': getattr(result_series, '_trades', None),  
                    '_equity_curve': getattr(result_series, '_equity_curve', None)  
                }

        except Exception as e:
            logger.error(f"Failed to prepare strategy outputs: {str(e)}")
            return

        # --- Подготовка точек входа/выхода для всех стратегий ---
        try:
            trades_points = {}

            for strat_name, output_Serial in all_strategy_outputs:
                
                output = output_Serial.get('_trades', None)
                if output is None:
                    logger.warning("output[_trades] is None")
                    # return

                trades_df = output

                if trades_df is None or trades_df.empty:
                    continue  # Нет сделок для этой стратегии

                if 'EntryBar' not in trades_df.columns or 'ExitBar' not in trades_df.columns:
                    continue  # Нет нужных колонок

                entry_points = trades_df[trades_df['EntryBar'].notnull()]
                exit_points = trades_df[trades_df['ExitBar'].notnull()]

                
                # Вычисляем координаты для входов
                if not entry_points.empty and price_df is not None and 'Close' in price_df.columns:
                    valid_entry_indices = entry_points['EntryBar'].astype(int)
                    valid_entry_indices = valid_entry_indices[valid_entry_indices < len(price_df)]
                    entry_x = price_df.index[valid_entry_indices]
                    entry_y = price_df['Close'].loc[entry_x]
                else:
                    entry_x = []
                    entry_y = []


                # Вычисляем координаты для выходов
                if not exit_points.empty and price_df is not None and 'Close' in price_df.columns:
                    valid_exit_indices = exit_points['ExitBar'].astype(int)
                    valid_exit_indices = valid_exit_indices[valid_exit_indices < len(price_df)]
                    exit_x = price_df.index[valid_exit_indices]
                    exit_y = price_df['Close'].loc[exit_x]
                else:
                    exit_x = []
                    exit_y = []

                trades_points[strat_name] = {
                    "entry_x": entry_x,
                    "entry_y": entry_y,
                    "exit_x": exit_x,
                    "exit_y": exit_y
                }
        except Exception as e:
            logger.error(f"Failed to prepare trades points: {str(e)}")
            return

        try:
            # --- Построение графика ---
            fig = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                row_heights=[0.3, 0.3, 0.4],
                vertical_spacing=0.05,
                subplot_titles=("Multi-Strategy Backtest: Price", "Multi-Strategy Backtest: Equity Curves", "Multi-Strategy Backtest: Result_Strategies"),
                specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"type": "table"}]]  # во второй строке – таблица
            )

            fig.update_layout(
                height=2000,
                width=1400,
                title_text=f"Multi-Strategy Backtest: Price & Equity Curves",
                xaxis_rangeslider_visible=False,
                hovermode="x unified",
                template="plotly_white"
            )

            column_widths = [200] + [100] * (len(results_df.columns) - 1)  # Увеличиваем первую колонку

            fig.add_trace(go.Table(
                # Добавляем DF внизу
                columnwidth=column_widths,
                header=dict(values=list(results_df.columns), align='left'),
                cells=dict(values=[results_df[col] for col in results_df.columns], align='center')
            ), row=3, col=1)

            # Добавляем свечи
            fig.add_trace(go.Candlestick(
                x=price_df.index,
                open=price_df['Open'],
                high=price_df['High'],
                low=price_df['Low'],
                close=price_df['Close'],
                name='Price Candlesticks'
            ), row=1, col=1)


            # Добавляем входы/выходы для каждой стратегии с разными цветами
            num_colors = len(strategies_dict)*2 
            colors = self.get_color(num_colors)
            colors_entry = colors[len(strategies_dict):]
            colors_exit = colors[:len(strategies_dict)]

            for i, (strat_name, points) in enumerate(trades_points.items()):
                c_entry = colors_entry[i % len(colors_entry)]
                c_exit = colors_exit[i % len(colors_exit)]

                entry_count = len(points["entry_x"])
                exit_count = len(points["exit_x"])

                fig.add_trace(go.Scatter(
                    x=points["entry_x"],
                    y=points["entry_y"],
                    mode='markers',
                    name=f'{strat_name} Entry ({entry_count})',
                    legendgroup=strat_name,
                    marker=dict(symbol='triangle-up', color=c_entry, size=12, line=dict(width=1, color='black')),
                    showlegend=True
                ), row=1, col=1)

                fig.add_trace(go.Scatter(
                    x=points["exit_x"],
                    y=points["exit_y"],
                    mode='markers',
                    name=f'{strat_name} Exit ({exit_count})',
                    legendgroup=strat_name,
                    marker=dict(symbol='triangle-down', color=c_exit, size=10, line=dict(width=1, color='black')),
                    showlegend=True
                ), row=1, col=1)

            enhanced_curves = {name: self._enhance_equity_curve(output) for name, output in all_strategy_outputs}
            num_colors = len(enhanced_curves)
            colors_lines = self.get_color(num_colors)

            for i, (strat_name, enhanced_curve) in enumerate(enhanced_curves.items()):
                color = colors_lines[i % len(colors_lines)]
                fig.add_trace(go.Scatter(
                    x=enhanced_curve.index,
                    y=enhanced_curve['Equity'],
                    mode='lines',
                    name=f'{strat_name} Equity',
                    legendgroup=strat_name,
                    line=dict(color=color)
                ), row=2, col=1)
        except Exception as e:
            logger.error(f"Failed to create interactive single large dashboard: {str(e)}")
            return

        # --- Сохранение графика ---
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            output_dir = os.path.join(self.FOLDER_REPORT, str(self.UNIQUE_ID))
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, f"output-single_large_dashboard-{timestamp}.html")
            
            pio.write_html(fig, file=filename, auto_open=False)

            logger.info(f"Interactive single large dashboard saved to: {filename}")

        except Exception as e:
            logger.error(f"Error creating single large dashboard: {str(e)}")

    async def run_backtest(self):
        start_process_time = datetime.datetime.now()
        self.UNIQUE_ID = int(start_process_time.timestamp())
        os.makedirs(f"{self.FOLDER_REPORT}/{self.UNIQUE_ID}", exist_ok=True)

        try:
            # Подготовка данных
            repo_kline = ClickHouseRepository(schema=KlineArchive, db=self.DB_NAME, table_name=self.TABLE_KLINE_ARCHIVE, port=8123)
            repo_orderbook = ClickHouseRepository(schema=OrderBookArchive, db=self.DB_NAME, table_name=self.TABLE_ORDERBOOK_ARCHIVE, port=8123)
            await repo_kline.ensure_table()
            await repo_orderbook.ensure_table()

            start_time_pd = pd.Timestamp(f"{self.START_TIME} 00:00:00")
            end_time_pd = pd.Timestamp(f"{self.END_TIME} 00:00:00")

            df_full = await prepare_backtest_data(repo_kline, repo_orderbook, start_time_pd, end_time_pd, database_mode = True)
            if df_full.empty:
                self.logger.info("No data loaded for backtesting, exiting.")
                return
            
        except Exception as e:
            self.logger.error(f"Failed to prepare backtest data: {str(e)}", exc_info=True)
            return
        
        self.logger.debug(f"df_full index type: {type(df_full.index)}")
        self.logger.debug(f"df_full index: {df_full.index}")
        self.logger.debug(f"df_full info: {df_full.info()}")
        self.logger.debug(f"df_full columns: {df_full.columns.tolist()}")
        self.logger.debug(f"df_full head: {df_full.head().to_string()}")
        
        try:
            df_full = df_full.loc[(df_full["timestamp"] >= start_time_pd) & (df_full["timestamp"] <= end_time_pd)]
                        
            if self.ARCHIVE_SOURCE and self.STREAM_SOURCE:
                self.logger.info("Full sources are selected. Loading data from both archive and stream.")

            elif self.ARCHIVE_SOURCE:
                self.logger.info("Archive source selected. Loading data from archive.")
                df_full = df_full.loc[df_full["type"].isin(['delta', 'snapshot'])]

            elif self.STREAM_SOURCE:
                self.logger.info("Stream source selected. Loading data from stream. This feature is not implemented yet.")
                df_full = df_full.loc[df_full["type"] == 'snapshot_stream']

            else:
                self.logger.warning("No data source selected. Skipping data load.")
                return

            if df_full.empty:
                self.logger.warning("No data loaded for backtesting, exiting.")
                return
            
            self.logger.info(f"!! Loaded data for backtesting. Timeframe: {start_time_pd} to {end_time_pd}")
            self.logger.debug(f"Data shape: {df_full.shape}")

            all_strategy_results_for_plot = []
            results = []
            all_optimized_params_for_saving = {} 

            for pair in self.PAIR_TOKENS:
                self.logger.info(f"\n--- Running backtest for pair: {pair} ---")
                for strat_class in self.strategies_to_test:
                    strat_name = strat_class.__name__
                    self.logger.info(f"\n--- Running Backtest for {strat_name} ---")
                    bt = Backtest(
                        df_full,
                        strat_class,
                        cash=settings.backtesting.cash,
                        commission=0.001,
                        exclusive_orders=True
                    )

                    # --- Запуск с параметрами по умолчанию / без оптимизации ---
                    start_time = datetime.datetime.now()
                    output = bt.run()
                    end_time = datetime.datetime.now()

                    all_strategy_results_for_plot.append((f"{strat_name} (Default)", output))

                    metrics = {
                        'Strategy': f"{strat_name}_{pair}",
                        'Final Equity [$]': output['Equity Final [$]'],
                        'Return [%]': output['Return [%]'],
                        'Max Drawdown [%]': output['Max. Drawdown [%]'],
                        'Number of Trades': output['# Trades'],
                        'Win Rate [%]': output['Win Rate [%]'] if output['# Trades'] > 0 else 0,
                        'Sharpe Ratio': output.get('Sharpe Ratio', 0),
                        'Duration': str(end_time - start_time),
                        'Parameters': 'Default' 
                    }
                    results.append(metrics)

                    if strat_name in self.strategy_params:
                        # self.strategy_params содержит диапазоны для optimize()
                        optimize_config = self.strategy_params[strat_name].copy() 
                        
                        # Проверяем, есть ли что оптимизировать (т.е. есть ли диапазоны)
                        if not any(isinstance(v, (range, list, tuple)) for v in optimize_config.values()):
                            self.logger.info(f"  ВНИМАНИЕ: Для стратегии {strat_name} не найдено параметров 'range' или 'list' для оптимизации. Пропускаем оптимизацию.")
                            continue 

                        self.logger.info(f"Optimizing parameters for {strat_name}...")
                        start_opt_time = datetime.datetime.now()
                        try:
                            # Извлекаем специальные аргументы для optimize()
                            maximize_metric = optimize_config.pop('maximize', 'Return [%]')
                            constraint_func = optimize_config.pop('constraint', None)

                            optimized_output = bt.optimize(
                                **optimize_config, # Передаем только параметры стратегии для оптимизации
                                maximize=maximize_metric,
                                constraint=constraint_func # Передаем функцию ограничения
                            )

                            end_opt_time = datetime.datetime.now()

                            # Backtesting.py сохраняет их в _strategy._params
                            best_found_params_raw = optimized_output._strategy._params # Это сырые параметры с типами NumPy

                            optimized_params = {}
                            for param in self.strategy_params[strat_name]:
                                val = getattr(optimized_output._strategy, param, None)
                                if val is not None:
                                    optimized_params[param] = val

                            formatted_best_params = {}
                            for param_name, param_value in best_found_params_raw.items():
                                formatted_best_params[param_name] = self.create_param_config(param_value)

                            all_optimized_params_for_saving[strat_name] = formatted_best_params # Сохраняем преобразованные параметры

                            opt_strat_name = f"{strat_name} (Optimized)_{pair}"
                            all_strategy_results_for_plot.append((opt_strat_name, optimized_output))

                            self.logger.debug(f"Optimized parameters for {strat_name}: {optimized_params}")
            
                            opt_metrics = {
                                'Strategy': opt_strat_name,
                                'Final Equity [$]': optimized_output['Equity Final [$]'],
                                'Return [%]': optimized_output['Return [%]'] if optimized_output['Return [%]'] is not None else 0,
                                'Max Drawdown [%]': optimized_output['Max. Drawdown [%]'],
                                'Number of Trades': optimized_output['# Trades'],
                                'Win Rate [%]': optimized_output['Win Rate [%]'] if optimized_output['# Trades'] > 0 else 0,
                                'Sharpe Ratio': optimized_output.get('Sharpe Ratio', 0),
                                'Duration': str(end_opt_time - start_opt_time),
                                'Parameters': optimized_params
                            }
                            results.append(opt_metrics)
                        except Exception as e:
                            self.logger.error(f"Optimization failed for {strat_name}: {str(e)}")
                    else:
                        self.logger.info(f"No optimization configuration found for {strat_name}. Skipping optimization.")


            self.logger.debug(f"All collected optimized parameters for saving: {all_optimized_params_for_saving}")
            if all_optimized_params_for_saving:
                self.save_optimized_params_to_file(all_optimized_params_for_saving, bt_setttings.best_path)
            else:
                self.logger.info("No strategies were optimized, skipping saving of best parameters.")


            results_df = pd.DataFrame(results)
            self.logger.info("\n--- Backtest Results Summary ---")
            self.logger.info(results_df.to_string(index=False))

            # Найдём колонки с '%' в названии
            percent_columns = [col for col in results_df.columns if '%' in col]
            # Найдём колонки с '%' в названии
            dollar_columns = [col for col in results_df.columns if '$' in col]

            # Округлим эти колонки до 5 знаков после запятой
            results_df[percent_columns] = results_df[percent_columns].round(4)
            results_df[dollar_columns] = results_df[dollar_columns].round(4)

            results_df.to_csv(f'{self.FOLDER_REPORT}/{self.UNIQUE_ID}/strategy_performance_{"_".join(self.PAIR_TOKENS)}_{self.START_TIME}_{self.END_TIME}.csv', index=False)
            self.logger.info(f"\nResults saved to '{self.FOLDER_REPORT}/{self.UNIQUE_ID}/strategy_performance_{"_".join(self.PAIR_TOKENS)}_{self.START_TIME}_{self.END_TIME}.csv'")

            best_strategy = results_df.loc[results_df['Return [%]'].idxmax()]
            self.logger.info(f"\nBest performing strategy: {best_strategy['Strategy']}")

            self.plot_single_large_dashboard(results_df, all_strategy_results_for_plot, price_df=df_full)

            end_process_time = datetime.datetime.now()
            self.logger.info(f"\nOverall script duration: {end_process_time - start_process_time}")

        except Exception as e:
            self.logger.error(f"Backtest failed: {str(e)}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    runner = BacktestRunner(
        archive_mode=True,
        stream_mode=True,
        archive_source=True,
        stream_source=True,
    )
    settings.start_time = "2025-06-20"
    settings.end_time = "2025-06-21"
    asyncio.run(runner.run_backtest())