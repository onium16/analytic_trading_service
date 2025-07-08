# src/application/datasets_parser.py
import os
import zipfile
import pandas as pd
from io import TextIOWrapper
from typing import Optional

class ZipDataLoader:
    """
    Класс для загрузки и обработки данных из zip-архива с файлом JSON 
    формата *.data (Json line).

    Атрибуты:
        base_dir (str): Корневая директория проекта.
        data_dir (str): Путь к директории с данными.
        filename (str): Имя zip-файла.
        zip_path (str): Полный путь до zip-архива с данными.
    """

    def __init__(self, base_dir: Optional[str] = None, data_dir: Optional[str] = None, filename: str = "2025-06-30_ETHUSDT_ob500.data.zip") -> None:
        """
        Инициализация с возможностью указать базовую директорию проекта.

        Если base_dir не указан, будет вычислен автоматически 
        как директория на два уровня выше текущей.
        Args:
            base_dir (Optional[str]): Корневая директория проекта.
            data_dir (Optional[str]): Путь к папке с файлами данных.
            filename (str): Имя zip-файла.
        """
        if base_dir is None:
            base_dir = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
        self.base_dir = base_dir
        self.data_dir = os.path.join(self.base_dir, "datasets") if data_dir is None else data_dir
        self.filename = filename
        self.zip_path = os.path.join(self.data_dir, self.filename)

    def load_data(self) -> pd.DataFrame:
        """
        Загружает и обрабатывает данные из zip-архива.

        Возвращает:
            pd.DataFrame: Обработанный DataFrame с расширенными полями.
        """
        with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
            json_filename = zip_ref.namelist()[0]
            with zip_ref.open(json_filename) as f:
                text_file = TextIOWrapper(f, encoding='utf-8')
                df = pd.read_json(text_file, lines=True)
                df_extracted = pd.concat([df[['ts']], pd.json_normalize(df['data'])], axis=1)
                df_full = pd.concat([df.drop(columns=['data']).reset_index(drop=True), df_extracted.drop(columns=['ts']).reset_index(drop=True)], axis=1)
        return df_full


