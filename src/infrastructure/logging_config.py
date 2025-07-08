# src/infrastructure/logging_config.py
import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
GENERAL_LOG_FILE = os.path.join(LOG_DIR, "quant_strategy.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "errors.log")

os.makedirs(LOG_DIR, exist_ok=True)

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[96m',
        'INFO': '\033[1;37m',
        'WARNING': '\033[1;33m',
        'ERROR': '\033[1;31m',
        'CRITICAL': '\033[1;41m',
        'CUSTOM': '\033[1;32m',
    }
    RESET = '\033[0m'

    def format(self, record):
        original_levelname = record.levelname
        color = self.COLORS.get(original_levelname, self.RESET)
        # Формируем цветную строку без изменения record
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    level_num = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(level_num)

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s'
    )

    # Консольный обработчик с цветным выводом
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level_num)
    console_formatter = ColoredFormatter(
        '%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s'
    )
    console_handler.setFormatter(console_formatter)

    # Общий файл логов (INFO+)
    general_file_handler = RotatingFileHandler(GENERAL_LOG_FILE, maxBytes=5_000_000, backupCount=3)
    general_file_handler.setLevel(logging.INFO)
    general_file_handler.setFormatter(formatter)

    # Файл ошибок (ERROR+)
    error_file_handler = RotatingFileHandler(ERROR_LOG_FILE, maxBytes=2_000_000, backupCount=3)
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(console_handler)
        logger.addHandler(general_file_handler)
        logger.addHandler(error_file_handler)

    return logger
