import logging
from logging.handlers import RotatingFileHandler
import sys
from src.config import LOG_DIR, LOG_LEVEL_FILE, LOG_LEVEL_CONSOLE


def setup_logging(
    log_dir=LOG_DIR,
    file_level=LOG_LEVEL_FILE,
    console_level=LOG_LEVEL_CONSOLE,
    log_filename="etl_process.log",
):
    log_file_path = log_dir / log_filename

    file_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)-10s | %(module)s:%(lineno)-4d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )


    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=10_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)


    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)


    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)



set_logging = setup_logging