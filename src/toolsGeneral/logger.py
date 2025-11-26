import logging
import os

def initiate_logger(name, log_file_path = None):
    logger = logging.getLogger(name)

    if logger.hasHandlers(): return logger

    logger.setLevel(logging.INFO)

    # if you want to save add file handler
    if log_file_path:
        add_file_handler(logger, log_file_path)

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("[%(levelname)s] : %(message)s"))
    logger.addHandler(console_handler)

    return logger

def add_file_handler(logger, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_handler = logging.FileHandler(file_path, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s : %(message)s"))
    logger.addHandler(file_handler)