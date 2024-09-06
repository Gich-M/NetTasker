"""Module for setting up logging for the TaskProcessor"""
import logging
from logging.handlers import RotatingFileHandler


def logging_setup(log_level: str, log_file: str, error_log_file: str) -> None:
    """
    Sets up logging for the TaskProcessor.

    Args:
        log_level (str): The log level (e.g., DEBUG, INFO, WARNING, ERROR).
        log_file (str): The file path for the main log file.
        error_log_file (str): The file path for the error log file.

    Raises:
        ValueError: If the log level is invalid,
            or if the log file paths are empty.
        IOError: If there is an error setting up logfiles.
    """
    if not log_level or not isinstance(log_level, str):
        raise ValueError("Invalid log level")
    if not log_file or not error_log_file:
        raise ValueError("Log file paths cannot be empty")

    try:
        level = getattr(logging, log_level.upper())
        if not isinstance(level, int):
            raise ValueError(f"Invalid log level {log_level}")

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                RotatingFileHandler(
                    log_file,
                    maxBytes=10 * 1024 * 1024,
                    backupCount=5,
                    mode='a'),
            ]
        )

        error_handler = RotatingFileHandler(
            error_log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            mode='a'
        )

        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(error_handler)

    except Exception as e:
        raise IOError("Error setting up logging: {str(e)}") from e
