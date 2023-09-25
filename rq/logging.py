import logging
import sys

from typing import Union

from rq import utils
from rq.defaults import DEFAULT_LOGGING_DATE_FORMAT
from rq.defaults import DEFAULT_LOGGING_FORMAT


class ColorizingStreamHandler(logging.StreamHandler):
    levels = {
        logging.WARNING: utils.yellow,
        logging.ERROR: utils.red,
        logging.CRITICAL: utils.red,
    }

    def __init__(self, exclude=None, *args, **kwargs):
        self.exclude = exclude
        super().__init__(*args, **kwargs)

    @property
    def is_tty(self):
        isatty = getattr(self.stream, 'isatty', None)
        return isatty and isatty()

    def format(self, record):
        message = logging.StreamHandler.format(self, record)
        if self.is_tty:
            # Don't colorize any traceback
            parts = message.split('\n', 1)
            parts[0] = " ".join([parts[0].split(" ", 1)[0], parts[0].split(" ", 1)[1]])

            message = '\n'.join(parts)

        return message


def setup_loghandlers(
    level: Union[int, str, None] = None,
    date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
    log_format: str = DEFAULT_LOGGING_FORMAT,
    name: str = 'rq.worker',
):
    """Sets up a log handler.

    Args:
        level (Union[int, str, None], optional): The log level.
            Access an integer level (10-50) or a string level ("info", "debug" etc). Defaults to None.
        date_format (str, optional): The date format to use. Defaults to DEFAULT_LOGGING_DATE_FORMAT ('%H:%M:%S').
        log_format (str, optional): The log format to use.
            Defaults to DEFAULT_LOGGING_FORMAT ('%(asctime)s %(message)s').
        name (str, optional): The looger name. Defaults to 'rq.worker'.
    """
    logger = logging.getLogger(name)

    if _has_effective_handler(logger):
        formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
        handler = ColorizingStreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        handler.addFilter(lambda record: record.levelno < logging.ERROR)
        error_handler = ColorizingStreamHandler(stream=sys.stderr)
        error_handler.setFormatter(formatter)
        error_handler.addFilter(lambda record: record.levelno >= logging.ERROR)
        logger.addHandler(handler)
        logger.addHandler(error_handler)

    if level is not None:
        # The level may be a numeric value (e.g. when using the logging module constants)
        # Or a string representation of the logging level
        logger.setLevel(level if isinstance(level, int) else level.upper())


def _has_effective_handler(logger) -> bool:
    """
    Checks if a logger has a handler that will catch its messages in its logger hierarchy.

    Args:
        logger (logging.Logger): The logger to be checked.

    Returns:
        is_configured (bool): True if a handler is found for the logger, False otherwise.
    """
    while True:
        if logger.handlers:
            return True
        if not logger.parent:
            return False
        logger = logger.parent
