"""Shared Logger"""

import sys
from loguru import logger as loguru_logger

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

LOG_LEVEL = "INFO"


def setup_logger():
    """Logger with stdout and file logging

    Returns:
        logger: An instance of Loguru
    """
    loguru_logger.remove()

    # output to console
    loguru_logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    )

    # outputs to file
    loguru_logger.add(
        "purr.log",
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
        rotation="500 MB",
        compression="zip",
    )

    return loguru_logger


logger = setup_logger()
