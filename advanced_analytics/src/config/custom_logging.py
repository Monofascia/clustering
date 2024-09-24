"""
logging
~~~~~~~

This module contains a class that create Custom Logger.
"""
from settings import Settings

import logging


class CustomLogger(object):
    """Custom class for logging.

    :param spark: SparkSession object.
    """

    format = "%(asctime)s %(levelname)s  %(name)s:%(lineno)d - %(message)s"
    datefmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(format=format, datefmt=datefmt)
    log_level = {
        "NOTSET": logging.NOTSET,
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARN,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __new__(cls, name):
        logger = logging.getLogger(name)
        logger.setLevel(cls.log_level[Settings.log_level.upper()])
        return logger

    @classmethod
    def get_default(cls, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        return logger
