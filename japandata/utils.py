import json
import logging.config
import sys

from rich.logging import RichHandler


def load_dict(filepath: str) -> dict:
    """Load a dictionary from a JSON's filepath.

    Args:
        filepath (str): location of file.

    Returns:
        Dict: loaded JSON data.
    """
    with open(filepath) as fp:
        d = json.load(fp)
    return d


def japanese_to_western(year):
    """
    Convert Japanese year to Western year.

    Args:
        year (str): Japanese year to convert. Must be in the format of S, H, or R followed by a number. Supports Showa, Heisei, and Reiwa eras.

    Returns:
        int: Western year.
    """
    if year.startswith("R"):
        return int(year[1:]) + 2018
    elif year.startswith("H"):
        return int(year[1:]) + 1988
    elif year.startswith("S"):
        return int(year[1:]) + 1925
    else:
        raise ValueError(f"Invalid year: {year}")


def western_to_japanese(year):
    """
    Convert Western year to Japanese year. Function valid as of March 2023.

    Args:
        year (int): Western year to convert. Must be greater than 1926.

    Returns:
        str: Japanese year. Format is S, H, or R followed by a number.

    """
    if year >= 2019:
        return "R" + str(year - 2018)
    elif year >= 1989:
        return "H" + str(year - 1988)
    elif year >= 1926:
        return "S" + str(year - 1925)
    else:
        raise ValueError(f"Invalid year: {year}")


# Logger
logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "minimal": {"format": "%(message)s"},
        "detailed": {
            "format": "%(levelname)s %(asctime)s [%(name)s:%(filename)s:%(funcName)s:%(lineno)d]\n%(message)s\n"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "minimal",
            "level": logging.DEBUG,
        },
        "info": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "detailed",
            "level": logging.INFO,
        },
        "error": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "detailed",
            "level": logging.ERROR,
        },
    },
    "root": {
        "handlers": ["console"],
        "level": logging.INFO,
        "propagate": True,
    },
}
logging.config.dictConfig(logging_config)
logger = logging.getLogger()
logger.handlers[0] = RichHandler(markup=True)
