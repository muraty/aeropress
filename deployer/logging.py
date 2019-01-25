import logging

from deployer import logger

FORMAT = "[%(asctime)s] Thread(%(threadName)s) %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s"


def setup_logging(level: str) -> None:
    level = getattr(logging, level.upper())
    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(logging.Formatter(FORMAT))
    logger.setLevel(level)
    logger.addHandler(h)
