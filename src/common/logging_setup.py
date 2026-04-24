import logging
import os

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: str = None) -> None:
    resolved = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=resolved,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        force=True,
    )
